"""
$ python -m spacy download en_core_web_sm
$ cd .../entity_knowledge_in_bert/app
$ conda env update -f environment.yml
$ python -m app.preprocess
"""
from __future__ import annotations
from SPARQLWrapper import SPARQLWrapper, JSON
import os
import sys
import datetime
import dateutil
import itertools
import json
import pathlib
import time
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, List, Tuple, Optional, Set, Union

import pandas as pd
import networkx as nx
import requests
import requests_cache
import spacy
import yfinance as yf
import elasticsearch
from dotenv import load_dotenv
from flashtext import KeywordProcessor
from qwikidata.entity import WikidataItem
from qwikidata.json_dump import WikidataJsonDump
from qwikidata.utils import dump_entities_to_json

import graph_tool.all as gt

from . import es

load_dotenv()
requests_cache.install_cache('cache')


def serialize_sets(obj: Any) -> Any:
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    return obj


def load_or_run(filepath: str, fn: Callable, forcerun=False) -> Any:
    def run():
        results = fn()
        with open(filepath, "w", encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, default=serialize_sets)
        return results
    if forcerun:
        return run()
    try:
        with open(filepath, encoding='utf-8') as f:
            print(f"- Load from '{filepath}', skip running")
            return json.load(f)
    except FileNotFoundError:
        return run()


def query_sparql(endpoint_url: str, query: str) -> dict:
    user_agent = "WDQS-example Python/%s.%s" % (
        sys.version_info[0], sys.version_info[1])
    # TODO adjust user agent; see https://w.wiki/CX6
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()


def query_wikidata_by_property():
    endpoint = "https://query.wikidata.org/sparql"
    query = """
    # 列出items有ticker property
    SELECT ?item ?itemLabel ?value ?valueLabel
    WHERE
    {
        ?item wdt:P414 ?value
        SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
    }
    # LIMIT 10
    """
    return query_sparql(endpoint, query)


def query_wikipage_title(wikidata_ids: List[str]) -> List[str]:
    def q(id: str):
        api = "https://www.wikidata.org/w/api.php"
        params = {
            "action": "wbgetentities",
            "format": "json",
            "props": "sitelinks",
            "ids": id,
        }
        j = requests.get(api, params=params).json()
        return j['entities'][id]['sitelinks']['enwiki']['title'].replace(' ', '_')
    return [q(id) for id in wikidata_ids]


def search_wikipage(terms: Iterable[str]) -> Dict[str, str]:
    def candidates(term: str) -> List[str]:
        cands = [term]
        # remove punct
        for c in ",.:;":
            term = term.replace(c, "")
        term = term.strip()
        cands.append(term)
        # remove extra words
        term = term.lower()
        for w in ("inc", "limited", "ltd"):
            term = term.replace(w, "")
        term = term.strip()
        cands.append(term)
        return cands

    results = dict()
    for tm in terms:
        for cand in candidates(tm):
            try:
                api = "https://en.wikipedia.org/w/api.php"
                params = {
                    "action": "opensearch",
                    "namespace": "0",
                    "search": cand,
                    "limit": 1,
                    "format": "json",
                    "redirects": "resolve",
                }
                j = requests.get(api, params=params).json()
                results[tm] = j[3][0]
                time.sleep(3)  # throttle
                break
            except IndexError:
                pass
        if tm not in results:
            results[tm] = None
    return results


def scrape_ticker_info_using_yfinance(tickers: Iterable[str]) -> Dict[str, dict]:
    ticker_info = {}
    for t in tickers:
        stock = yf.Ticker(t)
        try:
            ticker_info[t] = stock.info
            time.sleep(3)  # throttle
        except:
            print(t)
    return ticker_info


def download_tickers():
    pass


def download_ticker_info_from_iexcloud(tickers: Iterable[str], token: str) -> Dict[str, dict]:
    ticker_info = {}
    for t in tickers:
        try:
            r = requests.get(
                f"https://cloud.iexapis.com/stable/stock/{t.lower()}/company",
                params={"token": token})
            ticker_info[t] = r.json()
        except:
            pass
        time.sleep(1)  # throttle
    return ticker_info


def get_wktitles_graph(seedtitles: Iterable[str], n_depth: int) -> nx.DiGraph:
    """
    Return:
        DiGraph: (node, {mentions: json_str, depth: int})
    Usage:
        G.nodes["nodename"]["mentions"]
        G.nodes["nodename"]["depth"]
        G.out_degree("nodename")
    """
    G = nx.DiGraph()
    seeds = seedtitles
    for i in range(n_depth):
        print(f"Depth {i}: {len(seeds)} nodes")
        for seed in seeds:
            print(seed)
            doc = es.get_wikipage(seed)
            print(doc)
            if doc is None:
                unfounded = True
                mentions = []
            else:
                unfounded = False
                mentions = json.loads(doc.mentions)

            if seed in G:
                G.nodes[seed]['depth'] = i
                G.nodes[seed]['unfounded'] = unfounded
            else:
                G.add_node(seed, depth=i, unfounded=unfounded, mentions=set())

            for wktitle, mention in mentions:
                G.add_edge(seed, wktitle)
                try:
                    G.nodes[wktitle]['mentions'].add(mention)
                except KeyError:
                    G.nodes[wktitle]['mentions'] = {mention}
        seeds = [n for n in G if not 'depth' in G.nodes[n]]
        print(seeds)

    # 最後的seeds需要手動加入properties
    for seed in seeds:
        unfounded = False
        if seed in G:
            G.nodes[seed]['depth'] = n_depth
            G.nodes[seed]['unfounded'] = unfounded
        else:
            G.add_node(seed, depth=n_depth,
                       unfounded=unfounded, mentions=set())
    return G


def get_matched_wkd_entities(wktitles: List[str]) -> Dict[str, WikidataItem]:
    def is_matched(q: WikidataItem) -> bool:
        # 確認是否有中文名
        if q.get_label("zh") == "":
            print(f'Skip, no zh label: {q.get_enwiki_title()}')
            return False

        # entity不能是人
        cg = q.get_claim_group("P31")  # P31:instance_of
        instanceof = [c.mainsnak.datavalue.value['id'] for c in cg]
        if "Q5" in instanceof:  # Q5:human
            print(f'Skip, is a person: {q.get_enwiki_title()}')
            return False

        # entity不能有位置claim
        cg = q.get_claim_group("P625")  # P625:coordinate_location
        if cg.property_id is not None:
            print(f'Skip, has coordinate location: {q.get_enwiki_title()}')
            return False
        return True

    matched = dict()
    for wktitle in wktitles:
        try:
            doc = es.WikiData.get(id=wktitle)
            q = WikidataItem(json.loads(doc.json))
            if is_matched(q):
                matched[wktitle] = q
        except elasticsearch.NotFoundError:
            print(f"Not found wikidata: {wktitle}")

    return matched


def calc_pagerank(g: gt.Graph) -> List[Tuple[int, str, float]]:
    """
    Return: sorted list of tuples, [(vertex_idx, wk_title, pagerank_value), ....]
    """
    vp_label = g.vp['_graphml_vertex_id']  # same as wktitle
    pr = gt.pagerank(g)
    ranks = [(g.vertex_index[v], vp_label[v], pr[v]) for v in g.vertices()]
    ranks = sorted(ranks, key=lambda e: -e[-1])
    return ranks

### Corpus Processors ###


def expand_word(word: str):
    pass


def get_mention2idx_dict(mentions: Dict[str, List[str]]) -> Dict[int, Dict[int, int]]:
    return {k: i for i, k in enumerate(mentions.keys())}


def get_idx2mention_dict(mentions: Dict[str, List[str]]) -> Dict[int, Dict[int, int]]:
    return {i: k for i, k in enumerate(mentions.keys())}


def count_corpus_mentions_cooccurs(mentions: Dict[str, List[str]]) -> Any:
    mention2idx = get_mention2idx_dict(mentions)

    def count_cooccur(bag: Iterable[int], cooccur: Dict[int, Dict[int, int]] = defaultdict(lambda: defaultdict(int))) -> Dict[int, Dict[int, int]]:
        for a in bag:
            for b in bag:
                cooccur[a][b] += 1
        return cooccur

    nlp = spacy.load('en_core_web_sm')
    processor = KeywordProcessor()
    processor.add_keywords_from_dict(mentions)

    sent_cooccur, atk_cooccur = defaultdict(
        lambda: defaultdict(int)), defaultdict(lambda: defaultdict(int))
    atk_bags = []
    for i, hit in enumerate(es.scan_scraper_page("*cnbc*")):
        if i % 100 == 0:
            print(i)
        if i > 100:
            break
        sents = []
        try:
            sents.append(hit.article_title)
        except:
            pass
        try:
            sents += [sent.text for sent in nlp(hit.article_text).sents]
        except:
            pass

        atk_bag = set()
        for sent in sents:
            kws = processor.extract_keywords(sent, span_info=False)
            sent_bag = [mention2idx[kw] for kw in kws]
            sent_cooccur = count_cooccur(sent_bag, sent_cooccur)
            atk_bag |= set(sent_bag)
        atk_cooccur = count_cooccur(atk_bag, atk_cooccur)
        atk_bags.append(atk_bag)

    return (
        # 在一個句子中，兩個entity同時出現的次數: {a: {a: 4, b: 2, c:1}, b: {...}}
        dict(sent_cooccur),
        # 在一篇文章中，兩個entity同時出現的次數: {a: {a: 4, b: 2, c:1}, b: {...}}
        dict(atk_cooccur),
        # 在一篇文章中出現的entities集合成一個bag: [{a, b, c}, {a, c, f}, ...]
        set(map(frozenset, atk_bags)),)


def count_corpus_mentions_freqs(mentions: Dict[str, List[str]]) -> Dict[str, Dict[int, int]]:
    processor = KeywordProcessor()
    processor.add_keywords_from_dict(mentions)

    # {"aaa": {1230: 2, 1231: 1, 1233: 5}, "bbb": {...}}
    freq = defaultdict(lambda: defaultdict(int))
    for i, hit in enumerate(es.scan_scraper_page("*cnbc*")):
        if i % 100 == 0:
            print(i)
        try:
            d = dateutil.parser.parse(hit.article_published_at)
            o = d.date().toordinal()
            for kw in processor.extract_keywords(
                    f"{hit.article_title}\n{hit.article_text}", span_info=False):
                freq[kw][o] += 1
        except Exception as e:
            print(e)
            pass
    return dict(freq)


def main():
    # params
    IEXCLOUD_TOKEN = os.getenv("IEXCLOUD_TOKEN")

    # 需要事前提供的檔案

    # 在執行時會自動產生的檔案
    output_folder = './outputs'
    downlaods_folder = './downloads'
    entities_json = f"{output_folder}/wd_entities.json"
    tk_csv = './downloads/bats_symbols_traded_byx.csv'
    tk_info_json = "./downloads/iex_ticker_info.json"
    urls_json = f"{output_folder}/wiki_urls.json"
    mentions_json = f"{output_folder}/wiki_mentions.json"
    sent_cooccurs_json = f"{output_folder}/corpus_mentions_sent_cooccurs.json"
    atk_cooccurs_json = f"{output_folder}/corpus_mentions_atk_cooccurs.json"
    atk_bags_json = f"{output_folder}/corpus_mentions_atk_bags.json"
    freqs_json = f"{output_folder}/corpus_mentions_freqs.json"

    # Wiki processor requires:
    explore_n_wk_depth: int = 2  # 探索wk的層數
    adpot_n_wk_depth: int = 1    # 在n層以內的wk-titles會被實際採用（其他用作graph計算）
    wkd_dump_json = "./downloads/latest-all.json.bz2"
    seeded_wk_titles = []
    sp500_csv = f"{downlaods_folder}/s_and_p_500.csv"

    # Wiki processor outputs:
    wk_titles_graphml = f"{output_folder}/wk_titles.graphml.bz2"
    wk_pagerank_json = f"{output_folder}/wk_pagerank.json"
    wk_cat_tags_json = f"{output_folder}/wk_cat_tags.json"
    # wk_tags_json = f"{output_folder}/wk_tags.json"
    wk_tags_pagerank_csv = f"{output_folder}/wk_tags_pagerank.csv"

    wkd_filtered_entities_json = f"{output_folder}/wkd_filtered_entities.json"
    wk_ranked_titles_json = f"{output_folder}/wk_ranked_titles.json"
    wkd_entites_by_ranked_titles_json = f"{output_folder}/wkd_entites_by_ranked_titles.json"

    pathlib.Path(output_folder).mkdir(exist_ok=True)

    # print(get_matched_wkd_entities(titles, wkd_dump_path=wkd_dump_json))
    # entities = load_or_run(wkd_entites_by_ranked_titles_json,
    #                     lambda: get_matched_wkd_entities(titles, wkd_dump_path=wkd_dump_json),
    #                     forcerun=True)

    # print("從wikidata取得具有symbol屬性的entities")
    # results = load_or_run(
    #     entities_json, lambda: query_wikidata_by_property())
    # comp_wdids = [e['item']['value'].split('/')[-1]
    #               for e in results['results']['bindings']]

    # print("找wikidata-entity對應的wikipage")
    # comp_titles = load_or_run(
    #     comp_titles_json, lambda: query_wikipage_title(comp_wdids))
    # return

    # print("讀取tickers")
    # df = pd.read_csv(tk_csv)
    # tickers = list(df['Symbols'])
    # # tickers = ['ADBE', 'BA', 'RXT', 'TTOO']
    # print(f"載入ticker數量: {len(tickers)}")

    # print("從iexcloud抓ticker info")
    # infos = load_or_run(
    #     tk_info_json, lambda: download_ticker_info_from_iexcloud(tickers, IEXCLOUD_TOKEN))
    # names = [v['companyName'] for k, v in infos.items()]

    # print("找ticker-info中的公司名搜尋對應的wikipage")
    # urls = load_or_run(
    #     urls_json, lambda: search_wikipage(names))

    #  掃wikipedia-dump，從company的wiki-page開始抓裡面的mentions
    #  將新加入的mentions設為next_entities，重複抓取n次（=爬n層）
    # print(f"取得跟公司關聯的mentions - {depth}階層")
    # titles = [v.split('/')[-1].replace("_", " ")
    #           for _, v in urls.items() if v is not None]

    print(f"# 連線elasticsearch（用於存放wiki-page, news-corpus）")
    es.connect(["es:9200"])

    print(f"# 以S&P500 wikipage為起點，爬'{explore_n_wk_depth}階層'的titles，建立graph")
    # seedtitles = ["List of S&P 500 companies"]
    seedtitles = ["Wilson (company)"]
    try:
        # raise FileNotFoundError
        g = gt.load_graph(wk_titles_graphml)
        print(f"File loaded: {wk_titles_graphml}")
    except FileNotFoundError:
        print(f"File not found, create new one")
        g = get_wktitles_graph(seedtitles, n_depth=explore_n_wk_depth)
        for n in g:
            g.nodes[n]['mentions'] = json.dumps(
                g.nodes[n]['mentions'], ensure_ascii=False, default=serialize_sets)
        nx.write_graphml_lxml(g, wk_titles_graphml)
        g = gt.load_graph(wk_titles_graphml)

    print("# 使用完整的graph跑pagerank（為避免記憶體不足，改用graph-tool庫）")
    ranks = load_or_run(
        wk_pagerank_json, lambda: calc_pagerank(g), forcerun=True)

    print(f"# 挑出graph中的wiki-category，再找主要描述此category的wiki-title")

    def _cat_tags() -> Iterable[str]:
        _, wk_title, _ = zip(*ranks)
        cats = filter(lambda e: "Category:" in e, wk_title)
        # print(list(cats))
        # print([c for c in cats])
        tags = [es.get_corresponded_wktitles(cat_title=c) for c in cats]
        tags = set(itertools.chain(*tags))
        # tags &= set(tags)
        return tags

    cat_tags = load_or_run(
        wk_cat_tags_json, lambda: _cat_tags(), forcerun=True)

    print(f"# 依照wk-title找尋對應的wkd-entity")

    # tags = ["Technology", "Internet", "Metal"]
    cattag_entity = get_matched_wkd_entities(cat_tags)
    ranks_by_tags = []
    for _, wk_title, pagerank in ranks:
        try:
            e = cattag_entity[wk_title]
            ranks_by_tags.append(
                (e.entity_id, e.get_enwiki_title(), e.get_label("zh"), pagerank))
        except KeyError:
            pass

    print("# 將ranks存成csv格式")
    wkd_id, wk_title, zh_label, pagerank = zip(*ranks_by_tags)
    tags = wk_title
    df = pd.DataFrame(
        {'wkd_id': wkd_id,
         'wk_title': wk_title,
         'zh_label': zh_label,
         'pagerank': pagerank})
    df.to_csv(wk_tags_pagerank_csv, index=False)

    return

    print("# 找一個ticker的tags")

    def get_neighbors(v: gt.Vertex, n_expands: int = 2):
        seeds = set([v])
        traveled = set()
        for i in range(n_expands):
            nextseeds = set()
            for v in seeds:
                nextseeds |= set(v.out_neighbors())
            nextseeds -= seeds
            traveled |= seeds
            seeds = nextseeds
        return traveled

    # tags = set(["joint venture"])
    tickers = ["Wilson (company)"]
    tags_by_tickers = []
    for tk in tickers:
        v = gt.find_vertex(g, g.vp['_graphml_vertex_id'], tk)[0]
        neighbors = get_neighbors(v, n_expands=2)
        neighbors = set([g.vp['_graphml_vertex_id'][v] for v in neighbors])
        tags_by_tickers.append((tk, tags & neighbors))
    print(tags_by_tickers)

    return

    print(f"tag的排序、重要度、重複性（用max_flow、n_path之類的方式）")
    # for tk in tickers:
    #     neighbors = get_neighbors(tk)

    print(f"TODO:巡所有的news，計算mentions的詞頻")

    # print(f"巡所有的news，計算mentions的詞頻")

    # TODO: 擴展同義詞（用於flashtext）
    # print(f"載入S&P500，做為seed-wk-titles")
    # df = pd.read_csv(sp500_csv)
    # seedtitles = list(df['Name'])

    # print(f"以seed-wk-titles為起點，爬'{explore_n_wk_depth}階層'的wk-titles，建立graph")
    # try:
    #     # raise FileNotFoundError
    #     g = gt.load_graph(wk_titles_graphml)
    #     print(f"File loaded: {wk_titles_graphml}")
    # except FileNotFoundError:
    #     print(f"File not found, create new one")
    #     g = get_wktitles_graph(seedtitles, n_depth=explore_n_wk_depth)
    #     for n in g:
    #         g.nodes[n]['mentions'] = json.dumps(
    #             g.nodes[n]['mentions'], ensure_ascii=False, default=serialize_sets)
    #     nx.write_graphml_lxml(g, wk_titles_graphml)
    #     g = gt.load_graph(wk_titles_graphml)

    # print(f"僅採用{adpot_n_wk_depth}-depth的wk-titles")
    # vp_label = g.vp['_graphml_vertex_id']
    # vp_depth = g.vp['depth']
    # wktitles = [vp_label[v]
    #             for v in g.vertices() if vp_depth[v] <= adpot_n_wk_depth]

    # print("掃wkd-dump，將沒有中文名、有位置claim（很可能是地點）、是人的wk-titles排除")
    # try:
    #     raise FileNotFoundError
    #     entities = WikidataJsonDump(wkd_filtered_entities_json)
    #     filtered_wktitles = set([e.get_enwiki_title() for e in entities])
    #     print(f"File loaded: {wkd_filtered_entities_json}")
    # except FileNotFoundError:
    #     print(f"File not found, create new one")
    #     entities = get_matched_wkd_entities(
    #         wktitles, wkd_dump_path=wkd_dump_json)
    #     dump_entities_to_json(entities, wkd_filtered_entities_json)
    #     filtered_wktitles = set([e.get_enwiki_title() for e in entities])

    # print("使用完整的graph跑pagerank（為避免記憶體不足，改用graph-tool庫）")
    # load_or_run(wk_filtered_pagerank_json,
    #             lambda: calc_pagerank(g, included_wktitles=filtered_wktitles), forcerun=True)

    return

    # es.connect(["twintdevcontainer_es_1:9200"])

    # print("從corpus中算mention的cooccur數，以及抓出同一文章的mentions(bags)")
    # sent_cooccur, atk_cooccur, atk_bags = count_corpus_mentions_cooccurs(mentions)
    # _ = load_or_run(sent_cooccurs_json, lambda: sent_cooccur, forcerun=True,)
    # _ = load_or_run(ark_cooccurs_json, lambda: atk_cooccur, forcerun=True,)
    # _ = load_or_run(atk_bags_json, lambda: atk_bags, forcerun=True,)

    # _ = load_or_run(
    #     (sent_cooccurs_json, ark_cooccurs_json, atk_bags_json),
    #     lambda: count_corpus_mentions_cooccurs(mentions),
    #     forcerun=True,)

    # print("從corpus中算mention的daily-frequency-count")
    # _ = load_or_run(
    #     freqs_json, lambda: count_corpus_mentions_freqs(mentions), forcerun=True)

    # return


if __name__ == "__main__":
    main()
