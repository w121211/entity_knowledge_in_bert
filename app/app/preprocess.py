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
import json
import pathlib
import time
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, List, Tuple, Optional, Set, Union

import elasticsearch
import pandas as pd
import networkx as nx
import requests
import requests_cache
import spacy
import yfinance as yf
from dotenv import load_dotenv
from elasticsearch_dsl import connections, Document, Date, Keyword, Q, Search, Text, Range, Integer
from flashtext import KeywordProcessor

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


def get_wkmentions_graph(seedtitles: Iterable[str], depth: int) -> nx.DiGraph:
    """
    Return:
        DiDiGraph: (node, {mentions: json_str, depth: int})
    Usage:
        G.nodes["nodename"]["mentions"]
        G.nodes["nodename"]["depth"]
        G.out_degree("nodename")
    """
    def query(title: str):
        s = es.WikiPage.search()
        s = s.query("match", title=title)
        for hit in s.execute()[:1]:
            if hit.meta.score > 10:
                return hit
        return None

    def get_mentions(title: str) -> Tuple[bool, List[Tuple[str, str]]]:
        unfounded = False
        mentions = []
        try:
            page = es.WikiPage.get(id=title)
        except elasticsearch.NotFoundError:
            page = query(title)
            if page is None:
                unfounded = True
        else:
            mentions = json.loads(page.mentions)
        return unfounded, mentions

    G = nx.DiGraph()
    seeds = seedtitles
    for i in range(depth):
        print(f"Depth {i}: {len(seeds)} nodes")
        for seed in seeds:
            unfounded, mentions = get_mentions(seed)
            if seed in G:
                G.nodes[seed]['depth'] = i
                G.nodes[seed]['unfounded'] = unfounded
            else:
                G.add_node(seed, depth=i, unfounded=unfounded, mentions=set())

            for _, _, mention, wktitle in mentions:
                G.add_edge(seed, wktitle)
                try:
                    G.nodes[wktitle]['mentions'].add(mention)
                except KeyError:
                    G.nodes[wktitle]['mentions'] = {mention}
        seeds = [n for n in G if not 'depth' in G.nodes[n]]
    return G

# def


def get_matched_wkdatas_from_dump(wktitles: List[str]):
    print("掃整個wikidata-dump，將沒有中文名")

    def is_matched(e):
        if e.get_label("cn"):
            pass
        if e.get_claims("loc"):
            pass

    matched = set()
    for e in wikidata:
        if not e in wktitles:
            continue
        if is_matched(e):
            matched.add(e)
    return matched


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
    mentions_depth = 1

    output_folder = './outputs'
    downlaods_folder = './downloads'
    sp500_csv = f"{downlaods_folder}/s_and_p_500.csv"
    entities_json = f"{output_folder}/wd_entities.json"
    tk_csv = './downloads/bats_symbols_traded_byx.csv'
    tk_info_json = "./downloads/iex_ticker_info.json"
    urls_json = f"{output_folder}/wiki_urls.json"
    mentions_json = f"{output_folder}/wiki_mentions.json"
    sent_cooccurs_json = f"{output_folder}/corpus_mentions_sent_cooccurs.json"
    ark_cooccurs_json = f"{output_folder}/corpus_mentions_atk_cooccurs.json"
    atk_bags_json = f"{output_folder}/corpus_mentions_atk_bags.json"
    freqs_json = f"{output_folder}/corpus_mentions_freqs.json"
    wkmentions_graphml = f"{output_folder}/wk_mentions.graphml.bz2"

    pathlib.Path(output_folder).mkdir(exist_ok=True)

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

    print(f"連線elasticsearch（用於存放wiki-page, news-corpus）")
    es.connect(["es:9200"])

    #  掃wikipedia-dump，從company的wiki-page開始抓裡面的mentions
    #  將新加入的mentions設為next_entities，重複抓取n次（=爬n層）
    # print(f"取得跟公司關聯的mentions - {depth}階層")
    # titles = [v.split('/')[-1].replace("_", " ")
    #           for _, v in urls.items() if v is not None]

    # TODO: 擴展同義詞（用於flashtext）
    print(f"以S&P500為起點爬'{mentions_depth}階層'的wiki-mentions，建立graph")
    df = pd.read_csv(sp500_csv)
    titles = list(df['Name'])
    # G = nx.read_graphml(wkmentions_graphml)
    try:
        G = nx.read_graphml(wkmentions_graphml)
        for n in G:
            G.nodes[n]['mentions'] = json.loads(
                G.nodes[n]['mentions'])
    except FileNotFoundError:
        G = get_wkmentions_graph(titles, depth=mentions_depth)
        for n in G:
            G.nodes[n]['mentions'] = json.dumps(
                G.nodes[n]['mentions'], ensure_ascii=False, default=serialize_sets)
        nx.write_graphml_lxml(G, wkmentions_graphml)

    # print("找出Graph中重要的nodes: Pagerank")
    # pr = nx.pagerank(G)
    # print(G.nodes)

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
