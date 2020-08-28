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
from typing import Any, Callable, Dict, Iterable, List, Tuple, Optional, Set

import elasticsearch
import pandas as pd
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
    if isinstance(obj, set):
        return list(obj)
    return obj


def load_or_run(filepath: str, fn: Callable) -> Any:
    try:
        with open(filepath, encoding='utf-8') as f:
            print(f"- Load from '{filepath}', skip running")
            return json.load(f)
    except:
        results = fn()
        with open(filepath, "w", encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, default=serialize_sets)
        return results


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


def get_wikipage_mentions(starts: Iterable[str], depth: int) -> List[str]:
    es.connect()
    mentions = dict()

    def query(search: str):
        s = es.WikiPage.search()
        s = s.query("match", title=search)
        for hit in s.execute()[:1]:
            if hit.meta.score > 10:
                return hit
        return None

    def get_mentions(title: str) -> Tuple[Set[str], Set[str]]:
        founds = set()
        unfounds = set()
        try:
            page = es.WikiPage.get(id=title)
        except elasticsearch.NotFoundError:
            page = query(title)
            if page is None:
                unfounds.add(title)
                return founds, unfounds

        for m in json.loads(page.mentions):
            _, _, mention, wktitle = m
            mentions[wktitle] = mentions.get(wktitle, set()) | {mention}
            founds.add(wktitle)
        return founds, unfounds

    starts, visited, unfounds = set(starts), set(), set()
    for i in range(depth):
        print(f"Depth {i}: {len(starts)} nodes")
        founds = set()
        for s in starts:
            _founds, _unfounds = get_mentions(s)
            founds |= _founds
            unfounds |= _unfounds
        visited |= starts
        starts = founds - visited - unfounds

    return {
        "mentions": mentions,
        "unfounds": unfounds,
    }


def expand_word(word: str):
    pass


def count_corpus_mentions_cooccurs(mentions: Dict[str, List[str]]) -> Any:
    def count_cooccur(bag: Iterable[str], cooccur: Dict[str, Dict[str, int]] = dict()) -> Dict[str, Dict[str, int]]:
        for a in bag:
            cooccur[a] = cooccur.get(a, dict())
            for b in bag:
                cooccur[a][b] = cooccur[a].get(b, 0) + 1
        return cooccur

    es.connect()
    nlp = spacy.load('en_core_web_sm')
    processor = KeywordProcessor()
    processor.add_keywords_from_dict(mentions)

    sent_cooccur = dict()
    atk_cooccur = dict()
    atk_bags = []
    for hit in es.scan_scraper_page("cnbc"):
        sents = []
        try:
            sents.append(hit.article_title)
        except:
            pass
        try:
            sents += [sent.text for sent in nlp(hit.article_text).sents]
        except:
            pass

        bag = set()
        for sent in sents:
            kws = processor.extract_keywords(sent, span_info=False)
            sent_cooccur = count_cooccur(kws, sent_cooccur)
            bag |= set(kws)
        atk_cooccur = count_cooccur(bag, atk_cooccur)
        atk_bags.append(bag)

    return {
        # 在一個句子中，兩個entity同時出現的次數: {a: {a: 4, b: 2, c:1}, b: {...}}
        "sent_cooccur": sent_cooccur,
        # 在一篇文章中，兩個entity同時出現的次數: {a: {a: 4, b: 2, c:1}, b: {...}}
        "atk_cooccur": atk_cooccur,
        # 在一篇文章中出現的entities集合成一個bag: [{a, b, c}, {a, c, f}, ...]
        "atk_bags": atk_bags,
    }


def count_corpus_mentions_freqs(mentions: Dict[str, List[str]]) -> Dict[str, Dict[int, int]]:
    es.connect()
    processor = KeywordProcessor()
    processor.add_keywords_from_dict(mentions)

    # {"aaa": {1230: 2, 1231: 1, 1233: 5}, "bbb": {...}}
    freq = defaultdict(lambda: defaultdict(int))
    for hit in es.scan_scraper_page("cnbc"):
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

    output_folder = './outputs'
    entities_json = f"{output_folder}/wd_entities.json"
    tk_csv = './downloads/bats_symbols_traded_byx.csv'
    tk_info_json = "./downloads/iex_ticker_info.json"
    urls_json = f"{output_folder}/wiki_urls.json"
    mentions_json = f"{output_folder}/wiki_mentions.json"
    cooccurs_json = f"{output_folder}/corpus_mentions_cooccurs.json"
    freqs_json = f"{output_folder}/corpus_mentions_freqs.json"
    depth = 2

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

    #  掃wikipedia-dump，從company的wiki-page開始抓裡面的mentions
    #  將新加入的mentions設為next_entities，重複抓取n次（=爬n層）
    # print(f"取得跟公司關聯的mentions - {depth}階層")
    # titles = [v.split('/')[-1].replace("_", " ")
    #           for _, v in urls.items() if v is not None]

    print(f"取得S&P500關聯的mentions - {depth}階層")
    # titles = [v.split('/')[-1].replace("_", " ")
    #           for _, v in urls.items() if v is not None]
    titles = ["List_of_S&P_500_companies"]
    # titles = ["List_of_S%26P_500_companies"]
    mentions = load_or_run(
        mentions_json, lambda: get_wikipage_mentions(titles, depth=depth))
    mentions = mentions["mentions"]
    # for k, v in mentions.items():
    #     print(k, v)
    #     break
    #     if not isinstance(v, list):
    #         print(k, v)

    # 擴展同義詞（用於flashtext）
    print("從corpus中算mention的cooccur數，以及抓出同一文章的mentions(bags)")
    _ = load_or_run(
        cooccurs_json, lambda: count_corpus_mentions_cooccurs(mentions))

    print("從corpus中算mention的daily-frequency-count")
    _ = load_or_run(
        freqs_json, lambda: count_corpus_mentions_freqs(mentions))


if __name__ == "__main__":
    main()
