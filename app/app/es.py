"""
Setup (once only)
$ python es.py

Docker network
$ sudo docker network create twint-wikilink
$ sudo docker network connect twint-wikilink <wikilink-app-container-id>
$ sudo docker network connect twint-wikilink <twint-es-container-id>
$ ping -c 2 twintdevcontainer_es_1 -p 9200

Elasticsdump
$ multielasticdump --direction=dump --match='^.*$' --fsCompress --input=http://es:9200 --output=esdump_2020****
$ multielasticdump --direction=load --match='^.*$' --fsCompress --output=http://es:9200 --input=esdump_2020****
"""
from __future__ import annotations
import base64
import collections
import datetime
import glob
import json
import pickle
from typing import Iterable, List, Tuple, Optional

import elasticsearch
from elasticsearch_dsl import connections, Document, Date, Keyword, Q, Search, Text, Range, Integer


PAGE_ALIAS = "wiki-page"
PAGE_PATTERN = PAGE_ALIAS + '-*'
TICKER_ALIAS = "yahoo-ticker"
TICKER_PATTERN = TICKER_ALIAS + '-*'


class WikiPage(Document):
    title = Keyword(required=True)
    uid = Keyword(index=False)
    url = Keyword(index=False)
    text = Text(index=False)
    mentions = Keyword(index=False, multi=True)

    class Index:
        name = PAGE_ALIAS
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }

    def save(self, **kwargs):
        if 'id' not in self.meta:
            self.meta.id = self.title
        return super().save(**kwargs)


# class YahooTicker(Document):
#     symbol = Keyword(required=True)
#     info = Text(index=False)  # json string

#     class Index:
#         name = TICKER_ALIAS
#         settings = {
#             'number_of_shards': 1,
#             'number_of_replicas': 0
#         }

#     def save(self, **kwargs):
#         if 'id' not in self.meta:
#             self.meta.id = self.symbol
#         return super().save(**kwargs)

# ---------------
# Utilities
# ---------------


def wikiextracted_to_elastic(wikiextracted_folder_path: str):
    print("Wikiextracted to elastic start")
    for p in glob.glob(f'{wikiextracted_folder_path}/**/*'):
        with open(p) as f:
            for ln in f.readlines():
                j = json.loads(ln)
                charoffsets_mentions = pickle.loads(
                    base64.b64decode(j['internal_links'].encode('utf-8')))
                mentions = [
                    (char_start, char_end, mention, wiki_page_name)
                    for ((char_start, char_end), (mention, wiki_page_name)) in charoffsets_mentions.items()
                ]
                p = WikiPage(
                    title=j["title"],
                    uid=j["id"],
                    url=j["url"],
                    text=j["text"],
                    mentions=json.dumps(mentions, ensure_ascii=False))
                p.save()
    print("Wikiextracted to elastic finished")


def scan_scraper_page(url_filter: str, sorted: bool = False) -> Iterable[Document]:
    es = connections.get_connection()
    s = Search(using=es, index="scraper-page")
    q = Q('wildcard', resolved_url=url_filter) & Q("term", http_status=200)
    s = s.query(q)

    if sorted:
        s = s.sort('article_published_at')
        s = s.params(preserve_order=True)
    # resp = s.scan()
    # print(resp.hits.total)

    visited = set()
    for i, hit in enumerate(s.scan()):
        # if i > 100:
        #     break
        if hit.resolved_url in visited:
            continue
        visited.add(hit.resolved_url)
        yield hit


# ---------------
# Setup functions
# ---------------


def create_patterned_index(alias: str, pattern: str, create_alias: bool = True) -> None:
    """Run only one time to setup"""
    name = pattern.replace(
        '*', datetime.datetime.now().strftime('%Y%m%d%H%M'))
    # create_index
    es = connections.get_connection()
    es.indices.create(index=name)
    if create_alias:
        es.indices.update_aliases(body={
            'actions': [
                {"remove": {"alias": alias, "index": pattern}},
                {"add": {"alias": alias, "index": name}},
            ]
        })


def migrate(src, dest):
    es = connections.get_connection()
    es.reindex(body={"source": {"index": src}, "dest": {"index": dest}})
    es.indices.refresh(index=dest)


def connect(hosts: List[str]):
    # c = connections.Connections()
    # c.configure(default={"hosts": ["es.com"]}, local={"hosts": ["localhost"]})
    # c.remove_connection("default")
    connections.create_connection(hosts=hosts, timeout=20)


def setup(move: bool = False):
    create_patterned_index(PAGE_ALIAS, PAGE_PATTERN)
    create_patterned_index(TICKER_ALIAS, TICKER_PATTERN)
    # if move:
    #     migrate("wiki-page", PAGE_ALIAS)


if __name__ == '__main__':
    connect(['es:9200'])
    # seed()
    # setup(move=False)
    # migrate("news_rss", RSS_ALIAS)
    # create_patterned_index(PAGE_ALIAS, PAGE_PATTERN)
    create_patterned_index(TICKER_ALIAS, TICKER_PATTERN)
