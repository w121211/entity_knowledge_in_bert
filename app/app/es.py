"""
Setup (once only)
$ python app/es.py

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
import asyncio
import base64
import bz2
import collections
from concurrent.futures import process
import datetime
import glob
import gzip
import html
import json
import os
import pickle
import re
import threading
import queue
from typing import Iterable, List, Tuple, Optional

import elasticsearch
import mwparserfromhell
from elasticsearch_dsl import connections, Document, Date, Keyword, Q, Search, Text, Range, Integer, Float
from qwikidata.entity import WikidataItem
from qwikidata.json_dump import WikidataJsonDump

# import wikitextparser as wtp


PAGE_ALIAS = "wiki-page"
PAGE_PATTERN = f"{PAGE_ALIAS}-*"
REDIRECT_ALIAS = "wiki-redirect"
REDIRECT_PATTERN = f"{REDIRECT_ALIAS}-*"
WKD_ALIAS = "wiki-data"
WKD_PATTERN = f"{WKD_ALIAS}-*"

# TICKER_ALIAS = "yahoo-ticker"
# TICKER_PATTERN = TICKER_ALIAS + '-*'


class WikiRedirect(Document):
    title = Keyword(required=True)  # as doc._id for quick search
    redirect = Keyword()            # redirect to wiki title
    redirect_wkid = Keyword()       # redirect to wiki id
    dbpedia_id = Keyword()

    class Index:
        name = REDIRECT_ALIAS
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0
        }

    def save(self, **kwargs):
        if 'id' not in self.meta:
            self.meta.id = self.title
        return super().save(**kwargs)


class WikiPage(Document):
    wkid = Keyword(required=True)  # wiki page id, use as doc._id
    mentions = Keyword(multi=True, index=False)  # json string

    # fields from cirrussearch
    template = Keyword(multi=True)
    content_model = Keyword()
    # opening_text
    wiki = Keyword()
    # auxiliary_text
    language = Keyword()
    title = Keyword()
    text = Text(index=False)  # escaped
    # defaultsort
    # timestamp
    redirect = Text(index=False)  # json string
    wikibase_item = Keyword()
    # version_type
    # heading
    source_text = Text(index=False)
    # coordinates
    # version
    # external_link
    # namespace_text
    namespace = Integer()
    # text_bytes
    # incoming_links
    category = Keyword(multi=True)
    outgoing_link = Keyword(multi=True)
    popularity_score = Float()
    # create_timestamp
    # ores_articletopics

    class Index:
        name = PAGE_ALIAS
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0, }

    def save(self, **kwargs):
        if 'id' not in self.meta:
            self.meta.id = self.wkid
        return super().save(**kwargs)

    # @classmethod
    # def match(cls, title: str, min_score: int = 10):
    #     s = cls.search()
    #     s = s.query("match", title=title)
    #     for hit in s.execute()[:1]:
    #         if hit.meta.score > min_score:
    #             return hit
    #     return None


class WikiData(Document):
    en_title = Keyword(required=True)  # en wiki title, use as doc._id
    wkd_id = Keyword(required=True)  # wiki data id
    json = Text(index=False)

    class Index:
        name = WKD_ALIAS
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 0, }

    def save(self, **kwargs):
        if 'id' not in self.meta:
            self.meta.id = self.en_title
        return super().save(**kwargs)


def search_wikipage(title: str, min_score: int = 10):
    s = WikiRedirect.search()
    s = s.query("match", title=title)
    for hit in s.execute()[:1]:
        if hit.meta.score > min_score:
            return hit
    return None


def get_wikipage(title: str) -> Optional[WikiPage]:
    try:
        redirect = WikiRedirect.get(id=title)
        return WikiPage.get(id=redirect.redirect_wkid)
    except elasticsearch.NotFoundError:
        redirect = search_wikipage(title)
        if redirect is None:
            return
        else:
            return WikiPage.get(id=redirect.redirect_wkid)


def get_corresponded_wktitles(cat_title: str) -> List[str]:
    """給wiki category title，找到該category的主要page titles"""
    excluded = set(["Help:Categories"])
    excluded_sub = ("Category:", "Wikipedia:", "Template:", "CAT:")

    page = get_wikipage(title=cat_title)
    if page is None:
        return []
    # try:
    titles = set(page.outgoing_link) - excluded
    titles = filter(lambda e: not any(
        f in e for f in excluded_sub), titles)
    titles = [t.replace("_", " ") for t in titles]
    return titles
    # except:
    #     return []


# ---------------
# Utilities
# ---------------


class ThreadsafeIter:
    """https://gist.github.com/platdrag/e755f3947552804c42633a99ffd325d4"""

    def __init__(self, it):
        self.it = it
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return self.it.__next__()


def threadsafe_generator(f):
    def g(*a, **kw):
        return ThreadsafeIter(f(*a, **kw))
    return g


def _parse_mentions(text: str) -> List[str: str]:
    """
    Retrun: List of tuples, eg [("wiki title", "mentioned text"), ...]
    """
    # parsed = wtp.parse(text)
    # mentions = [(k.title, k.text or k.title) for k in parsed.wikilinks]
    wikicode = mwparserfromhell.parse(text)
    links = wikicode.filter_wikilinks()

    mentions = []
    for k in links:
        title = k.title.strip_code()
        text = title if k.text is None else k.text.strip_code()
        mentions.append((title, text))
    return mentions


def import_from_cirrusdump(dump_path: str, n_threads: int, skip: int = 0, first_n: int = None) -> None:
    def convert_title(title: str, namespace: int) -> str:
        return f"Category:{title}" if namespace == 14 else title

    def processor(ln1: str, ln2: str) -> None:
        j1 = json.loads(ln1)
        j2 = json.loads(ln2)
        if j2["namespace"] not in (0, 14):
            return
        wkid = j1["index"]["_id"]
        title = convert_title(j2["title"], j2["namespace"])
        popularity_score = j2["popularity_score"] if "popularity_score" in j2 else None
        wikibase_item = j2["wikibase_item"] if "wikibase_item" in j2 else None

        for r in j2["redirect"]:
            if r["namespace"] not in (0, 14):
                return
            doc = WikiRedirect(
                title=convert_title(r["title"], r["namespace"]),
                redirect=title,
                redirect_wkid=wkid)
            doc.save()

        # allow self-redirect
        doc = WikiRedirect(
            title=title,
            redirect=title,
            redirect_wkid=wkid)
        doc.save()

        doc = WikiPage(
            title=title,
            wkid=wkid,
            mentions=json.dumps(
                _parse_mentions(j2["source_text"]),
                ensure_ascii=False),
            template=j2["template"],
            content_model=j2["content_model"],
            wiki=j2["wiki"],
            language=j2["language"],
            text=j2["text"],
            redirect=json.dumps(j2["redirect"], ensure_ascii=False),
            wikibase_item=wikibase_item,
            # source_text=j2["source_text"],
            namespace=j2["namespace"],
            category=j2["category"],
            outgoing_link=j2["outgoing_link"],
            popularity_score=popularity_score,)
        doc.save()

    @threadsafe_generator
    def readline():
        with gzip.open(dump_path, 'r') as f:
            for i, ln1 in enumerate(f):
                if first_n is not None and i > first_n:
                    print(f"Early stop: {first_n}")
                    break
                if i < skip:
                    continue
                if i % 10000 == 0:
                    print(i)
                ln2 = next(f)
                yield ln1, ln2

    readline = readline()
    q = queue.Queue()

    def worker():
        while True:
            try:
                ln1, ln2 = q.get()
                processor(ln1, ln2)
            except Exception as e:
                # print(ln2.decode())
                print(e)
                # break
                # print(meta, doc)
            try:
                q.put(next(readline))
            except StopIteration:
                pass
            finally:
                q.task_done()

    for _ in range(n_threads):
        threading.Thread(target=worker, daemon=True).start()

    for _ in range(n_threads * 3):
        q.put(next(readline))

    q.join()
    print('All work completed')


def import_from_wkddump(dump_path: str, skip: int = 0, first_n: int = None) -> None:
    for i, entity_dict in enumerate(WikidataJsonDump(dump_path)):
        if first_n is not None and i > first_n:
            print(f"Early stop at {first_n}")
            break
        if i < skip:
            continue
        if i % 10000 == 0:
            print(i)
        if entity_dict["type"] == "item":
            e = WikidataItem(entity_dict)
            doc = WikiData(
                en_title=e.get_enwiki_title(),
                wkd_id=e.entity_id,
                json=json.dumps(e._entity_dict),)
            doc.save()


def import_from_wikiextracted(path_to_wikiextracted_folder: str):
    print("Wikiextracted to elastic start")
    for p in glob.glob(f'{path_to_wikiextracted_folder}/**/*'):
        print(p)
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
                    title=html.unescape(j["title"]),
                    uid=j["id"],
                    url=j["url"],
                    text=j["text"],
                    mentions=json.dumps(mentions, ensure_ascii=False))
                p.save()
    print("Wikiextracted to elastic finished")


def wkredirects_to_elastic(path_to_dbpedia_folds: str):
    require_files = (
        "page_ids_en.ttl.bz2",
        "redirects_lang=en.ttl.bz2",
    )

    def scan_ttl(path: str, proc_one: callable):
        with bz2.BZ2File(path, "rb") as f:
            for i, l in enumerate(f):
                if i % 10000 == 0:
                    print(i)
                # if i > 10:
                #     break
                ln = l.decode().strip()
                if ln.startswith("#"):
                    continue
                s, p, o, _ = ln.split(" ")

                proc_one(s, p, o)

                # title, redirect = pattern.findall(ln)[0]
                # title = html.unescape(title)
                # redirect = html.unescape(redirect)

    # add dpid
    def _proc(s, p, o):
        u = UpdateByQuery(index='wiki-page').using(es)
        # u = u.query("term", uid="21550")
        u = u.query("term", uid=o)
        u = u.script(
            source="ctx._source.dpid=params.dpid",
            params={"dpid": s},)
        resp = u.execute()
    scan_ttl(path, _proc)

    # add redirect
    def _proc(s, p, o):
        u = UpdateByQuery(index='wiki-page').using(es)
        # u = u.query("term", uid="21550")
        u = u.query("term", uid=o)
        u = u.script(
            source="ctx._source.dpid=params.dpid",
            params={"dpid": s},)
        resp = u.execute()
    scan_ttl(path, _proc)

    def proc_uid(ln: str):
        s, p, o, _ = ln.split(" ")
        try:
            _ = WikiPage.get(redirect)
        except:
            pass
        try:
            _ = WikiPage.get(redirect)
        except elasticsearch.NotFoundError:
            # print(f"Redirect page not found: {redirect}")
            pass
        try:
            p = WikiPage.get(title)
        except elasticsearch.NotFoundError:
            p = WikiPage(title=title, redirect=redirect)
            p.save()
        else:
            if p.title != redirect:
                p.update(redirect=redirect)


def wkredirects_to_elastic(path_to_redirects_file: str):
    print("Wiki-redirects to elastic start")
    pattern = re.compile(
        "<http://dbpedia.org/resource/(.*)> <http://dbpedia.org/ontology/wikiPageRedirects> <http://dbpedia.org/resource/(.*)> .")
    with bz2.BZ2File(path_to_redirects_file, "rb") as f:
        for i, l in enumerate(f):
            if i % 10000 == 0:
                print(i)
            # if i > 10:
            #     break
            ln = l.decode().strip()
            if ln.startswith("#"):
                continue
            # try:
            #     title, redirect = pattern.findall(ln)[0]
            # except Exception as e:
            #     print(e)
            #     print(ln)
            title, redirect = pattern.findall(ln)[0]
            title = html.unescape(title)
            redirect = html.unescape(redirect)
            try:
                _ = WikiPage.get(redirect)
            except elasticsearch.NotFoundError:
                # print(f"Redirect page not found: {redirect}")
                continue
            try:
                p = WikiPage.get(title)
            except elasticsearch.NotFoundError:
                p = WikiPage(title=title, redirect=redirect)
                p.save()
            else:
                if p.title != redirect:
                    p.update(redirect=redirect)
    print("Wiki-redirects to elastic finished")


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


if __name__ == '__main__':
    connect(['es:9200'])
    # create_patterned_index(PAGE_ALIAS, PAGE_PATTERN)
    # create_patterned_index(REDIRECT_ALIAS, REDIRECT_PATTERN)
    # create_patterned_index(WKD_ALIAS, WKD_PATTERN)

    # Import cirrusdump: require both content & general dump
    # import_from_cirrusdump(
    #     "/workspace/entity_knowledge_in_bert/app/downloads/cirrus/enwiki-20200907-cirrussearch-content.json.gz",
    #     n_threads=20,
    #     first_n=100000,)
    import_from_wkddump(
        "/workspace/entity_knowledge_in_bert/app/downloads/latest-all.json.bz2",
        first_n=10000,
    )

    # Deprecated
    # wikiextracted_to_elastic("./downloads/wikiextracted")
    # wkredirects_to_elastic("../downloads/redirects_lang=en.ttl.bz2")
