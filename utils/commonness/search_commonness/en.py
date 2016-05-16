from __future__ import division
#-*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import time
from whoosh.qparser import QueryParser
from whoosh.qparser import MultifieldParser
from whoosh.query import *
from whoosh.filedb.filestore import FileStorage
from whoosh.filedb.filestore import copy_to_ram
from whoosh import scoring

index_dir = '/data/m1/panx2/data/KBs/dump/wiki-dump/enwiki-20160305/output/commonness/commonness_index'
st = FileStorage(index_dir)
# st = copy_to_ram(st)
ix = st.open_index()

def search(text, limit=None):
    res = list()
    text = text.decode('utf-8')
    with ix.searcher() as searcher:
        parser = QueryParser('text', ix.schema)
        query = parser.parse(text)
        results = searcher.search(query, limit=limit, terms=True)
        for i in results:
            res.append(dict(i))
    return res
