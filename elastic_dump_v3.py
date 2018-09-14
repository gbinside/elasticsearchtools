from __future__ import print_function

from threading import Thread
from random import choice
import argparse
import json

try:  # py3
    from urllib.parse import urlparse, urlencode
    from urllib.request import urlopen, Request
    from urllib.error import HTTPError
    from queue import Queue
except ImportError:  # py2
    from urlparse import urlparse
    from urllib import urlencode
    from urllib2 import urlopen, Request
    from Queue import Queue

# es 1.3 2.0
# SEARCH_FOR_ALL = '{ "sort": [ "_doc" ], "query": { "match_all": {} }, "fields": ["*"], "_source": true }'
# es 5.x
SEARCH_FOR_ALL = u'{ "sort": [ "_doc" ], "query": { "match_all": {} }, "stored_fields": ["*"], "_source": true, ' \
                 u'"size": %i }'
# SEARCH_FOR_ALL = '{"query": { "match_all": {} }}'

parser = argparse.ArgumentParser(description="SANTA'S LITTLE HELPER\n")
parser.add_argument('--endpoint', dest='endpoint', type=str, default=None,
                    help='specify the endpoint of the ElasticSearch cluster')
parser.add_argument('--size', dest='size', type=int, default=1000,
                    help='specify the size of the data returned by 1 call - default 1000')
parser.add_argument('--scroll', dest='scroll', type=int, default=50,
                    help='specify the scroll time (in minutes) of the single windows of data - default 50')
parser.add_argument('--index', dest='index_name', type=str, default=None,
                    help='specify the index name to dump')


def print_data(q):
    while 1:
        for record in q.get():
            source = record['_source']
            record = {"index": {k: v for k, v in record.items() if k in ('_index', '_type', '_id')}}
            print(json.dumps(record))
            print(json.dumps(source))
        q.task_done()


def main():
    args = parser.parse_args()
    host = [x.strip() for x in args.endpoint.split(',')]
    search_query = (SEARCH_FOR_ALL % args.size).encode('utf-8')
    empty_query = u'{}'.encode('utf-8')

    q = Queue()
    t = Thread(target=print_data, args=(q,))
    t.daemon = True
    t.start()

    _scroll_id = None
    while 1:
        if _scroll_id is None:
            if args.index_name:
                req = Request('http://{0}/{1}/_search?scroll={2:d}m'.format(choice(host), args.index_name, args.scroll),
                              data=search_query)
                req.add_header('Content-Type', 'application/json')
                req.get_method = lambda: 'GET'
                r = urlopen(req)
            else:
                req = Request('http://{0}/_search?scroll={1:d}m'.format(choice(host), args.scroll),
                              data=search_query)
                req.add_header('Content-Type', 'application/json')
                req.get_method = lambda: 'GET'
                r = urlopen(req)
        else:
            req = Request(
                'http://{}/_search/scroll?scroll={:d}m&scroll_id={}'.format(choice(host), args.scroll, _scroll_id),
                data=empty_query)
            req.get_method = lambda: 'GET'
            req.add_header('Content-Type', 'application/json')
            r = urlopen(req)
        try:
            j2 = json.load(r.fp)
        except:
            continue
        try:
            (j2['hits']['hits'])
        except:
            print(j2)
        if j2.get('status', None) == 404:
            break
        if len(j2['hits']['hits']) == 0 or ('found' in j2 and not j2['found']):
            break
        q.put(j2['hits']['hits'])
        _scroll_id = j2['_scroll_id']
    q.join()


if __name__ == "__main__":
    main()
