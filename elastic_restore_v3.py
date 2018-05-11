from __future__ import print_function

import argparse
import json

import requests

# es 1.3 2.0
#SEARCH_FOR_ALL = '{ "sort": [ "_doc" ], "query": { "match_all": {} }, "fields": ["*"], "_source": true }'
# es 5.x
SEARCH_FOR_ALL = '{ "sort": [ "_doc" ], "query": { "match_all": {} }, "stored_fields": ["*"], "_source": true }'
#SEARCH_FOR_ALL = '{"query": { "match_all": {} }}'

parser = argparse.ArgumentParser(description="SANTA'S LITTLE HELPER\n")
parser.add_argument('--endpoint', dest='endpoint', type=str, default=None,
                    help='specify the endpoint of the ElasticSearch cluster')
parser.add_argument('--scroll', dest='scroll', type=int, default=50,
                    help='specify the scroll time (in minutes) of the single windows of data - default 50')
parser.add_argument('--index', dest='index_name', type=str, default=None,
                    help='specify the index name to dump')


def main():
    args = parser.parse_args()
    host = args.endpoint

    _scroll_id = None
    while 1:
        if _scroll_id is None:
            if args.index_name:
                r = requests.get('http://{}/{}/_search?scroll={:d}m'.format(host, args.index_name, args.scroll),
                                 data=SEARCH_FOR_ALL, headers = {'Content-Type': 'application/json'})
            else:
                r = requests.get('http://{}/_search?scroll={:d}m'.format(host, args.scroll), data=SEARCH_FOR_ALL, headers = {'Content-Type': 'application/json'})
        else:
            r = requests.get('http://{}/_search/scroll?scroll={:d}m&scroll_id={}'.format(host, args.scroll, _scroll_id), data={}, headers = {'Content-Type': 'application/json'})
        try:
            j2 = r.json()
        except:
            continue
#        print (j2)
        if j2.get('status', None) == 404:
            break
        if len(j2['hits']['hits']) == 0 or ('found' in j2 and not j2['found']):
            break
        for record in j2['hits']['hits']:
            source = record['_source']
            record = {k:v for k,v in record.items() if k in ('_index','_type','_id')}
            print(json.dumps(record))
            print(json.dumps(source))
        _scroll_id = j2['_scroll_id']

if __name__ == "__main__":
    main()






