import csv
import glob
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# MAPPINGS = [REPORT_DAT	SHIFT	OFFENSE	METHOD	BLOCK	DISTRICT	PSA	WARD	ANC	NEIGHBORHOOD_CLUSTER	BLOCK_GROUP	CENSUS_TRACT	VOTING_PRECINCT	CCN	XBLOCK	YBLOCK	START_DATE	END_DATE

es = Elasticsearch(http_auth=('elastic', 'dccrime'))

es.indices.create(index='crimes', ignore=400)

es.put_mapping(index='crimes', doc_type='crime', body={
    'properties': {
        'report_dat': {'type': 'date'},
        'end_date': {'type': 'date'},
        'start_date': {'type': 'date'},
    }
})

def parse_date(string):
    """Parse a datestring"""
    return datetime.strptime(string, '%m/%d/%Y %I:%M:%S %p')

def actions():
    indexed = 0
    for datafile in glob.glob('./data/*.csv'):
        with open(datafile) as records:
            crimes = csv.DictReader(records)
            for crime in crimes:
                try:
                    doc = {field.lower(): key for field, key in crime.items()}
                    doc['report_dat'] = parse_date(doc['report_dat'])
                    if doc['start_date'] != '':
                        doc['start_date'] = parse_date(doc['start_date'])
                    else:
                        doc['start_date'] = None
                    if doc['end_date'] != '':
                        doc['end_date'] = parse_date(doc['end_date'])
                    else:
                        doc['end_date'] = None
                    yield {'_id': crime['CCN'], '_type': 'crime', '_index': 'crimes', '_source': doc}
                    # created = es.index(index='crimes', doc_type='crime', id=crime['CCN'], body=doc)
                    indexed += 1
                    if (indexed % 1000) == 0:
                        print('Indexed {} records'.format(indexed))
                except:
                    print('failed to print doc {}', doc)
                    raise

stats = bulk(es, actions())
print('Index {} documents'.format(stats[0]))
