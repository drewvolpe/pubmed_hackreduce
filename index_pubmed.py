

from Bio import Medline, Entrez  # biopython
from datetime import datetime
import elasticsearch
import random
import re
import time


es_server_url = 'cluster-7-slave-00.sl.hackreduce.net:9200'


def index_article(record):

    d = {}
    d['docId'] = record['MedlineCitation']['PMID'].title()
    d['DateRevised'] = record['MedlineCitation']['DateRevised']
    d['DateCreated'] = record['MedlineCitation']['DateCreated']

    article_node = record['MedlineCitation']['Article']
    d['ArticleTitle'] = article_node['ArticleTitle']
    d['Language'] = article_node['Language']
    d['AuthorList'] = article_node.get('AuthorList', [])
    d['JournalTitle'] = article_node['Journal']['Title']
    d['JournalPubDate'] = article_node['Journal']['JournalIssue']['PubDate']
    d['PublicationTypeList'] = article_node['PublicationTypeList']

    mesh_list = []
    for heading in record['MedlineCitation']['MeshHeadingList']:
        mesh_list.append(heading['DescriptorName'].title())
    d['MeshHeadings'] = mesh_list

    es = elasticsearch.Elasticsearch([es_server_url])
    es.index(
        index='pubmed',
        doc_type='article',
        id=d['docId'],
        body=d
    )
    

def get_articles(doc_ids):
    handle = Entrez.efetch(db='pubmed', id='%s' % doc_ids, rettype='medline', retmode='xml')
    articles = Entrez.read(handle)
    return articles


def index_articles(start_id=50000, num_docs=1000, batch_size=100):
    doc_id = start_id
    
    while doc_id < start_id + num_docs:
        # Fetch and index batch_size articles at a time
        ids = [x for x in range(doc_id, doc_id + batch_size)]
        try:
            for article in get_articles(ids):
                index_article(article)

            print 'Indexed batch: %s - %s' % (doc_id, doc_id + batch_size)

        except Exception as e:
            print 'Exception: %s' % e

        time.sleep(0.4)  # play nice with their API
        doc_id += batch_size


if __name__ == "__main__":

    Entrez.email = "hackreduce@dewdrops.net" # tell them who we are
    
    print 'starting...'

    # start with Mar 2013:
    #     http://www.ncbi.nlm.nih.gov/pubmed/23000000?report=json&format=text
    start_id = 2300000
    num_docs = 10000
    print 'indexing %s articles starting with %s' % (num_docs, start_id)
    index_articles(start_id, num_docs)

    print 'done.'
