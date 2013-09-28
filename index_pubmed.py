

from Bio import Medline, Entrez  # biopython
from datetime import datetime
import elasticsearch
from lxml import etree
import random
import re
import time


es_server_url = 'cluster-7-slave-00.sl.hackreduce.net:9200'


def index_article(record, doc_id):

    d = {}
    d['docId'] = doc_id
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
        id=doc_id,
        body=d
    )
    

def get_article(doc_id):
    handle = Entrez.efetch(db='pubmed', id='%s' % doc_id, rettype='medline', retmode='xml')
    articles = Entrez.read(handle)
    if not articles:
        return None
    return articles[0]


def index_articles(start_id=50000, num_docs=1000):
    for doc_id in range(start_id, start_id + num_docs):
        try:
            article = get_article(doc_id)
            index_article(article, doc_id)
        except Exception as e:
            print 'Exception: %s' % e

        if random.randint(0, 10) == 0:
            print 'Indexed doc: %s' % doc_id
        time.sleep(0.5)  # play nice


if __name__ == "__main__":

    Entrez.email = "hackreduce@dewdrops.net" # tell them who we are
    
    print 'starting...'

    start_id = 50800
    num_docs = 1000
    print 'indexing %s articles starting with %s' % (num_docs, start_id)
    index_articles(start_id, num_docs)

    print 'done.'
