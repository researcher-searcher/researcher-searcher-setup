import xmltodict
import gzip
import csv
import time
import requests
import subprocess
import re
import os
import json
from random import randint
from loguru import logger
from environs import Env

env = Env()
env.read_env()

OUTDIR = env.str("OUTDIR")
if not os.path.exists(OUTDIR):
    os.makedirs(OUTDIR)
PUBMEDDATA=f'{OUTDIR}/pubmed_data.tsv'

def read_existing():
    logger.info(f'Read existing downloaded pubmed data from {PUBMEDDATA}')
    pubData = []
    if os.path.exists(PUBMEDDATA):
        with open(PUBMEDDATA, newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t')
            for row in reader:
                pubData.append({'pmid': row[0], 'year': row[1], 'title': row[2], 'abstract': row[3]})
        logger.info(f'{len(pubData)-1} publication(s) already downloaded')
    else:
        o=open(PUBMEDDATA,'w')
        o.write('pmid\tyear\ttitle\tabstract\n')
        o.close()
    return pubData

#function to convert DOIs to PubMed IDs
def doi_to_pmid(doiList):
	baseurl='https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?tool=my_tool&email=ben.elsworth@bristol.ac.uk&ids='
	url = baseurl+",".join(doiList)+'&idType=doi&format=json'
	pmidList=set()
	try:
		resp = requests.get(url).json()
		if 'records' in resp:
			for record in resp['records']:
				if 'pmid' in record:
					pmidList.add(record['pmid'])
	except:
		logger.info('requests error')
	return list(pmidList)

def pubmed_xml_parser(pubmed_article):
    if 'MedlineCitation' in pubmed_article:
        if 'PMID' in pubmed_article['MedlineCitation']:
            pmid = pubmed_article['MedlineCitation']['PMID']['#text']
        else:
            logger.info('No PMID')
            return
        if 'Article' in pubmed_article['MedlineCitation']:
            if 'Abstract' in pubmed_article['MedlineCitation']['Article']:
                #logger.info(pubmed_article['MedlineCitation']['Article']['Abstract']['AbstractText'])
                abstract = pubmed_article['MedlineCitation']['Article']['Abstract']['AbstractText']
                if isinstance(abstract, list):
                    try:
                        abstract = abstract[0]['#text']
                    except Exception as e:
                        logger.warning(f'Abstract error {e}')
                elif isinstance(abstract, dict):
                    try:
                        abstract = abstract['#text']
                    except Exception as e:
                        logger.warning(f'Abstract error {e}')
            else:
                logger.info('No Abstract')
                abstract=''
                #return
            if 'ArticleTitle' in pubmed_article['MedlineCitation']['Article']:
                title = pubmed_article['MedlineCitation']['Article']['ArticleTitle']
            else:
                logger.info('No ArticleTitle')
                title=''
                #return
        else:
            logger.info('No Article')
            return
        if 'DateCompleted' in pubmed_article['MedlineCitation']:
            year = pubmed_article['MedlineCitation']['DateCompleted']['Year']
        else:
            #DataCompleted is missing for some
            year=0
            logger.info('No DateCompleted')
            #logger.info(pubmed_article['MedlineCitation'])
        return pmid,title,abstract,year
    else:
        logger.info(f'error {pubmed_article}')

def get_pubmed_data_efetch(pmids):
	logger.info(pmids)
	pubData = read_existing()

	#check if already done
	pmidsToDo = []
	for p in pmids:
		if any(d['pmid'] == p for d in pubData):
			logger.info(f'{p} is done')
		else:
			pmidsToDo.append(p)

	if len(pmidsToDo)>0:
		logger.info(f'Processing {pmidsToDo}')
		url='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi'
		params = {'db': 'pubmed', 'id':",".join(pmids), 'retmode':'xml'}
		r = requests.get(url, params=params)
		try:
			r = requests.get(url, params=params)
			records = xmltodict.parse(r.text,dict_constructor=dict)
			with open(PUBMEDDATA, 'a', newline='') as csvfile:
				fieldnames = ['pmid', 'year', 'title' , 'abstract']
				writer = csv.DictWriter(csvfile, fieldnames=fieldnames,delimiter='\t')
				pubData = []
				if type(records['PubmedArticleSet']['PubmedArticle']) != list:
					logger.info('single pmid')
					pubmed_article=records['PubmedArticleSet']['PubmedArticle']
					pmid,title,abstract,year=pubmed_xml_parser(pubmed_article)
					pubData.append({'pmid':pmid,'year':int(year),'title':title,'abstract':abstract})
					writer.writerow({'pmid': pmid, 'year': int(year), 'title': title, 'abstract': abstract})
				else:
					logger.info('multiple pmid')
					for pubmed_article in records['PubmedArticleSet']['PubmedArticle']:
						pmid,title,abstract,year=pubmed_xml_parser(pubmed_article)
						pubData.append({'pmid':pmid,'year':int(year),'title':title,'abstract':abstract})
						writer.writerow({'pmid': pmid, 'year': int(year), 'title': title, 'abstract': abstract})
		except:
				logger.info('esearch error')

	else:
		logger.info('Nothing to do')
	pubFilter=[]
	for p in pubData:
		if p['pmid'] in pmids:
			pubFilter.append(p)
	logger.info(f'{len(pubFilter)} article(s) returned')
	return pubFilter
