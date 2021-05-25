import requests
import json
import os
from loguru import logger
from scripts.pubmed_functions import doi_to_pmid, get_pubmed_data_efetch
from environs import Env

env = Env()
env.read_env()

DATADIR = DATADIR = env.str("DATADIR")
if not os.path.exists(DATADIR):
    os.makedirs(DATADIR)
ORCIDFILE = f'{DATADIR}/orcid_data.tsv'

def get_ids_from_orcid_public_api(orcid):
	resp = requests.get("http://pub.orcid.org/"+orcid+"/works/",
	                    headers={'Accept':'application/orcid+json'})
	results = resp.json()
	pubData = []
	if 'group' in results:
		for i, result in enumerate( results['group']):
			pubDic={}
			if 'external-ids' in result:
				for e in result['external-ids']['external-id']:
					if e['external-id-type']=='pmid':
						pmid = e['external-id-value']
						pubDic['pmid']=pmid
					elif e['external-id-type']=='doi':
						doi = e['external-id-value']
						pubDic['doi']=doi
			if len(pubDic)>0:
				pubData.append(pubDic)
	else:
		logger.info('no data found')
	return pubData

#wrapper function to get PubMed data from list of ORCID IDs
def orcid_to_pubmedData(orcid_ids:list):
    if not isinstance(orcid_ids, list):
        logger.warning(f'{orcid_ids} is not a list!')
        exit()
    orcidPmidData=[]
    #check for existing data first
    if os.path.exists(ORCIDFILE):
        with open(ORCIDFILE) as f:
            for line in f:
                orcid,pmid = line.split('\t')
                orcidPmidData.append({'orcid':orcid,'pmid':pmid})
        orcidFile=open(ORCIDFILE,'a')
    else:
        orcidFile=open(ORCIDFILE,'w')
        orcidFile.write('orcid\tpmid\n')

    pubData=[]
    counter=0
    #for each orcid in list
    for o in orcid_ids:
        counter+=1
        if not any(d['orcid'] == o for d in orcidPmidData):
            print(counter,'Getting ORCID data for',o)
            orcidData=get_ids_from_orcid_public_api(o)
            pubMedIDs = set()
            doiIDs = set()
            for i in orcidData:
                if 'pmid' in i:
                    pubMedIDs.add(i['pmid'])
                if 'doi' in i:
                    doiIDs.add(i['doi'])
            logger.info(f'{len(pubMedIDs)} PMIDs')
            logger.info(f'{len(doiIDs)} DOIs')
            #convert DOIs to PubMedIDs
            doi_pmid=doi_to_pmid(list(doiIDs))
            allPMIDs = list(set(list(pubMedIDs)+list(doi_pmid)))
            pubData=get_pubmed_data_efetch(allPMIDs)
            for p in allPMIDs:
                orcidFile.write(o+'\t'+p+'\n')
    orcidFile.close()
    return pubData

#load orcid to pmid data
def load_orcid():
    logger.info('load_orcid')
    orcidToPubmedID={}
    with open(config.demoOrcidFile) as f:
        next(f)
        for line in f:
            orcid,pmid = line.rstrip().split('\t')
            if orcid in orcidToPubmedID:
                orcidToPubmedID[orcid].append(pmid)
            else:
                orcidToPubmedID[orcid]=[pmid]
    return orcidToPubmedID

#load the publication data
# def load_pubmed():
#     print('load_pubmed')
#     pubmedText={}
#     with open(config.demoPubmedFile, newline='') as csvfile:
#         reader = csv.reader(csvfile, delimiter='\t')
#         next(reader, None)
#         for row in reader:
#             text=row[2]+' '+row[3]
#             pubmedText[row[0]]=text
#     return pubmedText

#create dictionary of orcid to publication text
# def orcid_to_pubmed():
#     print('orcid_to_pubmed')
#     orcidToPubmedID=load_orcid()
#     pubmedText = load_pubmed()
#     orcidToPubmed={}
#     for orcid in orcidToPubmedID:
#         oText=''
#         for p in orcidToPubmedID[orcid]:
#             if p in pubmedText:
#                 oText+=(pubmedText[p])
#         orcidToPubmed[orcid]=oText
#     return orcidToPubmed
