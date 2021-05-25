from loguru import logger

from scripts.pubmed_functions import get_pubmed_data_efetch
from scripts.common_functions import get_ids_from_orcid_public_api, orcid_to_pubmedData

pubData=get_pubmed_data_efetch(['123'])
#orcidData=get_ids_from_orcid_public_api('0000-0001-7328-4233')
#logger.info(orcidData)

orcid_to_pubmedData(['0000-0001-7328-4233'])