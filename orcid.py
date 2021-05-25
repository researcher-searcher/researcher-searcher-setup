import pandas as pd

from loguru import logger

from scripts.pubmed_functions import get_pubmed_data_efetch
from scripts.common_functions import get_ids_from_orcid_public_api, orcid_to_pubmedData

from environs import Env

env = Env()
env.read_env()

DATADIR = env.str("DATADIR")

person_orcid_df = pd.read_csv(f'{DATADIR}/person_orcid.csv')

orcid_list = list(person_orcid_df['id'])
orcid_to_pubmedData(orcid_list)