import pandas as pd

from loguru import logger
from simple_parsing import ArgumentParser
from scripts.pubmed_functions import get_pubmed_data_efetch
from scripts.common_functions import get_ids_from_orcid_public_api, orcid_to_pubmedData

from environs import Env

env = Env()
env.read_env()

parser = ArgumentParser()
parser.add_argument("--input", type=str, help="Input file prefix")
parser.add_argument("--output", type=str, help="Output file prefix")

DATADIR = env.str("DATADIR")

@dataclass
class Options:
    """ Help string for this group of command-line arguments """

    top: int = -1  # How many emails to read

parser.add_arguments(Options, dest="options")

args = parser.parse_args()
logger.debug(args)
logger.debug("options:", args.options.top)


person_orcid_df = pd.read_csv(args.input)

orcid_list = list(person_orcid_df['id'])
orcid_to_pubmedData(orcid_list)