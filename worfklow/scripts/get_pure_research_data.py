import pandas as pd
import re
import requests
import os
from bs4 import BeautifulSoup
from dataclasses import dataclass
from simple_parsing import ArgumentParser
from loguru import logger
from workflow.scripts.general import mark_as_complete

parser = ArgumentParser()
parser.add_argument("--input", type=str, help="Input file prefix")
parser.add_argument("--output", type=str, help="Output file prefix")


@dataclass
class Options:
    """ Help string for this group of command-line arguments """

    top: int = -1  # How many to read

parser.add_arguments(Options, dest="options")

args = parser.parse_args()

# problem pages for abstract section headers
# https://research-information.bris.ac.uk/en/publications/skin-pigmentation-sun-exposure-and-vitamin-d-levels-in-children-o
# https://research-information.bris.ac.uk/en/publications/improving-womens-diet-quality-preconceptionally-and-during-gestat
# https://research-information.bris.ac.uk/en/publications/maternal-reproductive-hormones-and-angiogenic-factors-in-pregnanc

def read_file():
    df = pd.read_csv(f"{args.input}.tsv.gz", sep="\t")
    df.drop_duplicates(subset="url", inplace=True)
    df.dropna(inplace=True)
    logger.debug(df.head())
    return df


def create_research_data(df):
    data = []
    existing_data = []
    # check for existing data
    f = f"{args.output}.tsv.gz"

    if os.path.exists(f) and os.path.getsize(f) > 1:
        logger.info(f"Reading existing data {f}")
        existing_df = pd.read_csv(f, sep="\t")
        # print(existing_df)
        existing_data = list(existing_df["url"])
        # logger.debug(existing_data)
        try:
            data = existing_df.to_dict("records")
        except:
            logger.warning(f"Error when reading {f}")
        logger.debug(f"Got data on {len(existing_data)} urls")

    for i, rows in df.iterrows():
        if rows["url"] in existing_data:
            logger.debug(f"{rows['url']} already done")
        else:
            d = {
                "url": rows["url"],
                "title": rows["title"],
                "abstract": "NA",
            }
            abstract_data,pub_date = get_research_data(rows["url"])
            # logger.debug(abstract_data)
            try:
                abstract = abstract_data.getText(separator=" ").strip().replace("\n", " ")
                d["abstract"] = abstract
                #logger.debug(abstract)
            except:
                logger.warning(f"No abstract for {rows['url']}")
            d['year']=pub_date
            data.append(d)
    # logger.debug(data)
    research_details = pd.DataFrame(data)
    research_details.to_csv(f, sep="\t", index=False)
    mark_as_complete(args.output)


def get_research_data(url):
    logger.debug(url)
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    abstract_data = soup.find(
        "div",
        class_="rendering rendering_researchoutput rendering_researchoutput_abstractportal rendering_contributiontojournal rendering_abstractportal rendering_contributiontojournal_abstractportal",
    )
    pub_status = soup.find(
        "span",
        class_="date"
    )
    try:
        pub_date_text = pub_status.getText()
        m = re.search(r'[12]\d{3}',pub_date_text)
        if m:
            pub_date = m.group(0)
        else:
            pub_date = 'NA'
    except:
        pub_date = 'NA'
    logger.info(pub_date)
    #logger.info(abstract_data.getText(separator=" ").strip().replace("\n", " "))
    return abstract_data, pub_date

def test():
    url='https://research-information.bris.ac.uk/en/publications/improving-womens-diet-quality-preconceptionally-and-during-gestat'
    get_research_data(url)

if __name__ == "__main__":
    df = read_file()
    create_research_data(df)
    #test()