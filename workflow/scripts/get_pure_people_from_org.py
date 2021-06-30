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
logger.debug(args)
logger.debug("options:", args.options.top)


def read_file():
    org_df = pd.read_csv(f"{args.input}", names=["org_url"])
    logger.info(org_df.head())
    return org_df


def pure_finder_org(df):
    person_list = []
    f = f"{args.output}.tsv.gz"
    for i, row in df.iterrows():
        org_url = row["org_url"]
        logger.debug(org_url)
        try:
            res = requests.get(org_url)
            soup = BeautifulSoup(res.text, "html.parser")
            d = soup.find_all("a", class_="link person")
            for i in d:
                # logger.info(len(person_list))
                if len(person_list) < args.options.top:
                    person_list.append({"org_id": org_url, "person_id": i["href"]})
            logger.debug(len(person_list))
            # check for pagination
            pagination = soup.find("nav", class_="pages")
            if pagination:
                for li in pagination.findAll("a", class_="step"):
                    page = int(li.getText())
                    logger.info(page)
                    research_url = f"{org_url}/?page={page-1}"
                    logger.debug(research_url)
                    res = requests.get(research_url)
                    soup = BeautifulSoup(res.text, "html.parser")
                    d = soup.find_all("a", class_="link person")
                    for i in d:
                        if len(person_list) < args.options.top:
                            person_list.append(
                                {"org_id": org_url, "person_id": i["href"]}
                            )
                        else:
                            continue
                    logger.debug(len(person_list))
        except:
            logger.warning(f"{org_url} failed")
    person_df = pd.DataFrame(person_list)
    person_df.to_csv(f, sep="\t", index=False)
    mark_as_complete(args.output)
    return person_list


if __name__ == "__main__":
    org_df = read_file()
    person_list = pure_finder_org(org_df)
