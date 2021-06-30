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
    person_df = pd.read_csv(f"{args.input}.tsv.gz", sep="\t")
    logger.debug(person_df.head())
    return person_df


def create_research_data(person_df):
    data = []
    existing_data = []
    # check for existing data
    f = f"{args.output}.tsv.gz"

    if os.path.exists(f) and os.path.getsize(f) > 1:
        logger.info(f"Reading existing data {f}")
        existing_df = pd.read_csv(f, sep="\t")
        # print(existing_df)
        existing_data = list(existing_df["person_id"].unique())
        # logger.debug(existing_data)
        try:
            data = existing_df.to_dict("records")
        except:
            logger.warning(f"Error when reading {f}")
        logger.debug(f"Got data on {len(existing_data)}")

    for i, rows in person_df.iterrows():
        if rows["person_id"] in existing_data:
            logger.debug(f"{rows['person_id']} already done")
        else:
            url = rows["person_id"]
            if not url.startswith("https:"):
                logger.warning(f"Bad URL: {url}")
                continue
            research_output = get_research_output(url)
            # add empty row if no data
            if len(research_output) == 0:
                data.append(
                    {"person_id": rows["person_id"], "url": "NA", "title": "NA"}
                )
            # research_output = get_research_output('https://research-information.bris.ac.uk/en/persons/benjamin-l-elsworth')
            for r in research_output:
                # logger.debug(i)
                data.append(
                    {
                        "person_id": rows["person_id"],
                        "url": r["href"],
                        "title": r.getText(separator=" ").strip().replace("\n", " "),
                    }
                )
    # logger.debug(d)
    research_df = pd.DataFrame(data)
    research_df.to_csv(f, sep="\t", index=False)
    mark_as_complete(args.output)


def get_research_output(url):
    logger.debug(url)
    research_output = []
    try:
        research_url = f"{url}/publications"
        res = requests.get(research_url)
        soup = BeautifulSoup(res.text, "html.parser")
        research_output.extend(
            soup.find_all("a", class_="link", rel=re.compile("ContributionTo.*"))
        )
        logger.debug(len(research_output))
        # check for pagination
        pagination = soup.find("nav", class_="pages")
        if pagination:
            for li in pagination.findAll("a", class_="step"):
                page = int(li.getText())
                logger.info(page)
                research_url = f"{url}/publications/?page={page-1}"
                logger.debug(research_url)
                res = requests.get(research_url)
                soup = BeautifulSoup(res.text, "html.parser")
                research_output.extend(
                    soup.find_all(
                        "a", class_="link", rel=re.compile("ContributionTo.*")
                    )
                )
                logger.debug(len(research_output))
    except:
        logger.warning("get_research_output failed")
    # logger.debug(research_output)
    return research_output


def test():
    url = "https://research-information.bris.ac.uk/en/persons/tom-r-gaunt"
    get_research_output(url)


if __name__ == "__main__":
    person_df = read_file()
    create_research_data(person_df)
    # test()
