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

    top: int = -1  # How many emails to read


parser.add_arguments(Options, dest="options")

args = parser.parse_args()
logger.debug(args)
logger.debug("options:", args.options.top)


def read_emails():
    df = pd.read_csv(args.input, names=["email"])
    #make lowercase
    df['email'] = df['email'].str.lower()
    logger.info(df.shape)
    # check for dups
    df.drop_duplicates(inplace=True)
    logger.debug(df.head())
    logger.info(df.shape)

    pattern = re.compile(r"(^[a-zA-Z0-9_.+-]+@bristol.ac.uk$)")
    email_check = df["email"].apply(lambda x: True if pattern.match(x) else False)
    bad_emails = df[~email_check]
    bad_emails.to_csv("workflow/results/bad_emails.txt", index=False, header=False)
    # logger.debug(bad_emails)
    logger.debug(
        f"Removing {bad_emails.shape[0]} bad emails, see them here results/bad_emails.txt"
    )

    df = df[email_check]
    logger.debug(f"Left with {df.shape[0]} emails")

    # use top argument for testing
    if args.options.top:
        logger.debug(f"Keeping top {args.options.top}")
        df = df.head(n=args.options.top)

    logger.debug(f"Left with {df.shape[0]} emails")
    return df


def uob_finder_web(email):
    logger.debug(f"Searching for {email}")
    # https://research-information.bris.ac.uk/en/searchAll/advanced/?searchByRadioGroup=PartOfNameOrTitle&searchBy=PartOfNameOrTitle&allThese=&exactPhrase=ben.elsworth%40bristol.ac.uk&or=&minus=&family=persons&doSearch=Search&slowScroll=true&resultFamilyTabToSelect=
    url = f"https://research-information.bris.ac.uk/en/searchAll/advanced/?exactPhrase={email}&or=&minus=&family=persons&doSearch=Search&slowScroll=true&resultFamilyTabToSelect="
    res = requests.get(url)
    soup = BeautifulSoup(res.text, "html.parser")
    people_data = soup.find_all("li", class_="grid-result-item")
    person_info = False
    for p in people_data:
        find_person = p.find_all("a", class_="link person")
        # logger.debug(p)
        e = p.find("span", text=re.compile(email, re.IGNORECASE))
        if e:
            logger.debug(f"{find_person} {e}")
            person_page = find_person[0]["href"]
            # get name
            # m = re.match()
            name = find_person[0].getText()
            logger.debug(person_page)
            person_info = {"page": person_page, "name": name, "email": email}
    if person_info == False:
        logger.debug("No page found")
        person_info = {"page": "NA", "name": "NA", "email": email}
    return person_info


def get_all_people(email_df):
    data = []
    # check for existing data
    f = f"{args.output}.tsv.gz"
    existing_data = []
    if os.path.exists(f) and os.path.getsize(f) > 1:
        logger.info(f"Reading existing data {f}")
        existing_df = pd.read_csv(f, sep="\t")
        # print(existing_df)
        existing_data = list(existing_df["email"])
        # logger.debug(existing_data)
        try:
            data = existing_df.to_dict("records")
        except:
            logger.warning(f"Error when reading {f}")
        logger.debug(f"Got data on {len(existing_data)} urls")

    for i, row in email_df.iterrows():
        if row["email"] in existing_data:
            logger.info(f"{row['email']} done")
        else:
            person_info = uob_finder_web(email=row["email"])
            data.append(person_info)
    person_df = pd.DataFrame(data)
    email_df = pd.merge(email_df, person_df, left_on="email", right_on="email")
    logger.debug(email_df.head())
    email_df.to_csv(f, sep="\t", index=False)
    mark_as_complete(args.output)

if __name__ == "__main__":
    email_df = read_emails()
    get_all_people(email_df)
