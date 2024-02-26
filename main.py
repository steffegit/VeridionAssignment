from bs4 import BeautifulSoup as bs
from fastparquet import ParquetFile, write
import requests
import pandas as pd
import sys


# Function to parse parquet file and get the desired column
def parse_parquet_file(file, selected_column):
    try:
        parquet_file = ParquetFile(file)
        return parquet_file.to_pandas()[selected_column]
    except:
        print("Error: Could not parse parquet file")
        sys.exit(1)


# Function to crawl websites and get links to about and contact pages (if they exist)
# We do this so we can get the contact information of the companies
def crawl_websites(domain):
    print("Crawling website: " + domain)
    new_links = []
    new_links.append("http://" + domain)  # add the main page to the list of links
    try:
        response = requests.get(
            "http://" + domain, timeout=2
        ).text  # using timeout of 2 seconds to avoid long waits
        if not "404" in response or "error" in response or "not found" in response:
            soup = bs(response, "html.parser")
            for link in soup.find_all("a"):
                href = link.get("href")
                if "about" in href or "contact" in href:
                    new_links.append(href)
    except:
        # do nothing
        pass
    new_links = list(set(new_links))  # remove duplicates
    return new_links


def write_to_parquet(links_array):
    try:
        df = pd.DataFrame(links_array, columns=["links"])
        write("list_of_websites.snappy.parquet", df)
    except:
        print("Error: Could not write to parquet file")
        sys.exit(1)


if __name__ == "__main__":
    file = open("list of company websites.snappy.parquet", "rb")  # open parquet file
    df = parse_parquet_file(
        file, "domain"
    )  # parse parquet file and get the domain column
    file.close()

    all_links = []

    for domain in df:
        links = crawl_websites(domain)
        if links:
            print(links)
            all_links.extend(links)
