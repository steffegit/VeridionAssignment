from bs4 import BeautifulSoup as bs
from fastparquet import ParquetFile, write
import requests
import pandas as pd
import sys
import re
from geopy.geocoders import Nominatim

# Format: country, region, city, postcode, road, and road numbers.
# IMPORTANT: We'll use LXML parser for the BeautifulSoup object, because it's faster than the default parser

# TODO: Add color for errors and success messages

# Constants

TIMEOUT = 2  # timeout for requests

# Regex for finding zip codes
zip_code_regex = re.compile(r"\b\d{5}(?:[-\s]\d{4})?\b")

# Regex for finding street addresses
street_regex = re.compile(
    r"(?i)(^|\s)\d{2,7}\b\s+.{5,30}\b\s+(?:road|rd|way|street|st|str|avenue|ave|boulevard|blvd|lane|ln|drive|dr|terrace|ter|place|pl|court|ct)(?:\.|\s|$)"
)

# Regex for finding PO Box
po_box_regex = re.compile(r"(?i)(?:po|p.o.)\s+(?:box)")

# Regex for finding numbers
num_regex = re.compile(r"\d+")

geolocator = Nominatim(user_agent="veridionAssignment")

# End of Constants


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
    print(f"Crawling website: {domain}")
    new_links = []
    new_links.append(f"http://{domain}")  # add the main page to the list of links
    try:
        response = requests.get(f"http://{domain}", timeout=TIMEOUT).text
        if (
            not "404" in response
            or not "error" in response
            or not "not found" in response
        ):
            soup = bs(response, "lxml")
            for link in soup.find_all("a"):
                href = link.get("href")
                if "about" in href or "contact" in href:
                    if domain in href:
                        new_links.append(href)
                    else:
                        if "/" in href:
                            new_links.append(f"http://{domain}{href}")
                        else:
                            new_links.append(f"http://{domain}/{href}")
    except:
        # do nothing
        pass
    new_links = list(set(new_links))  # remove duplicates
    return new_links


def write_to_parquet(links_array):
    try:
        df = pd.DataFrame(links_array, columns=["link"])
        write("list_of_websites.snappy.parquet", df)
    except:
        print("Error: Could not write to parquet file")
        sys.exit(1)


# TODO: For each link in the list, use regex to grab the address information
# Maybe we can use session, and pass it to each function to avoid creating a new session for each link
# We can also use a thread pool to make the process faster


def create_final_address(location_from_street, location_from_zip):
    if location_from_street:
        return
    if location_from_zip:
        return


def parse_address(url):
    try:
        response = requests.get(url, timeout=TIMEOUT)
        if response.status_code != 200:
            print(f"Error getting {url}")
            return
        response = response.text
        soup = bs(response, "lxml")
    except:
        print(f"Error parsing page {url}")

    # Search for a valid formatted street address
    street_address = None
    try:
        street_address = [
            val for val in soup.find_all(string=street_regex) if len(val) <= 100
        ]
        if street_address:
            street_address = re.search(street_regex, street_address[0].text).group(0)
            street_address = re.sub(po_box_regex, "", street_address)
    except:
        print(f"Error getting street from {url}")

    # Search for a valid zip code
    zip_code = None
    try:
        zip_code = [
            val for val in soup.find_all(string=zip_code_regex) if len(val) <= 100
        ]

        if zip_code:
            zip_code = re.search(zip_code_regex, zip_code[0].text).string.strip()
            zip_code = re.sub(po_box_regex, "", zip_code)
    except:
        print(f"Error getting zipcode from page {url}")

    # Check if the street address and zip code are valid and return them
    location_from_street = None
    location_from_zip = None
    try:
        if street_address:
            location_from_street = geolocator.geocode(
                street_address, addressdetails=True, timeout=TIMEOUT
            )
        if zip_code:
            location_from_zip = geolocator.geocode(zip_code)
    except:
        print(
            f"Error validating location from the street_address or zip code from page {url}"
        )

    return [location_from_street, location_from_zip]

    # final_address = None
    # try:
    #     final_address = create_final_address(location_from_street, location_from_zip)
    # except:
    #     print(f"Error getting final address from page {url}")

    # return final_address


if __name__ == "__main__":
    file = open("list of company websites.snappy.parquet", "rb")  # open parquet file
    df = parse_parquet_file(
        file, "domain"
    )  # parse parquet file and get the domain column
    file.close()

    all_links = []

    # for domain in df:
    #     links = crawl_websites(domain)
    #     if links:
    #         print(links)
    #         all_links.extend(links)

    # for testing purposes:
    for i in range(10):
        links = crawl_websites(df[i])
        if links:
            all_links.extend(links)

    # print(all_links)

    for link in all_links:
        print(parse_address(link))
