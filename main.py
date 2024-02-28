import random
from bs4 import BeautifulSoup as bs
from fastparquet import ParquetFile, write
import requests
import pandas as pd
import sys
import re
from geopy.geocoders import Nominatim
from timeit import default_timer as timer
import json as JSON

# Format: country, region, city, postcode, road, and road numbers.
# IMPORTANT: We'll use LXML parser for the BeautifulSoup object, because it's faster than the default parser

# TODO: Add color for errors and success messages
# TODO: add logging (maybe use the logging module)

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

user_agents = open("user-agents.txt", "r").read().split("\n")

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
    headers = {"User-Agent": user_agents[random.randint(0, len(user_agents) - 1)]}
    print(f"Crawling website: {domain}")
    new_links = []
    new_links.append(f"https://{domain}")  # add the main page to the list of links
    try:
        response = requests.get(
            f"https://{domain}", timeout=TIMEOUT, headers=headers
        ).text
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
                            new_links.append(f"https://{domain}{href}")
                        else:
                            new_links.append(f"https://{domain}/{href}")
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
    if not location_from_street and not location_from_zip:
        return None

    address_from_street = None
    address_from_zip = None

    if location_from_street:
        address_from_street = location_from_street.raw.get("address")
    if location_from_zip:
        address_from_zip = location_from_zip.raw.get("address")

    country = choose_field(address_from_street, address_from_zip, "country")
    region = choose_field(address_from_street, address_from_zip, "state")
    city = choose_field(address_from_street, address_from_zip, "city")
    postcode = choose_field(address_from_street, address_from_zip, "postcode")
    road = choose_field(address_from_street, address_from_zip, "road")
    house_number = choose_field(address_from_street, address_from_zip, "house_number")

    return f"{country}, {region}, {city}, {postcode}, {road}, {house_number}"


def choose_field(first, second, field):
    field1 = None
    field2 = None
    if first and field in first:
        field1 = first[field]
    if second and field in second:
        field2 = second[field]

    if field1 and field2:
        if field == "road":
            return field1
        return field2

    if not field1:
        return field2

    return field1


def get_location(soup, regex, url):
    try:
        for val in soup.find_all(string=regex):
            if len(val) <= 100:
                location = re.search(regex, val).group(0)
                location = re.sub(po_box_regex, "", location)
                return location
    except AttributeError:
        print(f"AttributeError: Unable to find or process the location from {url}")
    except Exception as e:
        print(f"Unexpected error {e} occurred while getting location from {url}")
    return None


def parse_address(url):
    headers = {"User-Agent": user_agents[random.randint(0, len(user_agents) - 1)]}
    try:
        response = requests.get(url, timeout=TIMEOUT, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as https_err:
        print(f"https error occurred while getting {url}")
        return
    except Exception as err:
        print(f"Error occurred while getting {url}")
        return

    try:
        soup = bs(response.text, "lxml")
    except Exception as e:
        print(f"Error occurred while parsing page {url}. Error: {e}")
        return

    street_address = get_location(soup, street_regex, url)
    zip_code = get_location(soup, zip_code_regex, url)

    location_from_street = None
    location_from_zip = None

    if street_address:
        try:
            location_from_street = geolocator.geocode(
                street_address, addressdetails=True, timeout=TIMEOUT
            )
        except Exception as e:
            print(
                f"Error validating location from the street_address {street_address} from page {url}. Error: {e}"
            )

    if zip_code:
        try:
            location_from_zip = geolocator.geocode(zip_code)
        except Exception as e:
            print(
                f"Error validating location from the zip_code {zip_code} from page {url}. Error: {e}"
            )

    final_address = None
    try:
        final_address = create_final_address(location_from_street, location_from_zip)
    except Exception as e:
        print(f"Error getting final address from page {url}. Error {e}")

    return final_address


if __name__ == "__main__":
    start = timer()
    file = open("list of company websites.snappy.parquet", "rb")  # open parquet file
    df = parse_parquet_file(
        file, "domain"
    )  # parse parquet file and get the domain column
    file.close()

    # for domain in df:
    #     links = crawl_websites(domain)
    #     if links:
    #         print(links)
    #         all_links.extend(links)

    # for testing purposes:
    for i in range(20):
        links = crawl_websites(df[i])
        for link in links:
            output = parse_address(link)
            if output:
                print(output)

    end = timer()
    print(f"Time elapsed: {end - start} seconds")
