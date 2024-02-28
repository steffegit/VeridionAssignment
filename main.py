import requests
import re
import numpy as np
from bs4 import BeautifulSoup as bs
from timeit import default_timer as timer
from geopy.geocoders import Nominatim

from utils.io_operations import ParquetHandler

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


# Function to crawl websites and get links to about and contact pages (if they exist)
# We do this so we can get the contact information of the companies
def crawl_websites(domain):
    headers = {"User-Agent": user_agents[np.random.randint(0, len(user_agents))]}
    print(f"Crawling website: {domain}")
    new_links = []
    new_links.append(f"https://{domain}")  # add the main page to the list of links
    try:
        response = requests.get(
            f"https://{domain}", timeout=TIMEOUT, headers=headers, allow_redirects=True
        )

        # if the main page redirects to another page, we chage the domain to the redirected page's domain

        if domain not in response.url:
            domain = response.url.split("/")[2]

        response = response.text
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

    if not address_from_street and not address_from_zip:
        return None

    country = get_field_value(address_from_street, address_from_zip, "country")
    region = get_field_value(address_from_street, address_from_zip, "state")
    city = get_field_value(address_from_street, address_from_zip, "city")
    postcode = get_field_value(address_from_street, address_from_zip, "postcode")
    road = get_field_value(address_from_street, address_from_zip, "road")
    house_number = get_field_value(
        address_from_street, address_from_zip, "house_number"
    )

    return f"{country}, {region}, {city}, {postcode}, {road}, {house_number}"


def get_field_value(first, second, field):
    field1 = first.get(field) if first else None
    field2 = second.get(field) if second else None

    if field == "road" and field1:
        return field1

    return field2 or field1


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
    headers = {"User-Agent": user_agents[np.random.randint(0, len(user_agents))]}
    try:
        response = requests.get(
            url,
            timeout=TIMEOUT,
            headers=headers,
            allow_redirects=True,
        )
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

    # TODO: make the path be modifiable

    path = "list of company websites.snappy.parquet"
    ParquetHandler = ParquetHandler(path)

    # Parse parquet file and get the domain column
    df = ParquetHandler.parse_parquet("domain")

    # for domain in df:
    #     links = crawl_websites(domain)
    #     if links:
    #         print(links)
    #         all_links.extend(links)

    out = open("list-of-addresses.txt", "w")

    # for testing purposes:
    for i in range(50):
        links = crawl_websites(df[i])
        for link in links:
            output = parse_address(link)
            if output:
                print(output)
                out.write(f"{output}\n")

    end = timer()
    seconds = end - start
    m, s = divmod(seconds, 60)
    print(f"Time elapsed: {m} minutes and {s} seconds")
    out.close()
