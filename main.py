import requests
import numpy as np
from bs4 import BeautifulSoup as bs
from timeit import default_timer as timer
from geopy.geocoders import Nominatim

from utils.io_operations import ParquetHandler
from utils.crawler import WebsiteCrawler
from utils.user_agent_provider import UserAgentProvider
from utils.parser import AddressParser

# Format: country, region, city, postcode, road, and road numbers.
# IMPORTANT: We'll use LXML parser for the BeautifulSoup object, because it's faster than the default parser

# TODO: Add color for errors and success messages
# TODO: add logging (maybe use the logging module)
# TODO: Add try except for every file open/save operation

# Constants

TIMEOUT = 2  # timeout for requests

user_agents = open("user-agents.txt", "r").read().split("\n")

# End of Constants


if __name__ == "__main__":
    start = timer()

    # TODO: make the path be modifiable

    path = "list of company websites.snappy.parquet"
    ParquetHandler = ParquetHandler(path)
    Crawler = WebsiteCrawler(TIMEOUT)
    UserAgentProvider = UserAgentProvider(user_agents)
    AddressParser = AddressParser(timeout=TIMEOUT)

    # Parse parquet file and get the domain column
    df = ParquetHandler.parse_parquet("domain")

    out = open("list-of-addresses.txt", "w")

    # for testing purposes:
    for i in range(50):
        links = Crawler.crawl_website(df[i], UserAgentProvider.get_random_user_agent())
        for link in links:
            output = AddressParser.parse_address(
                link, UserAgentProvider.get_random_user_agent()
            )
            if output:
                print(output)
                # out.write(f"{output}\n")

    end = timer()
    seconds = end - start
    m, s = divmod(seconds, 60)
    print(f"Time elapsed: {m} minutes and {s} seconds")
    out.close()
