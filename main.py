from timeit import default_timer as timer

from utils.io_operations import ParquetHandler
from utils.crawler import WebsiteCrawler
from utils.user_agent_provider import UserAgentProvider
from utils.parser import AddressParser

from threading import Thread, Semaphore

# IMPORTANT: We'll use LXML parser for the BeautifulSoup object, because it's faster than the default parser

# TODO: Add color for errors and success messages
# TODO: add logging (maybe use the logging module)
# TODO: Add try except for every file open/save operation

TIMEOUT = 2  # timeout for requests

user_agents = open("user-agents.txt", "r").read().split("\n")

NUM_THREADS = 20

semaphore = Semaphore(NUM_THREADS)


def crawl_website_with_semaphore(df_element, user_agent, links):
    # Acquire the semaphore
    semaphore.acquire()
    try:
        # Call the original function
        Crawler.crawl_website(df_element, user_agent, links)
    finally:
        # Release the semaphore
        semaphore.release()


if __name__ == "__main__":
    start = timer()

    # TODO: make the path be modifiable

    path = "list of company websites.snappy.parquet"
    ParquetHandler = ParquetHandler(path)
    Crawler = WebsiteCrawler(TIMEOUT)
    UserAgentProvider = UserAgentProvider(user_agents)
    AddressParser = AddressParser(timeout=TIMEOUT)

    # Parse parquet file and get the domain column
    df = ParquetHandler.parse_parquet("domain")  # size of dataframe is 2479 rows

    threads = []

    # TODO: First, we need to crawl the website and get the links
    # Then, we need to parse the links and get the addresses
    # Finally, we need to save the addresses in a file

    links = []

    for i in range(df.size):
        element = df[i]
        t = Thread(
            target=crawl_website_with_semaphore,
            args=(
                element,
                UserAgentProvider.get_random_user_agent(),
                links,
            ),
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print("DONE CRAWLING FOR MORE LINKS")

    list_of_addresses = []

    # Write list_of_addresses to a csv file
    for link in links:
        output = AddressParser.parse_address(
            link, UserAgentProvider.get_random_user_agent()
        )
        if output is not None:
            list_of_addresses.append(output)

    list_of_addresses = list(set(list_of_addresses))

    # ParquetHandler.write_to_csv("output.csv", list_of_addresses)

    with open("output-backup.csv", "w") as f:
        for item in list_of_addresses:
            f.write(f"{item}\n")

    end = timer()
    seconds = end - start
    m, s = divmod(seconds, 60)
    print(f"Time elapsed: {m} minutes and {s} seconds")
    print(f"Extracted {len(list_of_addresses)} adresses from {df.size} domains")
