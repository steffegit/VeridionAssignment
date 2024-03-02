from timeit import default_timer as timer
from threading import Thread, Semaphore

from utils.io_operations import IOHandler
from utils.crawler import WebsiteCrawler
from utils.user_agent_provider import UserAgentProvider
from utils.parser import AddressParser

TIMEOUT = 2  # timeout for requests
NUM_THREADS = 20
semaphore = Semaphore(NUM_THREADS)


def crawl_website_with_semaphore(df_element, user_agent, links):
    """
    Crawl website with semaphore
    :param df_element: element from the dataframe
    :param user_agent: str
    :param links: list
    """

    semaphore.acquire()

    try:
        WebsiteCrawler(TIMEOUT).crawl_website(df_element, user_agent, links)
    finally:
        semaphore.release()


def crawl_websites(df, user_agent_provider):
    threads = []
    links = []

    for i in range(df.size):
        element = df[i]
        t = Thread(
            target=crawl_website_with_semaphore,
            args=(
                element,
                user_agent_provider.get_random_user_agent(),
                links,
            ),
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return links


def main():
    start = timer()

    # Define file paths and parameters
    path = "list of company websites.snappy.parquet"
    user_agents = open("user-agents.txt", "r").read().split("\n")

    # Initialize handlers and providers
    io_handler = IOHandler()
    user_agent_provider = UserAgentProvider(user_agents)
    address_parser = AddressParser(timeout=TIMEOUT)

    # Read the domain data from the parquet file
    df = io_handler.parse_parquet(path, "domain")

    # Crawl the websites and get the links
    links = crawl_websites(df, user_agent_provider)

    # Parse the addresses from the links
    list_of_addresses = [
        address_parser.parse_address(link, user_agent_provider.get_random_user_agent())
        for link in links
    ]

    # Write the addresses to a parquet file
    io_handler.write_to_parquet(list_of_addresses)

    # Calculate and print the elapsed time
    end = timer()
    seconds = end - start
    m, s = divmod(seconds, 60)
    print(f"Time elapsed: {m} minutes and {s} seconds")
    print(f"Extracted {len(list_of_addresses)} addresses from {df.size} domains")


if __name__ == "__main__":
    main()
