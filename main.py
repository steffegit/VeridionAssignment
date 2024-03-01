from timeit import default_timer as timer
from threading import Thread, Semaphore

from utils.io_operations import ParquetHandler
from utils.crawler import WebsiteCrawler
from utils.user_agent_provider import UserAgentProvider
from utils.parser import AddressParser

TIMEOUT = 2  # timeout for requests
NUM_THREADS = 20
semaphore = Semaphore(NUM_THREADS)


def crawl_website_with_semaphore(df_element, user_agent, links):
    semaphore.acquire()
    try:
        WebsiteCrawler(TIMEOUT).crawl_website(df_element, user_agent, links)
    finally:
        semaphore.release()


def main():
    start = timer()

    path = "list of company websites.snappy.parquet"
    user_agents = open("user-agents.txt", "r").read().split("\n")

    parquet_handler = ParquetHandler(path)
    user_agent_provider = UserAgentProvider(user_agents)
    address_parser = AddressParser(timeout=TIMEOUT)

    df = parquet_handler.parse_parquet("domain")

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

    print("DONE CRAWLING FOR MORE LINKS")

    with open("links.txt", "w") as out:
        for link in links:
            out.write(f"{link}\n")

    list_of_addresses = []

    for link in links:
        print(f"Processing the {links.index(link) + 1} link:")
        address_parser.parse_address(
            link, user_agent_provider.get_random_user_agent(), list_of_addresses
        )

    with open("output-backup.csv", "w") as f:
        for item in list_of_addresses:
            f.write(f"{item}\n")

    end = timer()
    seconds = end - start
    m, s = divmod(seconds, 60)
    print(f"Time elapsed: {m} minutes and {s} seconds")
    print(f"Extracted {len(list_of_addresses)} adresses from {df.size} domains")


if __name__ == "__main__":
    main()
