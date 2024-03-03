from timeit import default_timer as timer
from threading import Thread, Semaphore
import sys
from tkinter import Tk
from tkinter.filedialog import askopenfilename
from colorama import init as colorama_init
from colorama import Fore, Style

from utils.io_operations import IOHandler
from utils.crawler import WebsiteCrawler
from utils.user_agent_provider import UserAgentProvider
from utils.parser import AddressParser

TIMEOUT = 2  # timeout for requests
NUM_THREADS = 20
semaphore = Semaphore(NUM_THREADS)


def crawl_website_with_semaphore(df_element, user_agent, responses):
    """
    Crawl website with semaphore
    :param df_element: element from the dataframe
    :param user_agent: str
    :param links: list
    """

    semaphore.acquire()

    try:
        WebsiteCrawler(TIMEOUT).crawl_website(df_element, user_agent, responses)
    finally:
        semaphore.release()


def crawl_websites(df, no_of_websites, user_agent_provider):
    threads = []
    responses = []

    for i in range(no_of_websites):
        element = df[i]
        t = Thread(
            target=crawl_website_with_semaphore,
            args=(
                element,
                user_agent_provider.get_random_user_agent(),
                responses,
            ),
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    return responses


def main():
    start = timer()

    colorama_init()

    def print_error_and_exit(error_message):
        print(f"{Fore.RED}ERROR: {error_message}{Style.RESET_ALL}")
        sys.exit(1)

    logo = f"""
{Fore.YELLOW}
    ___       __    __                       ____                           
   /   | ____/ /___/ /_______  __________   / __ \____ ______________  _____
  / /| |/ __  / __  / ___/ _ \/ ___/ ___/  / /_/ / __ `/ ___/ ___/ _ \/ ___/
 / ___ / /_/ / /_/ / /  /  __(__  |__  )  / ____/ /_/ / /  (__  )  __/ /    
/_/  |_\__,_/\__,_/_/   \___/____/____/  /_/    \__,_/_/  /____/\___/_/     
                                                                            
                            {Fore.GREEN}Made by: {Fore.RED}@steffegit{Style.RESET_ALL}
                                                                            """

    print(logo)

    # Create the Tkinter root
    Tk().withdraw()

    # Ask the user to select a file
    print(
        f"{Fore.CYAN}INPUT: Please select the file containing the list of company websites\n{Style.RESET_ALL}"
    )

    path = askopenfilename(
        title="Choose the file containing the list of company websites",
    )

    if path:
        print(f"File loaded successfully: {path}")
    else:
        print_error_and_exit("No file selected")

    try:
        print("Loading user agents")
        user_agents = open("input/user-agents.txt", "r").read().split("\n")
        print("User agents loaded successfully")
    except:
        print_error_and_exit("Could not load user agents")

    # Initialize handlers and providers
    io_handler = IOHandler()
    user_agent_provider = UserAgentProvider(user_agents)
    address_parser = AddressParser(timeout=TIMEOUT)

    # Read the domain data from the parquet file
    df = io_handler.parse_parquet(path, "domain")

    # Crawl the websites and get the links
    responses = crawl_websites(df, df.size, user_agent_provider)

    # Parse the addresses from the links
    list_of_addresses = []
    for element in responses:
        print(
            f"{Fore.LIGHTGREEN_EX}[{responses.index(element) + 1}] {Style.RESET_ALL}Extracting address from {element[0].get('domain')}{Style.RESET_ALL}"
        )

        address_parser.parse_address(
            element,
            user_agent_provider.get_random_user_agent(),
            list_of_addresses,
        )

    # Write the addresses to a parquet file
    io_handler.write_to_parquet(list_of_addresses)

    # Calculate and print the elapsed time
    end = timer()
    seconds = end - start
    m, s = divmod(seconds, 60)

    print("\n-------------------------------------------------------")
    print(f"Time elapsed: {Fore.GREEN}{m} minutes and {s} seconds{Style.RESET_ALL}")
    print(
        f"Extracted {Fore.GREEN}{len(list_of_addresses)}{Style.RESET_ALL} addresses from {Fore.YELLOW}{df.size}{Style.RESET_ALL} domains"
    )
    print("-------------------------------------------------------")


if __name__ == "__main__":
    main()
