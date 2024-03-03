import requests
from bs4 import BeautifulSoup as bs
import urllib3
from colorama import init as colorama_init
from colorama import Fore, Style


class WebsiteCrawler:
    def __init__(self, timeout):
        self.timeout = timeout

    # def get_random_user_agent(self):
    #     """
    #     Returns a random user agent from the list of user agents
    #     :return: str
    #     """
    #     return self.user_agents[np.random.randint(0, len(self.user_agents))]

    def crawl_website(self, domain, user_agent, output_arr):
        """
        Crawls the website and returns a list of links found on the website
        We are specifically looking for links that contain "about" or "contact" in them
        :param domain: str
        :return: list
        """

        urllib3.disable_warnings()

        headers = {"User-Agent": user_agent}
        print(f"Crawling website: {domain}")
        new_links = []  # add the main page to the list of links
        responses = []
        try:
            response = requests.get(
                f"https://{domain}",
                timeout=self.timeout,
                headers=headers,
                allow_redirects=True,
                verify=False,
            )

            # if the main page redirects to another page, we change the domain to the redirected page's domain
            if domain not in response.url:
                domain = response.url.split("/")[2]

            response = response.text
            return_dict = {
                "domain": domain,
                "response": response,
            }
            responses.append(return_dict)

            if (
                not "404" in response
                or not "error" in response
                or not "not found" in response
            ):
                soup = bs(response, "lxml")
                if soup.find_all("a"):
                    for link in soup.find_all("a"):
                        href = link.get("href")
                        if (
                            "about" in href
                            or "contact" in href
                            and not "mailto" in href
                        ):
                            if domain in href:
                                new_links.append(href)
                            else:
                                if not "http" in href:
                                    if "/" == href[0]:
                                        new_links.append(f"https://{domain}{href}")
                                    else:
                                        new_links.append(f"https://{domain}/{href}")
                                else:
                                    new_links.append(href)
        except Exception as e:
            print(
                f"{Fore.RED}Could not crawl website: {domain}. Error: {e}{Style.RESET_ALL}"
            )
            pass

        new_links = list(set(new_links))  # remove duplicates
        for link in new_links:
            try:
                response = requests.get(
                    link,
                    timeout=self.timeout,
                    headers=headers,
                    allow_redirects=True,
                    verify=False,
                )
                if response.status_code == 200:
                    return_dict = {
                        "domain": domain,
                        "response": response.text,
                    }
                    responses.append(return_dict)

            except Exception as e:
                pass

        if responses:
            output_arr.append(responses)
