import requests
from bs4 import BeautifulSoup as bs
import numpy as np


class WebsiteCrawler:
    def __init__(self, user_agents, timeout):
        self.user_agents = user_agents
        self.timeout = timeout

    def get_random_user_agent(self):
        """
        Returns a random user agent from the list of user agents
        :return: str
        """
        return self.user_agents[np.random.randint(0, len(self.user_agents))]

    def crawl_website(self, domain):
        """
        Crawls the website and returns a list of links found on the website
        We are specifically looking for links that contain "about" or "contact" in them
        :param domain: str
        :return: list
        """
        headers = {"User-Agent": self.get_random_user_agent()}
        print(f"Crawling website: {domain}")
        new_links = [f"https://{domain}"]  # add the main page to the list of links
        try:
            response = requests.get(
                f"https://{domain}",
                timeout=self.timeout,
                headers=headers,
                allow_redirects=True,
            )

            # if the main page redirects to another page, we change the domain to the redirected page's domain
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
