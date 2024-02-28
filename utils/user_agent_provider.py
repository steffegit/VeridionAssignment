import numpy as np


class UserAgentProvider:
    def __init__(self, user_agents):
        self.user_agents = user_agents

    def get_random_user_agent(self):
        """
        Returns a random user agent from the list of user agents
        :return: str
        """
        return self.user_agents[np.random.randint(0, len(self.user_agents))]
