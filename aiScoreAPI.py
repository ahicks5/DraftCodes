import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment
from datetime import datetime, timedelta
from connectSources import find_ref_dfs


class PullAIScore:
    test_link = 'https://www.aiscore.com/'

    def __init__(self):
        self.session = requests.Session()

    def get_soup(self, url):
        """Fetch the page content and convert it into BeautifulSoup object"""
        #try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json'
        }
        response = requests.get(url, headers=headers)
        #response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        #except requests.exceptions.RequestException as e:
        #    print(f"Request failed: {e}")
        #    return None
        return soup

    def full_test(self):
        print(self.get_soup(self.test_link))


if __name__ == '__main__':
    aiscore = PullAIScore()
    aiscore.full_test()