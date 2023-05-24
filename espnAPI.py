import pandas as pd
import requests
from bs4 import BeautifulSoup


class PullESPN:
    pregame_sport_links = {
        'soccer': 'https://www.espn.com/soccer/schedule/_/date/20230524',
        'nhl': 'https://www.espn.com/nhl/schedule/_/date/20230524',
        'nba': 'https://www.espn.com/nba/schedule/_/date/20230524',
        'nfl': 'https://www.espn.com/nfl/schedule/_/date/20230524',
        'mlb': 'https://www.espn.com/mlb/schedule/_/date/20230524',
    }

    def get_soup(self, url):
        """Fetch the page content and convert it into BeautifulSoup object"""
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None
        return soup

    def extract_game_links(self, soup):
        """Extract game links from the parsed HTML"""
        links = []
        leagues = soup.select('div.ScheduleTables.mb5')
        for league in leagues:
            tbody = league.find('tbody', {'class': 'Table__TBODY'})
            games = tbody.findAll('tr')
            for game in games:
                a_links = game.findAll('a')
                for tag in a_links:
                    href = tag.get('href')
                    if href and 'gameId=' in href:
                        links.append(f'https://www.espn.com{href}')
        return list(set(links))

    def get_game_links(self):
        """Get all game links from the pregame sports links"""
        links = []
        for sport, espn_link in self.pregame_sport_links.items():
            soup = self.get_soup(espn_link)
            if soup:
                links.extend(self.extract_game_links(soup))
        return links

    def parse_game_stats(self, url):
        """Parse game stats from the given URL"""
        soup = self.get_soup(url)
        if soup:
            team_names = soup.findAll('h2',
                                      {'class': 'ScoreCell__TeamName ScoreCell__TeamName--displayName truncate db'})
            team_1 = team_names[0].text.strip()
            team_2 = team_names[1].text.strip()

            matchup_predictor = soup.find('div', {'class': 'matchupPredictor'})
            predictions = matchup_predictor.select('div.matchupPredictor__teamValue')

            team_1_prediction = predictions[0].text.strip()
            team_2_prediction = predictions[1].text.strip()

            return {
                'team1': team_1,
                'team1_pred': team_1_prediction,
                'team2': team_2,
                'team2_pred': team_2_prediction
            }
        return None

    def assemble_espn_data(self):
        links = self.get_game_links()

        game_list = []
        for link in links:
            print(link)
            game_dict = self.parse_game_stats(link)
            game_list.append(game_dict)

        df = pd.DataFrame(game_list)
        print(df.head)


if __name__ == '__main__':
    espn = PullESPN()
    espn.assemble_espn_data()