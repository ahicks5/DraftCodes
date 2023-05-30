import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment
from datetime import datetime, timedelta


class PullESPN:
    pregame_sport_links = {
        'soccer': 'https://www.espn.com/soccer/schedule/_/date/',
        'nhl': 'https://www.espn.com/nhl/schedule/_/date/',
        'nba': 'https://www.espn.com/nba/schedule/_/date/',
        'nfl': 'https://www.espn.com/nfl/schedule/_/date/',
        'mlb': 'https://www.espn.com/mlb/schedule/_/date/',
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

    def today_tom_date_url(self):
        # Get today's date
        today = datetime.now()

        # Format today's date as 'YYYYMMDD'
        today_str = today.strftime('%Y%m%d')

        # Get tomorrow's date
        tomorrow = today + timedelta(days=1)

        # Format tomorrow's date as 'YYYYMMDD'
        tomorrow_str = tomorrow.strftime('%Y%m%d')

        return today_str, tomorrow_str

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
        today_date, tom_date = self.today_tom_date_url()
        links = []
        for day in [today_date, tom_date]:
            for sport, espn_link in self.pregame_sport_links.items():
                soup = self.get_soup(espn_link)
                if soup:
                    links.extend(self.extract_game_links(soup))
        return links

    def get_game_time_date(self, soup):
        try:
            game_time = soup.find('span', {'data-behavior': 'date_time'})['data-date']
            game_time = pd.to_datetime(game_time)
        except:
            game_time = soup.find(lambda tag: tag.name == 'div' and
                                             tag.get('class') and
                                             "GameInfo__Meta" in tag.get('class')).text.strip()
            if 'Coverage' in game_time:
                game_time = game_time.split('Coverage')[0]

            game_time = pd.to_datetime(game_time)

        return game_time

    def parse_game_stats(self, url):
        """Parse game stats from the given URL"""
        soup = self.get_soup(url)
        if soup:
            game_time = self.get_game_time_date(soup)

            try:
                team_names = soup.findAll('h2',
                                          {'class': 'ScoreCell__TeamName ScoreCell__TeamName--displayName truncate db'})
                team_1 = team_names[0].text.strip()
                team_2 = team_names[1].text.strip()
            except:
                team_1 = soup.findAll('span', {'class': 'long-name'})[0].text.strip()
                team_2 = soup.findAll('span', {'class': 'long-name'})[1].text.strip()

            try:
                matchup_predictor = soup.find('div', {'class': 'matchupPredictor'})
                predictions = matchup_predictor.select('div.matchupPredictor__teamValue')

                team_1_prediction = predictions[0].text.strip()
                team_2_prediction = predictions[1].text.strip()
            except:
                team_1_prediction = None
                team_2_prediction = None

            return {
                'game_time': game_time,
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
        df = df.drop_duplicates()
        return df


if __name__ == '__main__':
    espn = PullESPN()
    #soup = espn.get_soup('https://www.espn.com/nhl/game?gameId=401550957')
    #espn.get_game_time_date(soup)
    print(espn.assemble_espn_data().to_csv('espn_test.csv', index=False))