import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

team_ref_df = pd.read_csv('Team_Reference.csv')
#team_ref_df = pd.read_csv('/var/www/html/Website/Team_Reference.csv')

class VsinSharp:

    def __init__(self):
        self.url = 'https://stats.vsinstats.com/pageviewer/vsinstats.php?pageid=vsindksplits'

    @staticmethod
    def percentstring_to_percent(percent):
        return float(percent.strip('%')) / 100

    def get_vsin_sport_tables(self, soup):
        sport_dict = {}
        for div in soup.findAll('div', {'id': True}):
            if 'dk-' in div['id']:
                if div['id'] == 'dk-ufc':  # SKIP UFC
                    continue
                else:
                    try:
                        sport_name = div['id'].split('dk-')[1].upper()
                        sport_df = pd.read_html(str(div))[0]
                        sport_dict[sport_name] = sport_df
                    except:
                        continue

        return sport_dict

    def get_data(self):
        page = requests.get(self.url)
        soup = BeautifulSoup(page.text, 'html.parser')

        # define tables
        sport_tables = self.get_vsin_sport_tables(soup)

        df_dict = {}

        for sport in sport_tables:
            df = sport_tables[sport]
            df.columns = ['Team_names', 'Spread', 'Spread_handle', 'Spread_bets', 'Total', 'Total_handle',
                          'Total_bets', 'Moneyline', 'Moneyline_handle', 'Moneyline_bets']
            game_dict_list = df.to_dict('records')
            clean_list = []
            for game_dict in game_dict_list:
                clean_list.append(self.clean_data(game_dict))

            df = pd.DataFrame(clean_list)
            df['Sport'] = sport
            df_dict[sport] = df

        # combine dfs
        df_list = list(df_dict.values())
        main_df = pd.concat(df_list, axis=0)

        self.show_missing_refs(main_df, team_ref_df)

        return main_df

    def clean_data(self, games_dict):
        clean_dict = {}
        clean_dict['away_team'], clean_dict['home_team'] = self.clean_teams(games_dict['Team_names'])

        clean_dict['away_spread'], clean_dict['home_spread'] = self.clean_spread(games_dict['Spread'])

        clean_dict['away_spread_handle'], clean_dict['home_spread_handle'] = self.clean_percents(
            games_dict['Spread_handle'])
        clean_dict['away_spread_bets'], clean_dict['home_spread_bets'] = self.clean_percents(
            games_dict['Spread_bets'])

        clean_dict['total_over'], clean_dict['total_under'] = self.clean_total(games_dict['Total'])
        clean_dict['total_over_handle'], clean_dict['total_under_handle'] = self.clean_percents(
            games_dict['Total_handle'])
        clean_dict['total_over_bets'], clean_dict['total_under_bets'] = self.clean_percents(
            games_dict['Total_bets'])

        # protocol for moneyline
        clean_dict['away_ml'], clean_dict['home_ml'] = self.clean_moneyline(games_dict['Moneyline'])

        clean_dict['away_ml_handle'], clean_dict['home_ml_handle'] = self.clean_percents(
            games_dict['Moneyline_handle'])
        clean_dict['away_ml_bets'], clean_dict['home_ml_bets'] = self.clean_percents(
            games_dict['Moneyline_bets'])

        return clean_dict

    def clean_teams(self, team_string):
        away_team, home_team = [team.strip() for team in team_string.split('\xa0')]

        return away_team, home_team

    def clean_moneyline(self, ml_string):
        if ml_string == '--':
            away_ml = home_ml = ''
        else:
            away_ml, home_ml = self.split_at_first_non_number(ml_string)

        return away_ml, home_ml

    def clean_spread(self, spread_string):
        if spread_string == '--':
            away_spread = ''
            home_spread = ''
        elif spread_string[-1] == '-':
            away_spread = spread_string[:-1]
            home_spread = ''
        else:
            away_spread, home_spread = self.split_at_first_non_number(spread_string)

        return away_spread, home_spread

    def split_at_first_non_number(self, s):
        match = re.search("\d[^0-9\.]", s[1:])
        if match:
            split_index = match.start() + 2  # adding 2 to account for the slice offset and the non-number character
            return s[:split_index].strip(), s[split_index:].strip()
        else:
            return s, ""

    def clean_percents(self, per_string):
        first_percent = per_string.split('%')[0].strip() + '%'
        second_percent = per_string.split('%')[1].strip() + '%'

        return first_percent, second_percent

    def show_missing_refs(self, df, team_ref_df):
        teams_matchups = pd.concat([df['away_team'], df['home_team']]).unique()
        teams_reference = team_ref_df['VSIN Names'].unique()
        missing_teams = [team for team in teams_matchups if team not in teams_reference]

        df_missing_teams = pd.DataFrame(missing_teams, columns=['team'])
        df_missing_teams.to_csv('vsin_missing_teams.csv', index=False)

    def clean_total(self, total_string):
        if total_string == '--':
            over = under = ''
        elif total_string[-1] == '-':
            over = total_string.split('Ov ')[1][:-1]
            under = ''
        elif total_string[:2] == '--':
            over = ''
            under = total_string.split('Un ')[1].strip()
        elif 'Un' not in total_string and 'Ov' in total_string:
            over = total_string.split('Ov ')[1].strip()
            under = ''
        elif 'Ov' not in total_string and 'Un' in total_string:
            over = ''
            under = total_string.split('Un ')[1].strip()
        else:
            over = total_string.split('Un')[0].split(' ')[1].strip()
            under = total_string.split('Un')[1].strip()

        return over, under


if __name__ == '__main__':
    vsin_sharp = VsinSharp()
    data = vsin_sharp.get_data()
    data.to_csv('vsin.csv', index=False)
