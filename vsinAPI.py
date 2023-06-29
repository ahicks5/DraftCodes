import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from dateutil import parser
from connectSources import find_ref_dfs

team_ref_df, sport_ref_df, espn_df = find_ref_dfs()

class VsinSharp:

    def __init__(self):
        self.url = 'https://stats.vsinstats.com/pageviewer/vsinstats.php?pageid=vsindksplits'

    @staticmethod
    def percentstring_to_percent(percent):
        return float(percent.strip('%')) / 100

    def parse_html_table(self, table):
        rows = []
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            cells = [cell.text for cell in cells]
            rows.append(cells)
        return pd.DataFrame(rows)

    def get_vsin_sport_tables(self, soup):
        sport_dict = {}
        for div in soup.findAll('div', {'id': True}):
            if 'dk-' in div['id']:
                if div['id'] == 'dk-ufc':  # SKIP UFC
                    continue
                else:
                    sport_name = div['id'].split('dk-')[1].upper()
                    tables = div.find_all("table")
                    if tables:
                        sport_df = self.parse_html_table(tables[0])
                        sport_df = sport_df.iloc[:, :10]
                        sport_dict[sport_name] = sport_df

        return sport_dict

    def get_data(self):
        page = requests.get(self.url)
        soup = BeautifulSoup(page.text, 'html.parser')

        # define tables
        sport_tables = self.get_vsin_sport_tables(soup)

        df_dict = {}

        for sport in sport_tables:
            if sport_tables[sport].empty:
                continue
            else:
                clean_df = self.clean_vsin_df(sport_tables[sport])

                clean_df.columns = ['Team_names', 'Spread', 'Spread_handle', 'Spread_bets', 'Total', 'Total_handle',
                              'Total_bets', 'Moneyline', 'Moneyline_handle', 'Moneyline_bets', 'game_date']
                game_dict_list = clean_df.to_dict('records')
                clean_list = []
                for game_dict in game_dict_list:
                    clean_list.append(self.clean_data(game_dict))

                clean_df = pd.DataFrame(clean_list)
                clean_df['Sport'] = sport
                df_dict[sport] = clean_df

        # combine dfs
        df_list = list(df_dict.values())
        main_df = pd.concat(df_list, axis=0)

        # add in clean names
        main_df = self.add_vsin_ref_names(main_df)

        # clean date and generate id
        main_df = self.generate_game_date_ID(main_df)

        self.show_missing_refs(main_df, team_ref_df)

        return main_df

    def add_vsin_ref_names(self, df):
        # merge away team
        team_ref_df_espn = team_ref_df[['VSIN Names', 'Final Names']]

        df = df.merge(team_ref_df_espn, left_on='away_team', right_on='VSIN Names', how='left')
        df = df.rename(columns={'Final Names': 'away_team_clean'})
        df = df.drop(columns=['VSIN Names'])

        df = df.merge(team_ref_df_espn, left_on='home_team', right_on='VSIN Names', how='left')
        df = df.rename(columns={'Final Names': 'home_team_clean'})
        df = df.drop(columns=['VSIN Names'])

        return df

    def generate_game_date_ID(self, df):
        """use MMDDYYYY-Away-Home clean team names as ID"""
        current_year = pd.to_datetime('today').year  # get current year
        df['game_date'] = df['game_date'] + ', ' + str(current_year)  # append current year to each date

        # Convert all values in 'game_time' to datetime objects, setting errors='coerce' to handle unconvertable values
        df['game_date'] = pd.to_datetime(df['game_date'], format='%A, %B %d, %Y', errors='coerce')

        # Create 'DC_Game_ID' column
        df['DC_Game_ID'] = df['game_date'].dt.strftime('%m%d%Y') + '-' + df['away_team_clean'] + '-' + df[
            'home_team_clean']

        return df

    def is_date(self, game_date):
        try:
            parser.parse(game_date)
            return True
        except ValueError:
            return False

    def clean_vsin_df(self, df):
        # Step 1: Identify date rows
        # This might need to be adjusted based on the actual format of the dates
        column_a = df.columns[0]

        df['is_date'] = df[column_a].apply(self.is_date)

        # Step 2: Add new column with game date
        df['game_date'] = df.loc[df['is_date'], column_a]

        # Step 3: Forward fill to replace NaNs with most recent date
        df['game_date'] = df['game_date'].ffill()

        # Optionally, you might want to drop the original date rows from the dataframe
        df = df[~df['is_date']]

        # And you can drop 'is_date' column as well
        df = df.drop(columns=['is_date'])

        return df

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

        clean_dict['game_date'] = games_dict['game_date'].strip('\xa0')

        return clean_dict

    def clean_teams(self, team_string):
        team_string = team_string.strip('\xa0')
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
        first_percent = float(per_string.split('%')[0].strip()) / 100
        second_percent = float(per_string.split('%')[1].strip()) / 100

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
    #data.to_csv('vsin.csv', index=False)
