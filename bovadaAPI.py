import pandas as pd
import requests
import pytz
from datetime import datetime, timedelta
import json
from loadCSVs import load_previous_data
from bovadaHelperFunctions import two_way_hcap, two_way_12, three_way_1X2, two_way_OU

# Constants
BOVADA_DF_DATATYPES_PATH = 'bovada_df_datatypes.json'
BOVADA_DATA_CSV_PATH = 'Bovada_Data.csv'
MISSING_TEAMS_SPORTS_CSV = 'bovada_missing_teams_sports.csv'
LOOKAHEAD_DAYS = 300
UTC = 'UTC'
CST = 'America/Chicago'
CLEAN_TIME_FORMAT = '%I:%M %p'


class PullBovada:
    def __init__(self):
        self.session = requests.Session()
        self.pregame_sport_links = load_previous_data()['bovada_links'].set_index('Sport')['Link'].to_dict()
        self.team_ref = load_previous_data()['team_ref']

        # Load the data types from the JSON file
        try:
            with open('/var/www/html/Website/bovada_df_datatypes.json', 'r') as file:
                datatypes = json.load(file)
        except:
            with open(BOVADA_DF_DATATYPES_PATH, 'r') as file:
                datatypes = json.load(file)
        self.bovada_df = load_previous_data()['bovada_data']
        # Set the data types of the columns in the DataFrame
        for column, dtype in datatypes.items():
            self.bovada_df[column] = self.bovada_df[column].astype(dtype)

    def get_json(self, url):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json'
        }
        response = requests.get(url, headers=headers)
        try:
            return response.json()
        except json.decoder.JSONDecodeError:
            print(f"Failed to decode JSON from response. Response text was:\n{response.text}")
            return None

    def parse_game_data(self, game):
        game_dict = {f'game_{key}': value for key, value in game.items() if not isinstance(value, list)}
        for i, competitor in enumerate(game.get('competitors', [])):
            game_dict.update({
                f'competitor_{i + 1}': competitor['name'],
                f'competitor_{i + 1}_home': competitor['home']
            })

        key_map = {
            '2W-HCAP': two_way_hcap,
            '2W-12': two_way_12,
            '3W-1X2': three_way_1X2,
            '2W-OU': two_way_OU
        }

        for mkt in game['displayGroups'][0]['markets']:
            if mkt['key'] in key_map:
                game_dict.update(key_map[mkt['key']](mkt))

        return game_dict

    def scrape_single_page(self, url):
        site_json = self.get_json(url)
        if not site_json:
            return []
        return [self.parse_game_data(game) for game in site_json[0]['events']]

    def bovada_data_to_dataframe(self, games):
        # list of lists, combine then df
        all_games = [game for league in games for game in league]
        df = pd.DataFrame(all_games)
        return df

    def add_date_columns(self, df):
        df['game_startTime'] = pd.to_datetime(df['game_startTime'], unit='ms')
        df['game_startTime_cst'] = df['game_startTime'].dt.tz_localize(UTC).dt.tz_convert(CST)
        df['game_date'] = df['game_startTime_cst'].dt.date
        df['game_time'] = df['game_startTime_cst'].dt.strftime(CLEAN_TIME_FORMAT)

        return df

    def generate_game_date_ID(self, df):
        """use MMDDYYYY-Away-Home clean team names as ID"""
        df['DC_Game_ID'] = df['game_startTime_cst'].dt.strftime('%m%d%Y') + '-' + df['away_team_clean'] + '-' + df['home_team_clean']

        return df

    def filter_for_newly_upcoming_games(self, df):
        central = pytz.timezone('US/Central')
        current_time = datetime.now(central)
        two_days_from_now = current_time + timedelta(days=300)
        filtered_df = df[(df['game_startTime_cst'] >= current_time) & (df['game_startTime_cst'] <= two_days_from_now)]

        return filtered_df

    def show_missing_refs(self, df, team_ref_df):
        team_sport_matchups = set(tuple(x) for x in df[['competitor_1', 'game_sport']].values.tolist() + df[['competitor_2', 'game_sport']].values.tolist())
        teams_reference = set(team_ref_df['Bovada Names'].unique())
        missing_teams_sports = [team_sport for team_sport in team_sport_matchups if team_sport[0] not in teams_reference]

        df_missing_teams_sports = pd.DataFrame(missing_teams_sports, columns=['team', 'sport'])
        df_missing_teams_sports.to_csv('bovada_missing_teams_sports.csv', index=False)

    def add_bov_ref_names(self, df):
        # merge away team
        team_ref_df_bov = self.team_ref[['Bovada Names', 'Final Names']]

        df = df.merge(team_ref_df_bov, left_on='competitor_2', right_on='Bovada Names', how='left')
        df = df.rename(columns={'Final Names': 'away_team_clean'})
        df = df.drop(columns=['Bovada Names'])

        df = df.merge(team_ref_df_bov, left_on='competitor_1', right_on='Bovada Names', how='left')
        df = df.rename(columns={'Final Names': 'home_team_clean'})
        df = df.drop(columns=['Bovada Names'])

        return df

    def combine_old_and_new(self, new_df):
        new_df['game_id'] = new_df['game_id'].astype('int64')

        # Filter out rows in the old DataFrame that have the same game_id as in the new DataFrame
        filtered_old_df = self.bovada_df.loc[~self.bovada_df['game_id'].isin(new_df['game_id'])]

        # Concatenate the new DataFrame with the filtered old DataFrame
        final_df = pd.concat([new_df, filtered_old_df])

        # Convert game_startTime_cst column to datetime, handling timezone-aware datetime objects
        final_df['game_startTime_cst'] = pd.to_datetime(final_df['game_startTime_cst'], utc=True)

        # Convert the timezone back to CDT
        final_df['game_startTime_cst'] = final_df['game_startTime_cst'].dt.tz_convert('America/Chicago')

        # resort
        final_df = final_df.sort_values(by='game_startTime_cst', ascending=True).reset_index(drop=True)

        return final_df

    def scrape_main_sports(self):
        league_lists = []
        for sport in self.pregame_sport_links:
            league_lists.append(self.scrape_single_page(self.pregame_sport_links[sport]))

        df = self.bovada_data_to_dataframe(league_lists)
        df = self.add_date_columns(df)

        # filter for today
        df = self.filter_for_newly_upcoming_games(df)

        # add clean
        df = self.add_bov_ref_names(df)

        # add id
        df = self.generate_game_date_ID(df)

        # check missing refs
        self.show_missing_refs(df, self.team_ref)

        # combine
        final_df = self.combine_old_and_new(df)

        # Save to csv
        try:
            final_df.to_csv('/var/www/html/Website/Bovada_Data.csv', index=False)
        except:
            final_df.to_csv('Bovada_Data.csv', index=False)

        # Return the final DataFrame
        return final_df


if __name__ == '__main__':
    bov = PullBovada()
    bov.scrape_main_sports().to_csv('test_bov.csv', index=False)