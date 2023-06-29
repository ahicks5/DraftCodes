import pandas as pd
import requests
import pytz
from datetime import datetime, timedelta
import json
from connectSources import find_ref_dfs

team_ref_df, sport_ref_df, espn_df = find_ref_dfs()


class PullBovada:
    # build a dataframe with all upcoming games
    pregame_sport_links = {
        #'nba': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/basketball/nba?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        'mlb': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/baseball/mlb?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        #'epl': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/soccer/europe/england/premier-league?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        #'spainLaLiga': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/soccer/europe/spain/la-liga?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        #'italySerieA': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/soccer/europe/italy/serie-a?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        #'nhl': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/hockey/nhl?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        'nfl': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/football/nfl?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        'tableTennis': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/table-tennis?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        'LoL': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/esports/league-of-legends?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        'mls': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/soccer/north-america/united-states/mls?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        'cfb': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/football/college-football?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        'wnba': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/basketball/wnba?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        'cfl': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/football/cfl?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en',
        'usfl': 'https://www.bovada.lv/services/sports/event/coupon/events/A/description/football/usfl?marketFilterId=def&preMatchOnly=true&eventsLimit=5000&lang=en'
    }

    def __init__(self):
        self.session = requests.Session()

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

        def two_way_hcap(mkt):
            try:
                price0, price1 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price']
                return {'team_1_hcap': price0['handicap'],
                        'team_1_hcap_odds': price0['american'],
                        'team_2_hcap': price1['handicap'],
                        'team_2_hcap_odds': price1['american']}
            except Exception as e:
                print(f"Error in two_way_hcap: {e}")
                return {'team_1_hcap': 'nan', 'team_1_hcap_odds': 'nan', 'team_2_hcap': 'nan',
                        'team_2_hcap_odds': 'nan'}

        def two_way_12(mkt):
            try:
                price0, price1 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price']
                return {'team_1_ml_odds': price0['american'],
                        'team_2_ml_odds': price1['american']}
            except Exception as e:
                print(f"Error in two_way_ml: {e}")
                return {'team_1_ml_odds': 'nan',
                        'team_2_ml_odds': 'nan'}

        def three_way_1X2(mkt):
            price0, price1, price2 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price'], mkt['outcomes'][2][
                'price']
            return {'team_1_ml_odds': price0['american'],
                    'team_2_ml_odds': price1['american'],
                    'draw_ml_odds': price2['american']}

        def two_way_OU(mkt):
            try:
                price0, price1 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price']
                return {f'{mkt["outcomes"][0]["description"]}_line': price0['handicap'],
                        f'{mkt["outcomes"][0]["description"]}_odds': price0['american'],
                        f'{mkt["outcomes"][1]["description"]}_line': price1['handicap'],
                        f'{mkt["outcomes"][1]["description"]}_odds': price1['american']}
            except Exception as e:
                print(f"Error in two_way_OU: {e}")
                try:
                    description0 = mkt["outcomes"][0]["description"]
                except Exception:
                    description0 = 'nan'
                try:
                    description1 = mkt["outcomes"][1]["description"]
                except Exception:
                    description1 = 'nan'
                return {f'{description0}_line': 'nan',
                        f'{description0}_odds': 'nan',
                        f'{description1}_line': 'nan',
                        f'{description1}_odds': 'nan'}

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
        # Convert the time column from milliseconds to datetime
        df['game_startTime'] = pd.to_datetime(df['game_startTime'], unit='ms')
        # Convert UTC to Central Time
        df['game_startTime_cst'] = df['game_startTime'].dt.tz_localize('UTC').dt.tz_convert('America/Chicago')
        # Create a date column
        df['game_date'] = df['game_startTime_cst'].dt.date
        # Create a time column
        df['game_time'] = df['game_startTime_cst'].dt.strftime('%I:%M %p')

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
        team_ref_df_bov = team_ref_df[['Bovada Names', 'Final Names']]

        df = df.merge(team_ref_df_bov, left_on='competitor_2', right_on='Bovada Names', how='left')
        df = df.rename(columns={'Final Names': 'away_team_clean'})
        df = df.drop(columns=['Bovada Names'])

        df = df.merge(team_ref_df_bov, left_on='competitor_1', right_on='Bovada Names', how='left')
        df = df.rename(columns={'Final Names': 'home_team_clean'})
        df = df.drop(columns=['Bovada Names'])

        return df

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
        self.show_missing_refs(df, team_ref_df)

        return df


if __name__ == '__main__':
    bov = PullBovada()
    bov.scrape_main_sports().to_csv('test_bov.csv', index=False)