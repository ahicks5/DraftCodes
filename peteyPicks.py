from bovadaHelperFunctions import *
import pandas as pd
from SQL_Auth import host, user, password, database
import mysql.connector


def parse_first_half_to_df(url):
    data = get_json(url)

    game_dict = {}
    game_dict['game_id'] = data[0]['events'][0]['id']
    game_dict['sport'] = data[0]['events'][0]['sport']
    for competitor in data[0]['events'][0]['competitors']:
        if competitor['home']:
            game_dict['home_team'] = competitor['name']
        elif not competitor['home']:
            game_dict['away_team'] = competitor['name']

    for group in data[0]['events'][0]['displayGroups']:
        for market in group['markets']:
            if market['period']['description'] == "First Half":
                if market['description'] == "Point Spread":
                    game_dict['away_1h_spread'] = market['outcomes'][0]['price']['handicap']
                    game_dict['home_1h_spread'] = market['outcomes'][1]['price']['handicap']
                elif market['description'] == "Total":
                    game_dict['over_1h_total'] = market['outcomes'][0]['price']['handicap']
                    game_dict['under_1h_total'] = market['outcomes'][0]['price']['handicap']

    return game_dict


class PetePicks:
    def __init__(self):
        self.connection = mysql.connector.connect(
            host=host,  # Server, usually localhost
            user=user,  # your username, e.g., root
            password=password,  # your password
            database=database  # Name of the database
            )

    def get_bovada_game_links_from_db(self):
        cursor = self.connection.cursor()
        query = "SELECT * FROM bovada_data"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        cursor.close()

        return df

    def get_links_from_sport(self, game_sport):
        bov_df = self.get_bovada_game_links_from_db()
        bov_df = bov_df[bov_df['game_sport'] == game_sport]

        game_links = list(bov_df['game_link'].unique())

        # convert to per game json
        final_links = []
        for game in game_links:
            new_link = 'https://www.bovada.lv/services/sports/event/coupon/events/A/description' + game
            final_links.append(new_link)

        return final_links

    def pull_first_half_bovada(self, game_sport):
        links = self.get_links_from_sport(game_sport)

        all_dicts = []
        for link in links:
            game_dict = parse_first_half_to_df(link)
            all_dicts.append(game_dict)

        df = pd.DataFrame(all_dicts)

        # Convert both columns to numeric data type
        df['over_1h_total'] = pd.to_numeric(df['over_1h_total'], errors='coerce')
        df['away_1h_spread'] = pd.to_numeric(df['away_1h_spread'], errors='coerce')

        # Fill missing values with 0 (or another appropriate value if needed)
        df['over_1h_total'].fillna(0, inplace=True)
        df['away_1h_spread'].fillna(0, inplace=True)

        # Now calculate the spread_diff
        df['spread_diff'] = df['over_1h_total'] - abs(df['away_1h_spread'])

        df = df[df['spread_diff'] > 0]
        df = df.sort_values(by='spread_diff', ascending=True)

        df.to_csv('games.csv', index=False)

        final_df = df[['away_team', 'home_team', 'away_1h_spread', 'home_1h_spread', 'over_1h_total', 'spread_diff']]
        print(final_df.to_html(index=False))


if __name__ == '__main__':
    pete = PetePicks()
    pete.pull_first_half_bovada('FOOT')
