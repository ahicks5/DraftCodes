import mysql.connector
from SQL_Auth import host, user, password, database
import pandas as pd
from mlb_indicators_util import *
import time


class BaseballIndicators:
    def __init__(self):
        self.connection = mysql.connector.connect(
            host=host,  # Server, usually localhost
            user=user,  # your username, e.g., root
            password=password,  # your password
            database=database  # Name of the database
        )
        self.df = self.get_teamrefs_from_db()

    def get_teamrefs_from_db(self):
        cursor = self.connection.cursor()
        query = "SELECT * FROM combined_data"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        cursor.close()

        return df

    def generate_mlb_indicator_db(self):
        # first generate base of indicator db
        ind_df = self.df[['DC_Game_ID', 'away_team_clean', 'home_team_clean', 'bovada_sport', 'bovada_league', 'cst_game_date', 'cst_game_time', 'team_1_hcap', 'team_2_hcap', 'team_1_ml_odds', 'team_2_ml_odds', 'Over_line', 'Under_line']]

        # create indicator columns on original df, and eventually clean and relink
        df = self.df
        df = apply_all_indicators(df)
        print('Indicators applied...')

        # combine
        ind_df = ind_df.merge(df, on='DC_Game_ID', how='left')

        # upload
        post_updated_mlb_indicators(ind_df)
        print('MLB Indicators Uploaded...')


def run_and_upload_mlb_indicators():
    start = time.time()
    mlb = BaseballIndicators()
    mlb.generate_mlb_indicator_db()
    end = time.time()
    print(f"MLB Indicator time taken: {end - start:.2f} seconds")


if __name__ == '__main__':
    run_and_upload_mlb_indicators()
