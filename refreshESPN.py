import pandas as pd
from datetime import datetime, timedelta
import pytz
from ESPNHelperFunctions import *
import mysql.connector
import time

cdt = pytz.timezone('America/Chicago')


class RefreshESPN:
    def __init__(self):
        self.connection = mysql.connector.connect(
            host=host,  # Server, usually localhost
            user=user,  # your username, e.g., root
            password=password,  # your password
            database=database  # Name of the database
        )

    def get_teamrefs_from_db(self):
        cursor = self.connection.cursor()
        query = "SELECT * FROM team_reference"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        cursor.close()

        return df

    def generate_new_espn(self):
        links = get_game_links()

        # assemble game and predictions
        game_list = []

        for link in links:
            game_dict = parse_game_stats(link)
            game_dict['lastMod_ESPNpred'] = datetime.now(cdt).strftime("%m/%d/%Y %I:%M:%S %p")
            game_list.append(game_dict)

        pred_df = pd.DataFrame(game_list)
        pred_df = pred_df.drop_duplicates()

        # add names, add id, check missing
        df = self.add_espn_ref_names(pred_df)
        df = generate_game_date_ID(df)

        return df

    def get_espn_data_from_db(self):
        cursor = self.connection.cursor()
        query = "SELECT * FROM espn_data"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        cursor.close()

        return df

    def add_espn_ref_names(self, df):
        # merge away team
        team_ref_df_espn = self.get_teamrefs_from_db()[['ESPN Names', 'Final Names']]

        df = df.merge(team_ref_df_espn, left_on='team1', right_on='ESPN Names', how='left')
        df = df.rename(columns={'Final Names': 'away_team_clean'})
        df = df.drop(columns=['ESPN Names'])

        df = df.merge(team_ref_df_espn, left_on='team2', right_on='ESPN Names', how='left')
        df = df.rename(columns={'Final Names': 'home_team_clean'})
        df = df.drop(columns=['ESPN Names'])

        return df

    def close_sql(self):
        self.connection.close()

    def generate_and_refresh_espn(self):
        new_df = self.generate_new_espn()
        print("ESPN New DF Pulled...")
        old_df = self.get_espn_data_from_db()

        new_df.set_index('gameID', inplace=True)
        old_df.set_index('gameID', inplace=True)

        final_df = new_df.combine_first(old_df)
        final_df.reset_index(inplace=True)
        final_df = final_df.drop_duplicates()
        print("Combined with old...")

        self.close_sql()
        post_updated_espn(final_df)
        print("~~ESPN Updated to Database~~")


if __name__ == '__main__':
    start = time.time()
    espn = RefreshESPN()
    espn.generate_and_refresh_espn()
    end = time.time()
    print(f"ESPN time taken: {end - start:.2f} seconds")