from vsinHelperFunctions import *
import mysql.connector
import requests
from bs4 import BeautifulSoup
import time


class RefreshVSIN:
    def __init__(self):
        self.url = 'https://stats.vsinstats.com/pageviewer/vsinstats.php?pageid=vsindksplits'
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

    def add_vsin_ref_names(self, df):
        # merge away team
        team_ref_df_espn = self.get_teamrefs_from_db()[['VSIN Names', 'Final Names']]

        df = df.merge(team_ref_df_espn, left_on='vsin_away_team', right_on='VSIN Names', how='left')
        df = df.rename(columns={'Final Names': 'away_team_clean'})
        df = df.drop(columns=['VSIN Names'])

        df = df.merge(team_ref_df_espn, left_on='vsin_home_team', right_on='VSIN Names', how='left')
        df = df.rename(columns={'Final Names': 'home_team_clean'})
        df = df.drop(columns=['VSIN Names'])

        return df

    def generate_new_vsin_df(self):
        page = requests.get(self.url)
        soup = BeautifulSoup(page.text, 'html.parser')

        # define tables
        sport_tables = get_vsin_sport_tables(soup)

        df_dict = {}

        for sport in sport_tables:
            if sport_tables[sport].empty:
                continue
            else:
                clean_df = clean_vsin_df(sport_tables[sport])

                clean_df.columns = ['Team_names', 'Spread', 'Spread_handle', 'Spread_bets', 'Total', 'Total_handle',
                                    'Total_bets', 'Moneyline', 'Moneyline_handle', 'Moneyline_bets', 'game_date']
                game_dict_list = clean_df.to_dict('records')
                clean_list = []
                for game_dict in game_dict_list:
                    clean_list.append(clean_data(game_dict))

                clean_df = pd.DataFrame(clean_list)
                clean_df['Sport'] = sport
                df_dict[sport] = clean_df

        # combine dfs
        df_list = list(df_dict.values())
        main_df = pd.concat(df_list, axis=0)

        # add in clean names
        main_df = self.add_vsin_ref_names(main_df)

        # clean date and generate id
        main_df = generate_game_date_ID(main_df)

        return main_df

    def close_sql(self):
        self.connection.close()

    def generate_and_update_vsin(self):
        df = self.generate_new_vsin_df()
        print('New VSIN Generated...')
        self.close_sql()
        post_updated_vsin(df)
        print('~~VSIN Updated to Database~~')


def update_vsin():
    start = time.time()
    vsin = RefreshVSIN()
    vsin.generate_and_update_vsin()
    end = time.time()
    print(f"VSIN time taken: {end - start:.2f} seconds")


if __name__ == '__main__':
    update_vsin()