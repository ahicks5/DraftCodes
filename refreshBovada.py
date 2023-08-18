import mysql.connector
from bovadaHelperFunctions import *
import time
from SQL_Auth import host, user, password, database


class RefreshBovada:
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

    def get_bovada_data_from_db(self):
        cursor = self.connection.cursor()
        query = "SELECT * FROM bovada_data"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        cursor.close()

        return df

    def get_sport_links_from_db(self):
        cursor = self.connection.cursor()
        query = "SELECT * FROM bovada_sport_links"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        cursor.close()

        return dict(zip(df['Sport'], df['Link']))

    def add_bov_ref_names(self, df):
        team_ref_df_bov = self.get_teamrefs_from_db()[['Bovada Names', 'Final Names']]

        df = df.merge(team_ref_df_bov, left_on='bovada_away_team', right_on='Bovada Names', how='left')
        df = df.rename(columns={'Final Names': 'away_team_clean'})
        df = df.drop(columns=['Bovada Names'])

        df = df.merge(team_ref_df_bov, left_on='bovada_home_team', right_on='Bovada Names', how='left')
        df = df.rename(columns={'Final Names': 'home_team_clean'})
        df = df.drop(columns=['Bovada Names'])

        return df

    def generate_new_bovada_df(self):
        league_lists = []
        sport_dict = self.get_sport_links_from_db()

        # only focus on mlb for now
        for sport in sport_dict:
            if sport in ['mlb', 'nfl', 'cfb', 'epl']:
                league_lists.append(scrape_single_page(sport_dict[sport]))
            else:
                continue

        all_games = [game for league in league_lists for game in league]
        df = pd.DataFrame(all_games)

        # clean formatting
        df = clean_bovada_df(df)

        # add clean names, add ID
        df = self.add_bov_ref_names(df)
        df = generate_game_date_ID(df)

        return df

    def consolidate_new_old(self, new_df):
        # load in old bovada_df
        old_df = self.get_bovada_data_from_db()

        # Filter out rows in the old DataFrame that have the same game_id as in the new DataFrame
        filtered_old_df = old_df.loc[~old_df['game_id'].isin(new_df['game_id'])]

        # Concatenate the new DataFrame with the filtered old DataFrame
        final_df = pd.concat([new_df, filtered_old_df])

        # Convert game_startTime_cst column to datetime, handling timezone-aware datetime objects
        final_df['cst_game_startTime'] = pd.to_datetime(final_df['cst_game_startTime'], utc=True)
        # Convert the timezone back to CDT
        final_df['cst_game_startTime'] = final_df['cst_game_startTime'].dt.tz_convert('America/Chicago')

        # resort
        final_df = final_df.sort_values(by='cst_game_startTime', ascending=True)

        return final_df

    def remove_old_data(self, df):
        # Convert the cst_game_date column to a datetime dtype
        df['cst_game_date'] = pd.to_datetime(df['cst_game_date'])

        # Make sure the column is timezone-naive before localizing
        df['cst_game_date'] = df['cst_game_date'].dt.tz_localize(None).dt.tz_localize('America/Chicago')

        # Get the current UTC timestamp
        today_utc = pd.Timestamp.utcnow()

        # Convert it directly to CDT (Central Time, which accounts for both CST and CDT)
        today_cdt = today_utc.tz_convert('America/Chicago').normalize()

        # Filter out dates that are before today
        filtered_df = df[df['cst_game_date'] >= today_cdt]

        return filtered_df

    def close_sql(self):
        self.connection.close()

    # FULL PROCESS
    def generate_and_update_bovada(self):
        df = self.generate_new_bovada_df()
        print('New Bovada Generated...')
        df = self.consolidate_new_old(df)
        print('Old and New Bovada Combined...')
        df = self.remove_old_data(df)
        print('All Games Prior to Today Removed...')
        self.close_sql()
        post_updated_bovada(df)
        print('~~Bovada Updated to Database~~')


def update_bovada():
    start = time.time()
    bov = RefreshBovada()
    bov.generate_and_update_bovada()
    end = time.time()
    print(f"Bovada time taken: {end - start:.2f} seconds")


if __name__ == '__main__':
    update_bovada()