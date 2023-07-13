import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
from SQL_Auth import host, user, password, database
import mysql.connector
from sqlalchemy import create_engine


def parse_single_game(df, game_dict):
    # Prepare a list of team prefixes (for labeling purposes)
    team_prefixes = ['a', 'h']

    # Process two teams at a time
    for j in range(2):
        # Get the corresponding row
        row = df.loc[j]

        # Set the team prefix based on the order of the teams
        prefix = team_prefixes[j]

        # Add the team name, runs, hits, and errors to the dictionary
        game_dict[f'{prefix}_team'] = row['Team']
        game_dict[f'{prefix}_R'] = row['R']
        game_dict[f'{prefix}_H'] = row['H']
        game_dict[f'{prefix}_E'] = row['E']

        # Dynamically add the scores per inning to the dictionary
        innings = row.index[1:-3]  # Exclude 'Team', 'R', 'H', and 'E' columns
        for inning in innings:
            game_dict[f'{prefix}_{inning}'] = row[inning]

    # Now 'games' is a list of dictionaries where each dictionary represents a game
    return game_dict


def link_to_df(link):
    page = requests.get(link)
    soup = BeautifulSoup(page.text, 'html.parser')
    summary_box = str(soup.find('div', {'class': 'linescore_wrap'}))
    df = pd.read_html(summary_box)[0]
    # remove first col and first row for formatting
    df = df.drop(df.columns[0], axis=1)
    df = df.iloc[:-1]
    df.rename(columns={df.columns[0]: 'Team'}, inplace=True)
    df = df.replace('X', 0)

    return df


def link_to_gamedate(link):
    game_date = link.split('.shtml')[0][:-1].split('/boxes/')[1].split('/')[1].strip()[3:]
    game_date = datetime.strptime(game_date, '%Y%m%d')

    return game_date


def post_updated_bballref(df):
    # Create a SQLAlchemy engine
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    # Upload the DataFrame to MySQL as a new table
    df.to_sql(name='baseball_reference_season_games', con=engine, if_exists='replace', index=False)


def generate_game_date_ID(df):
    """use MMDDYYYY-Away-Home clean team names as ID"""
    df['game_date'] = pd.to_datetime(df['game_date'])  # Convert to datetime
    df['DC_Game_ID'] = df['game_date'].dt.strftime('%m%d%Y') + '-' + df['away_team_clean'] + '-' + df['home_team_clean']

    return df


class BaseballReference:
    def __init__(self):
        self.season_year = '2023'
        self.url = f'https://www.baseball-reference.com/leagues/majors/{self.season_year}-schedule.shtml'
        self.connection = mysql.connector.connect(
            host=host,  # Server, usually localhost
            user=user,  # your username, e.g., root
            password=password,  # your password
            database=database  # Name of the database
        )

    def get_season_baseball_from_db(self):
        cursor = self.connection.cursor()
        query = "SELECT * FROM baseball_reference_season_games"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        cursor.close()

        return df

    def close_sql(self):
        self.connection.close()

    def pull_boxscore_links(self):
        page = requests.get(self.url)
        soup = BeautifulSoup(page.text, 'html.parser')

        boxscores = []

        a_links = soup.findAll('a')
        for a in a_links:
            if a.text == 'Boxscore':
                link = f'https://www.baseball-reference.com{a["href"]}'
                boxscores.append(link)

        return boxscores

    def pull_games(self, boxscores):
        game_list = []
        for link in boxscores:
            time.sleep(3.1)
            df = link_to_df(link)
            game_date = link_to_gamedate(link)

            game_dict = {'game_link': link, 'game_date': game_date}
            game_dict = parse_single_game(df, game_dict)

            game_list.append(game_dict)

        final_df = pd.DataFrame(game_list)
        return final_df

    def get_teamrefs_from_db(self):
        cursor = self.connection.cursor()
        query = "SELECT * FROM team_reference"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        cursor.close()

        return df

    def add_bball_ref_names(self, df):
        team_ref_df_bov = self.get_teamrefs_from_db()[['Sports Reference Names', 'Final Names']]

        df = df.merge(team_ref_df_bov, left_on='a_team', right_on='Sports Reference Names', how='left')
        df = df.rename(columns={'Final Names': 'away_team_clean'})
        df = df.drop(columns=['Sports Reference Names'])

        df = df.merge(team_ref_df_bov, left_on='h_team', right_on='Sports Reference Names', how='left')
        df = df.rename(columns={'Final Names': 'home_team_clean'})
        df = df.drop(columns=['Sports Reference Names'])

        return df

    def refresh_and_update_baseball_season_games(self):
        old_df = self.get_season_baseball_from_db()
        old_df = old_df.drop_duplicates()
        old_games = old_df['game_link'].unique().tolist()
        boxscores = self.pull_boxscore_links()

        new_games = []

        # check if already accounted for

        for game in boxscores:
            if game in old_games:
                continue
            else:
                new_games.append(game)

        if len(new_games) == 0:
            print('No new games to add!')
            return

        final_df = self.pull_games(new_games)

        final_df = pd.concat([old_df, final_df], ignore_index=True)

        print('New games assembled!')

        # add clean names and ids
        final_df = self.add_bball_ref_names(final_df)
        final_df = generate_game_date_ID(final_df)
        self.close_sql()

        # upload to sql
        post_updated_bballref(final_df)

        print('Added to database!')


if __name__ == '__main__':
    br = BaseballReference()
    br.refresh_and_update_baseball_season_games()
