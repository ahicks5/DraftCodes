import pandas as pd
import mysql.connector
from SQL_Auth import host, user, password, database
from IndicatorUtils import *
from sqlalchemy import create_engine
import time


def post_updated_combined(df):
    # Create a SQLAlchemy engine
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    # Upload the DataFrame to MySQL as a new table
    df.to_sql(name='combined_data', con=engine, if_exists='replace', index=False)


def load_database(datatable):
    connection = mysql.connector.connect(
        host=host,  # Server, usually localhost
        user=user,  # your username, e.g., root
        password=password,  # your password
        database=database  # Name of the database
    )

    cursor = connection.cursor()
    query = f"SELECT * FROM {datatable}"
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=columns)

    cursor.close()
    connection.close()
    return df


def add_vsin_columns(df):
    # spread
    df['a_sp_follow_public'] = df.apply(a_sp_follow_public, axis=1)
    df['h_sp_follow_public'] = df['a_sp_follow_public'] * -1
    df['a_sp_fade_public'] = df['h_sp_follow_public']
    df['h_sp_fade_public'] = df['a_sp_follow_public']

    df['a_sp_follow_sharps'] = df.apply(a_sp_follow_sharp, axis=1)
    df['h_sp_follow_sharps'] = df['a_sp_follow_sharps'] * -1
    df['a_sp_fade_sharps'] = df['h_sp_follow_sharps']
    df['h_sp_fade_sharps'] = df['a_sp_follow_sharps']

    # moneyline
    df['a_ml_follow_public'] = df.apply(a_ml_follow_public, axis=1)
    df['h_ml_follow_public'] = df['a_ml_follow_public'] * -1
    df['a_ml_fade_public'] = df['h_ml_follow_public']
    df['h_ml_fade_public'] = df['a_ml_follow_public']

    df['a_ml_follow_sharps'] = df.apply(a_ml_follow_sharp, axis=1)
    df['h_ml_follow_sharps'] = df['a_ml_follow_sharps'] * -1
    df['a_ml_fade_sharps'] = df['h_ml_follow_sharps']
    df['h_ml_fade_sharps'] = df['a_ml_follow_sharps']

    # totals
    df['u_follow_public'] = df.apply(u_follow_public, axis=1)
    df['o_follow_public'] = df['u_follow_public'] * -1
    df['u_fade_public'] = df['o_follow_public']
    df['o_fade_public'] = df['u_follow_public']

    df['u_follow_sharps'] = df.apply(u_follow_sharp, axis=1)
    df['o_follow_sharps'] = df['u_follow_sharps'] * -1
    df['u_fade_sharps'] = df['o_follow_sharps']
    df['o_fade_sharps'] = df['u_follow_sharps']

    return df


def add_espn_columns(df):
    # add bovada implied columns
    df['a_bov_implied_ml_percent'] = df.apply(moneyline_to_probability_1, axis=1)
    df['h_bov_implied_ml_percent'] = df.apply(moneyline_to_probability_2, axis=1)

    # convert espn probs
    df['team1_pred'] = df['team1_pred'].str.rstrip('%').astype('float') / 100.0
    df['team2_pred'] = df['team2_pred'].str.rstrip('%').astype('float') / 100.0

    # add moneyline
    df['a_ml_follow_espn'] = df.apply(a_ml_follow_espn, axis=1)
    df['h_ml_follow_espn'] = df['a_ml_follow_espn'] * -1
    df['a_ml_fade_espn'] = df['h_ml_follow_espn']
    df['h_ml_fade_espn'] = df['a_ml_follow_espn']

    return df


def determine_outcome(row, team_name):
    # Away scenario
    if row['a_team'] == team_name:
        if int(row['a_R']) > int(row['h_R']):
            return 'Win'
        elif int(row['a_R']) < int(row['h_R']):
            return 'Loss'
        else:
            return '-'

    # Home scenario
    if row['h_team'] == team_name:
        if int(row['a_R']) < int(row['h_R']):
            return 'Win'
        elif int(row['a_R']) > int(row['h_R']):
            return 'Loss'
        else:
            return '-'

    return None


def get_streak(outcome):
    # We convert the series into a DataFrame and reverse it because we want to start counting from the bottom
    df = pd.DataFrame(outcome.iloc[::-1])

    # We generate a helper column that changes value when 'Outcome' changes
    df['streak_id'] = (df['Outcome'] != df['Outcome'].shift(1)).cumsum()

    # We count the number of consecutive 'Outcome'
    df['streak_length'] = df.groupby('streak_id').cumcount() + 1

    # We keep only the first row of each streak
    streak_df = df[df['streak_id'] != df['streak_id'].shift(-1)]

    # We create the final 'streak' column with 'W' or 'L' plus the length of the streak
    streak_df.loc[:, 'streak'] = streak_df['Outcome'].str[0] + streak_df['streak_length'].astype(str)

    # We reverse again to restore the original order
    streak_df = streak_df.iloc[::-1]

    # We extract the first value
    streak_value = streak_df['streak'].iat[0]

    return streak_value


def get_win_rate(outcome, n=10):
    # We count the number of 'Win' in the last n games
    win_count = outcome.tail(n).eq('Win').sum()

    # We calculate the win rate
    win_rate = win_count / n

    return win_rate


def generate_bball_ref_df(team_name):
    df = load_database('baseball_reference_season_games')
    team_df = df[(df['a_team'] == team_name) | (df['h_team'] == team_name)]

    # Assign win or lose
    team_df.loc[:, 'Outcome'] = team_df.apply(lambda row: determine_outcome(row, team_name), axis=1)

    # Step 1: separate the DataFrame
    home_games = team_df[team_df['h_team'] == team_name]
    away_games = team_df[team_df['a_team'] == team_name]

    # Step 2: select relevant columns
    home_cols = [col for col in team_df.columns if col.startswith('h_')] + ['game_date'] + ['Outcome']
    away_cols = [col for col in team_df.columns if col.startswith('a_')] + ['game_date'] + ['Outcome']

    home_games = home_games[home_cols]
    away_games = away_games[away_cols]

    home_games['home_away'] = 'Home'
    away_games['home_away'] = 'Away'

    # Step 3: remove 'h_' and 'a_' prefixes
    home_games.columns = [col.replace('h_', '') for col in home_games.columns]
    away_games.columns = [col.replace('a_', '') for col in away_games.columns]

    # Step 4: combine the two parts
    combined_df = pd.concat([home_games, away_games])

    return combined_df


class Indicators:
    def __init__(self):
        self.bballref_df = load_database('baseball_reference_season_games')
        self.bovada_df = load_database('bovada_data')
        self.espn_df = load_database('espn_data')
        self.vsin_df = load_database('vsin_data_mlb')

    def link_all(self):
        bov_df = self.bovada_df.dropna(subset=['DC_Game_ID'])
        vsin_df = self.vsin_df.dropna(subset=['DC_Game_ID'])
        espn_df = self.espn_df.dropna(subset=['DC_Game_ID'])

        df = bov_df.merge(vsin_df, how='left', on='DC_Game_ID')
        df = df.merge(espn_df, how='left', on='DC_Game_ID')

        # make unique by bovada ID to bandaid double-headers
        df = df.drop_duplicates(subset='game_id', keep='first')
        df = df.drop_duplicates(subset='DC_Game_ID', keep='first')

        #take out games if team names don't show
        df = df.dropna(subset=['away_team_clean'])

        return df

    def add_indicators(self):
        df = self.link_all()
        print('Bovada, VSIN, ESPN data combined...')

        post_updated_combined(df)
        print('Combined Df Posted...')

        return df

    def add_bballref_columns(self, df):
        # Generate stats for each team and add as new columns to main_df
        for index, row in df.iterrows():
            if row['game_sport'] == 'BASE':
                try:
                    # Get the away team stats
                    away_team = row['away_team_clean']  # replace with your actual column name for away team
                    away_stats = self.get_bball_ref_stats(away_team)  # assuming `add_bball_ref` function accepts team name as an argument

                    # Get the home team stats
                    home_team = row['home_team_clean']  # replace with your actual column name for home team
                    home_stats = self.get_bball_ref_stats(home_team)  # assuming `add_bball_ref` function accepts team name as an argument

                    # Add the stats to the main_df with the appropriate prefix
                    for key, value in away_stats.items():
                        df.loc[index, 'a_' + key] = value
                    for key, value in home_stats.items():
                        df.loc[index, 'h_' + key] = value
                except:
                    continue

        return df

    def get_bball_ref_stats(self, team_name):
        df = generate_bball_ref_df(team_name)

        df = df.sort_values('game_date', ascending=False)

        df['R'] = pd.to_numeric(df['R'], errors='coerce')

        avg_runs = df['R'].mean()
        avg_a_runs = df[df['home_away'] == 'Away']['R'].mean()
        avg_h_runs = df[df['home_away'] == 'Home']['R'].mean()

        avg_runs_l5 = df.head(5)['R'].mean()
        avg_a_runs_l5 = df[df['home_away'] == 'Away'].head(5)['R'].mean()
        avg_h_runs_l5 = df[df['home_away'] == 'Home'].head(5)['R'].mean()

        current_streak = get_streak(df['Outcome'])
        recent_win_rate = get_win_rate(df['Outcome'])

        return {
            "avg_runs": avg_runs,
            "avg_a_runs": avg_a_runs,
            "avg_h_runs": avg_h_runs,
            "avg_runs_l5": avg_runs_l5,
            "avg_a_runs_l5": avg_a_runs_l5,
            "avg_h_runs_l5": avg_h_runs_l5,
            "current_streak": current_streak,
            "recent_win_rate": recent_win_rate,
        }


def combine_all():
    start = time.time()
    ind = Indicators()
    ind.add_indicators()
    end = time.time()
    print(f"Combination time taken: {end - start:.2f} seconds")


if __name__ == '__main__':
    combine_all()
