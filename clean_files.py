from connectSources import find_ref_dfs
import pandas as pd
from datetime import datetime
import pytz

team_ref_df, sport_ref_df, espn_df, bovada_df = find_ref_dfs()

try:
    ind_df = pd.read_csv('/var/www/html/Website/Indicator_Data.csv')
except:
    ind_df = pd.read_csv('Indicator_Data.csv')

class cleanFiles:
    def __init__(self):
        self.espn_df = espn_df
        self.bovada_df = bovada_df
        self.ind_df = ind_df

    def clean_all(self):
        # Convert the strings in 'game_time' to datetime objects
        self.espn_df['game_time'] = pd.to_datetime(self.espn_df['game_time'], format="%Y-%m-%d %H:%M:%S")
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        new_espn = self.espn_df[self.espn_df['game_time'] >= today]

        try:
            new_espn.to_csv('/var/www/html/Website/ESPN_Data.csv', index=False)
        except:
            new_espn.to_csv('ESPN_Data.csv', index=False)

        # Convert the strings in 'game_startTime_cst' to datetime objects
        self.bovada_df['game_startTime_cst'] = pd.to_datetime(self.bovada_df['game_startTime_cst'],
                                                              format="%Y-%m-%d %H:%M:%S%z")
        # Get today's date in timezone-aware format (and time set to midnight)
        cdt = pytz.timezone('America/Chicago')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        today = cdt.localize(today)
        new_bov = self.bovada_df[self.bovada_df['game_startTime_cst'] >= today]

        try:
            new_bov.to_csv('/var/www/html/Website/Bovada_Data.csv', index=False)
        except:
            new_bov.to_csv('Bovada_Data.csv', index=False)


if __name__ == '__main__':
    clean = cleanFiles()
    clean.clean_all()