from connectSources import find_ref_dfs
import pandas as pd
from datetime import datetime
import pytz
from loadCSVs import load_previous_data
try:
    BOVADA_FILEPATH = '/var/www/html/Website/' + 'Bovada_Data.csv'
    ESPN_FILEPATH = '/var/www/html/Website/' + 'ESPN_Data.csv'
except:
    BOVADA_FILEPATH = 'Bovada_Data.csv'
    ESPN_FILEPATH = 'ESPN_Data.csv'

class cleanFiles:
    def __init__(self):
        self.espn_df = load_previous_data()['espn_data']
        self.bovada_df = load_previous_data()['bovada_data']

    def clean_all(self):
        # Convert the strings in 'game_time' to datetime objects
        self.espn_df['game_time'] = pd.to_datetime(self.espn_df['game_time'], infer_datetime_format=True)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        new_espn = self.espn_df[self.espn_df['game_time'] >= today]

        try:
            new_espn.to_csv('/var/www/html/Website/' + 'ESPN_Data.csv', index=False)
        except:
            new_espn.to_csv('ESPN_Data.csv', index=False)


        # Convert the strings in 'game_startTime_cst' to datetime objects
        self.bovada_df['game_startTime_cst'] = pd.to_datetime(self.bovada_df['game_startTime_cst'],
                                                              infer_datetime_format=True)
        # Get today's date in timezone-aware format (and time set to midnight)
        cdt = pytz.timezone('America/Chicago')
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        today = cdt.localize(today)

        new_bov = self.bovada_df[self.bovada_df['game_startTime_cst'] >= today]
        try:
            new_bov.to_csv('/var/www/html/Website/' + 'Bovada_Data.csv', index=False)
        except:
            new_bov.to_csv('Bovada_Data.csv', index=False)

        print('All clean!')


if __name__ == '__main__':
    clean = cleanFiles()
    clean.clean_all()