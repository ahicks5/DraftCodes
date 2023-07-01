from connectSources import find_ref_dfs
import pandas as pd
from datetime import datetime
import pytz
from loadCSVs import load_previous_data


class cleanFiles:
    def __init__(self):
        self.espn_df = load_previous_data()['espn_data']
        self.bovada_df = load_previous_data()['bovada_data']

    def clean_all(self):
        cdt = pytz.timezone('America/Chicago')
        today = datetime.now(pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today = today.astimezone(cdt)

        # For ESPN Data
        self.espn_df['game_time'] = pd.to_datetime(self.espn_df['game_time'])
        # Convert to timezone-aware
        self.espn_df['game_time'] = self.espn_df['game_time'].apply(lambda x: cdt.localize(x))
        # Filter
        new_espn = self.espn_df[self.espn_df['game_time'] >= today]

        # Save to csv
        try:
            new_espn.to_csv('/var/www/html/Website/' + 'ESPN_Data.csv', index=False)
        except:
            new_espn.to_csv('ESPN_Data.csv', index=False)


        # For Bovada Data
        self.bovada_df['game_startTime_cst'] = pd.to_datetime(self.bovada_df['game_startTime_cst'])
        # Filter
        new_bov = self.bovada_df[self.bovada_df['game_startTime_cst'] >= today]

        # Save to csv
        try:
            new_bov.to_csv('/var/www/html/Website/' + 'Bovada_Data.csv', index=False)
        except:
            new_bov.to_csv('Bovada_Data.csv', index=False)

        print('All clean!')


if __name__ == '__main__':
    clean = cleanFiles()
    clean.clean_all()