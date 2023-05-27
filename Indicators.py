import pandas as pd
from bovadaAPI import PullBovada
from vsinAPI import VsinSharp
from connectSources import ConnectSources

#ref_df = pd.read_csv('References.csv')
ref_df = pd.read_csv('/var/www/html/Website/References.csv')


class Indicators:
    def __init__(self):
        bov = PullBovada()
        vsin = VsinSharp()
        self.bov_df = bov.scrape_main_sports()
        self.vsin_df = vsin.get_data()
        conn = ConnectSources(self.bov_df, self.vsin_df, ref_df)
        self.merged_df = conn.merge_vsin_data()

    def sharp_indicator(self):
        merged_df = self.merged_df

        merged_df['sharp_spread_ind'] = merged_df.apply(self.sharp_spread, axis=1)
        merged_df['sharp_moneyline_ind'] = merged_df.apply(self.sharp_moneyline, axis=1)
        merged_df['sharp_total_ind'] = merged_df.apply(self.sharp_overunder, axis=1)

        return merged_df

    def sharp_spread(self, row):
        if (row['away_spread_handle'] > row['home_spread_handle']) and (row['away_spread_bets'] > row['home_spread_bets']):
            return 'Away'
        elif (row['away_spread_handle'] < row['home_spread_handle']) and (row['away_spread_bets'] < row['home_spread_bets']):
            return 'Home'
        else:
            return None

    def sharp_moneyline(self, row):
        if (row['away_ml_handle'] > row['home_ml_handle']) and (row['away_ml_bets'] > row['home_ml_bets']):
            return 'Away'
        elif (row['away_ml_handle'] < row['home_ml_handle']) and (row['away_ml_bets'] < row['home_ml_bets']):
            return 'Home'
        else:
            return None

    def sharp_overunder(self, row):
        if (row['total_over_handle'] > row['total_under_handle']) and (row['total_over_bets'] > row['total_under_bets']):
            return 'Over'
        elif (row['total_over_handle'] < row['total_under_handle']) and (row['total_over_bets'] < row['total_under_bets']):
            return 'Under'
        else:
            return None


if __name__ == '__main__':
    ind = Indicators()
    ind.sharp_indicator().to_csv("test.csv")
