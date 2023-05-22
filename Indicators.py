import pandas as pd
from bovadaAPI import PullBovada
from vsinAPI import VsinSharp
from connectSources import ConnectSources

ref_df = pd.read_excel('References.xlsx')


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

        merged_df['Spread_Ind'] = merged_df.apply(self.sharp_spread, axis=1)

        return merged_df

    def sharp_spread(self, row):
        if (row['away_spread_handle'] > row['home_spread_handle']) and (row['away_spread_bets'] > row['home_spread_bets']):
            return 'Away'
        elif (row['away_spread_handle'] < row['home_spread_handle']) and (row['away_spread_bets'] < row['home_spread_bets']):
            return 'Home'
        else:
            return None


if __name__ == '__main__':
    ind = Indicators()
    #print(ind.sharp_indicator())
