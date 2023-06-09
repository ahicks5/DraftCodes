import pandas as pd
from bovadaAPI import PullBovada
from vsinAPI import VsinSharp
from connectSources import ConnectSources
from espnAPI import PullESPN
from connectSources import find_ref_dfs

team_ref_df, sport_ref_df = find_ref_dfs()

class Indicators:
    def __init__(self):
        bov = PullBovada()
        vsin = VsinSharp()
        espn = PullESPN()
        self.bov_df = bov.scrape_main_sports()
        self.vsin_df = vsin.get_data()
        self.espn_df = espn.assemble_espn_data()
        conn = ConnectSources(self.bov_df, self.vsin_df, self.espn_df, ref_df, sport_ref_df)
        self.merged_df = conn.merge_all_sources()

    def sharp_indicator(self):
        merged_df = self.merged_df

        merged_df['sharp_spread_ind'] = merged_df.apply(self.sharp_spread, axis=1)
        merged_df['sharp_moneyline_ind'] = merged_df.apply(self.sharp_moneyline, axis=1)
        merged_df['sharp_total_ind'] = merged_df.apply(self.sharp_overunder, axis=1)

        return merged_df

    def sharp_spread(self, row):
        try:
            away_dollar_per_bet = row['away_spread_handle'] / row['away_spread_bets']
            home_dollar_per_bet = row['home_spread_handle'] / row['home_spread_bets']

            final_score = 0

            if (row['away_spread_handle'] > 0.5) and (row['away_spread_bets'] > 0.5):
                final_score += 1
            elif (row['away_spread_handle'] > 0.5) and away_dollar_per_bet > 1:
                final_score += 2

            if (row['home_spread_handle'] > 0.5) and (row['home_spread_bets'] > 0.5):
                final_score += -1
            elif (row['home_spread_handle'] > 0.5) and home_dollar_per_bet > 1:
                final_score += -2

            return final_score
        except:
            return None


    def sharp_moneyline(self, row):
        try:
            away_dollar_per_bet = row['away_ml_handle'] / row['away_ml_bets']
            home_dollar_per_bet = row['home_ml_handle'] / row['home_ml_bets']

            final_score = 0

            if (row['away_ml_handle'] > 0.5) and (row['away_ml_bets'] > 0.5):
                final_score += 1
            elif (row['away_ml_handle'] > 0.5) and away_dollar_per_bet > 1:
                final_score += 2

            if (row['home_ml_handle'] > 0.5) and (row['home_ml_bets'] > 0.5):
                final_score += -1
            elif (row['home_ml_handle'] > 0.5) and home_dollar_per_bet > 1:
                final_score += -2

            return final_score
        except:
            return None

    def sharp_overunder(self, row):
        try:
            over_dollar_per_bet = row['total_over_handle'] / row['total_over_bets']
            under_dollar_per_bet = row['total_under_handle'] / row['total_under_bets']

            final_score = 0

            if (row['total_over_handle'] > 0.5) and (row['total_over_bets'] > 0.5):
                final_score += 1
            elif (row['total_over_handle'] > 0.5) and over_dollar_per_bet > 1:
                final_score += 2

            if (row['total_under_handle'] > 0.5) and (row['total_under_bets'] > 0.5):
                final_score += -1
            elif (row['total_under_handle'] > 0.5) and under_dollar_per_bet > 1:
                final_score += -2

            return final_score
        except:
            return None


if __name__ == '__main__':
    ind = Indicators()
    ind.sharp_indicator().to_csv("indicators.csv")
