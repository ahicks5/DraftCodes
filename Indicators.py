import pandas as pd
from bovadaAPI import PullBovada
from vsinAPI import VsinSharp
from connectSources import ConnectSources
from espnAPI import PullESPN
from connectSources import find_ref_dfs
import time

team_ref_df, sport_ref_df = find_ref_dfs()

class Indicators:
    def __init__(self):
        bov = PullBovada()
        vsin = VsinSharp()
        espn = PullESPN()
        self.bov_df = bov.scrape_main_sports()
        self.vsin_df = vsin.get_data()
        self.espn_df = espn.assemble_espn_data()
        conn = ConnectSources(self.bov_df, self.vsin_df, self.espn_df, team_ref_df, sport_ref_df)
        self.merged_df = conn.merge_all_sources()

    @staticmethod
    def probability_to_moneyline(probability):
        if pd.isna(probability) or probability <= 0 or probability > 1:
            return None

        if probability <= 0.5:
            return int((100 / probability) - 100)
        else:
            return int((100 / (probability - 1)))

    def sharp_indicator(self):
        merged_df = self.merged_df

        # sharps
        merged_df['sharp_spread_ind'] = merged_df.apply(self.sharp_spread, axis=1)
        merged_df['sharp_moneyline_ind'] = merged_df.apply(self.sharp_moneyline, axis=1)
        merged_df['sharp_total_ind'] = merged_df.apply(self.sharp_overunder, axis=1)

        # espn
        merged_df = self.add_espn_ml_spread(merged_df)
        merged_df['espn_moneyline_ind'] = merged_df.apply(self.espn_ml_ind, axis=1)

        # streaks
        merged_df['espn_streak_ind'] = merged_df.apply(self.espn_streaks, axis=1)

        return merged_df

    def add_espn_ml_spread(self, df):
        # Convert percentages to float probabilities
        df['team1_pred'] = df['team1_pred'].str.rstrip('%').astype('float') / 100.0
        df['team2_pred'] = df['team2_pred'].str.rstrip('%').astype('float') / 100.0

        df['espn_team1_ml'] = df['team1_pred'].apply(self.probability_to_moneyline)
        df['espn_team2_ml'] = df['team2_pred'].apply(self.probability_to_moneyline)

        return df

    def espn_ml_ind(self, row):
        if pd.isna(row['espn_team1_ml']) or pd.isna(row['espn_team1_ml']) or pd.isna(row['team_1_ml_odds']) or pd.isna(row['team_2_ml_odds']):
            return 0

        if row['team_1_ml_odds'] == 'EVEN':
            row['team_1_ml_odds'] = 100

        if row['team_2_ml_odds'] == 'EVEN':
            row['team_2_ml_odds'] = 100

        final_score = 0

        if float(row['espn_team1_ml']) < float(row['team_1_ml_odds']):
            final_score += 1

        if float(row['espn_team2_ml']) < float(row['team_2_ml_odds']):
            final_score += -1

        return final_score


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

    def espn_streaks(self, row):
        try:
            final_score = 0

            # overall streaks
            if row['A_espn_current_streak'][0] == 'W' and int(row['A_espn_current_streak'][1:]) >= 3:
                final_score += 1
            elif row['A_espn_current_streak'][0] == 'L' and int(row['A_espn_current_streak'][1:]) >= 3:
                final_score += -1

            if row['H_espn_current_streak'][0] == 'W' and int(row['H_espn_current_streak'][1:]) >= 3:
                final_score += -1
            elif row['H_espn_current_streak'][0] == 'L' and int(row['H_espn_current_streak'][1:]) >= 3:
                final_score += 1

            # away/home streaks
            if row['A_espn_away_streak'][0] == 'W' and int(row['A_espn_away_streak'][1:]) >= 3:
                final_score += 1
            elif row['A_espn_away_streak'][0] == 'L' and int(row['A_espn_away_streak'][1:]) >= 3:
                final_score += -1

            if row['H_espn_home_streak'][0] == 'W' and int(row['H_espn_home_streak'][1:]) >= 3:
                final_score += -1
            elif row['H_espn_home_streak'][0] == 'L' and int(row['H_espn_home_streak'][1:]) >= 3:
                final_score += 1

            return final_score

        except:
            return 0


if __name__ == '__main__':
    start_time = time.time()

    ind = Indicators()
    ind.sharp_indicator().to_csv("indicators.csv")

    end_time = time.time()
    print(f'Script took {end_time - start_time}')
