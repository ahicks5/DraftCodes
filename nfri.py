import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np


class NFRI:
    def __init__(self, data_csv):
        self.data_csv = data_csv

    def historical_df(self):
        df = pd.read_csv(self.data_csv)

        # clean up the date
        df['date'] = df['date'].astype('str')
        df['date'] = df['date'].str[:-1]
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

        # adjust team names
        name_changes = {
            'Los Angeles Angels of Anaheim': 'Los Angeles Angels',
            'Cleveland Indians': 'Cleveland Guardians',
            'Florida Marlins': 'Miami Marlins'
        }

        for key in name_changes:
            df = df.replace(key, name_changes[key])

        return df

    def pitcher_names(self, df):
        if df.empty:
            df = self.historical_df()
            combined_columns = df['away_s_pitcher'].append(df['home_s_pitcher'])
            pitchers = combined_columns.unique().tolist()
            pitchers.sort()
        else:
            combined_columns = df['away_s_pitcher'].append(df['home_s_pitcher'])
            pitchers = combined_columns.unique().tolist()
            pitchers.sort()
        return pitchers

    def team_names(self, df):
        if df.empty:
            df = self.historical_df()
            combined_columns = df['away_team'].append(df['home_team'])
            teams = combined_columns.unique().tolist()
            teams.sort()
        else:
            combined_columns = df['away_team'].append(df['home_team'])
            teams = combined_columns.unique().tolist()
            teams.sort()
        return teams

    def pitcher_df(self, pitcher_name):
        df = self.historical_df()
        # away games
        df_away = df[df['away_s_pitcher'] == pitcher_name]
        away_cols = ['game_link', 'date', 'home_team', 'away_s_pitcher']
        for col in list(df_away.columns):
            if col[:2] == 'a_' and col[-1].isdigit():
                away_cols.append(col)
        df_away = df_away[away_cols]

        # home games
        df_home = df[df['home_s_pitcher'] == pitcher_name]
        home_cols = ['game_link', 'date', 'away_team', 'home_s_pitcher']
        for col in list(df_home.columns):
            if col[:2] == 'h_' and col[-1].isdigit():
                home_cols.append(col)
        df_home = df_home[home_cols]

        fin_col_names = ['game_link', 'date', 'opposing_team', 'pitcher']
        for col in list(df_away.columns):
            if col in fin_col_names:
                continue
            elif col[-1].isdigit():
                new_col = 'inning_' + col.split('_')[1]
                fin_col_names.append(new_col)

        df_away.columns = fin_col_names
        df_home.columns = fin_col_names

        df_pitcher = pd.concat([df_away, df_home], axis=0)

        return df_pitcher

    def team_df(self, team_name):
        df = self.historical_df()
        # away games
        df_away = df[df['away_team'] == team_name]
        away_cols = ['game_link', 'date', 'away_team', 'home_s_pitcher']
        for col in list(df_away.columns):
            if col[:2] == 'a_' and col[-1].isdigit():
                away_cols.append(col)
        df_away = df_away[away_cols]

        # home games
        df_home = df[df['home_team'] == team_name]
        home_cols = ['game_link', 'date', 'home_team', 'away_s_pitcher']
        for col in list(df_home.columns):
            if col[:2] == 'h_' and col[-1].isdigit():
                home_cols.append(col)
        df_home = df_home[home_cols]

        fin_col_names = ['game_link', 'date', 'current_team', 'pitcher']
        for col in list(df_away.columns):
            if col in fin_col_names:
                continue
            elif col[-1].isdigit():
                new_col = 'inning_' + col.split('_')[1]
                fin_col_names.append(new_col)

        df_away.columns = fin_col_names
        df_home.columns = fin_col_names

        df_team = pd.concat([df_away, df_home], axis=0)

        return df_team

    def nrfi_percent_pitcher(self, pitcher_name, end_date=0):
        df_pitcher = self.pitcher_df(pitcher_name)

        if end_date != 0:
            hist_mask = df_pitcher['date'] < end_date
            df_pitcher = df_pitcher[hist_mask]

        game_num = len(df_pitcher)
        nrfi_count = (df_pitcher['inning_1'] > 0).sum()
        nrfi_percent = nrfi_count / game_num

        return nrfi_percent

    def nrfi_percent_team(self, team_name, end_date=0):
        df_team = self.team_df(team_name)

        if end_date != 0:
            hist_mask = df_team['date'] < end_date
            df_team = df_team[hist_mask]

        game_num = len(df_team)
        nrfi_count = (df_team['inning_1'] > 0).sum()
        nrfi_percent = nrfi_count / game_num

        return nrfi_percent

    def all_pitcher_nrfi(self, names, end_date=0):
        all_data = {}
        for pitcher in names:
            all_data[pitcher] = self.nrfi_percent_pitcher(pitcher, end_date)
            print(pitcher, all_data[pitcher])

        return all_data

    def all_team_nrfi(self, names, end_date=0):
        all_data = {}
        for team in names:
            all_data[team] = self.nrfi_percent_team(team, end_date)
            print(team, all_data[team])

        return all_data

    def get_pitchers_teams_from_testset(self, new_df, end_date):
        # get names of teams / players
        comb_col = new_df['away_s_pitcher'].append(new_df['home_s_pitcher'])
        test_pitchers = comb_col.unique().tolist()
        test_pitchers.sort()

        comb_col = new_df['away_team'].append(new_df['home_team'])
        test_teams = comb_col.unique().tolist()
        test_teams.sort()

        pitcher_data = self.all_pitcher_nrfi(test_pitchers, end_date)
        team_data = self.all_team_nrfi(test_teams, end_date)

        return pitcher_data, team_data

    def test_model(self, end_date, p_wt, t_wt, pitcher_data, team_data, new_df):
        for index, row in new_df.iterrows():
            top_inn = (pitcher_data[row['home_s_pitcher']] * p_wt) + (team_data[row['away_team']] * t_wt)
            bot_inn = (pitcher_data[row['away_s_pitcher']] * p_wt) + (team_data[row['home_team']] * t_wt)

            first_inn_score_percent = 1 - (1 - top_inn) * (1 - bot_inn)

            if first_inn_score_percent > 0.5:
                prediction = True
                if (row['a_1'] + row['h_1']) > 0:
                    outcome = True
                else:
                    outcome = False
            else:
                prediction = False
                if (row['a_1'] + row['h_1']) > 0:
                    outcome = True
                else:
                    outcome = False

            if prediction == outcome:
                success = True
            else:
                success = False

            new_df.loc[index, 'NRFI_predict'] = prediction
            new_df.loc[index, 'NRFI_outcome'] = outcome
            new_df.loc[index, 'NRFI_success'] = success

        fin_dict = {
            'end_date': end_date,
            'p_wt': p_wt,
            't_wt': t_wt,
            'success_rate': new_df['NRFI_success'].mean()
        }

        return fin_dict

    def optimize_wts(self):
        historical_df = self.historical_df()

        dates = ['12/31/2022', '7/1/2022', '12/31/2021', '7/1/2021', '12/31/2020', '7/1/2020', '12/31/2019', '7/1/2019']

        final_list = []

        for date in dates:
            new_df = historical_df[historical_df['date'] >= date]
            pitcher_data = self.all_pitcher_nrfi(self.pitcher_names(new_df), date)
            team_data = self.all_team_nrfi(self.team_names(new_df), date)

            for i in range(0, 100):
                fin_dict = self.test_model(date, i/100, (1-i/100), pitcher_data, team_data, new_df)
                final_list.append(fin_dict)
                df = pd.DataFrame(final_list)
                df.to_csv('optimize_wts.csv', index=False)
                print(fin_dict)

    @staticmethod
    def contour_map_wt_optimize(df):
        #for date in ['12/31/2022', '7/1/2022', '12/31/2021', '7/1/2021', '12/31/2020', '7/1/2020', '12/31/2019', '7/1/2019']:
        date = '7/1/2022'
        df = df[df['end_date'] == date]
        df = df.drop(columns=['end_date'])

        # Reshape the p_wt and t_wt values to a 10x10 grid
        p_wt_grid = df['p_wt'].values.reshape(10, 10)
        t_wt_grid = df['t_wt'].values.reshape(10, 10)

        # Reshape the success_rate values to a 10x10 grid
        success_rate_grid = df['success_rate'].values.reshape(10, 10)

        # Generate the contour plot
        plt.figure()
        contour = plt.contour(p_wt_grid, t_wt_grid, success_rate_grid, levels=20, cmap='RdYlBu_r')
        plt.xlabel('P Weight')
        plt.ylabel('T Weight')
        plt.colorbar(contour, label='Success Rate')
        plt.title(f'Success Rate Contour Plot: {date}')
        plt.show()


if __name__ == '__main__':
    analysis = NFRI(r'All_Games.csv')
    analysis.optimize_wts()