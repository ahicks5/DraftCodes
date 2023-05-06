import pandas as pd

class NFRI:
    def __init__(self, data_csv):
        self.data_csv = data_csv

    def historical_df(self):
        df = pd.read_csv(self.data_csv)

        # clean up the date
        df['date'] = df['date'].astype('str')
        df['date'] = df['date'].str[:-1]
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df['date'] = df['date'].dt.strftime('%m-%d-%Y')

        return df

    def pitcher_names(self):
        df = self.historical_df()
        combined_columns = df['away_s_pitcher'].append(df['home_s_pitcher'])
        pitchers = combined_columns.unique().tolist()
        pitchers.sort()
        return pitchers

    def team_names(self):
        df = self.historical_df()
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

    def all_pitcher_nrfi(self, end_date=0):
        all_data = {}
        for pitcher in self.pitcher_names():
            all_data[pitcher] = self.nrfi_percent_pitcher(pitcher, end_date)
            print(pitcher, all_data[pitcher])

        return all_data

    def all_team_nrfi(self, end_date=0):
        all_data = {}
        for team in self.team_names():
            all_data[team] = self.nrfi_percent_team(team, end_date)
            print(team, all_data[team])

        return all_data

    def test_model(self, end_date, p_wt, t_wt):
        df = self.historical_df()

        # test data
        new_mask = df['date'] >= end_date
        new_df = df[new_mask]

        pitcher_data = self.all_pitcher_nrfi(end_date)
        team_data = self.all_team_nrfi(end_date)

        df['NRFI_predict'] = 0
        df['NRFI_outcome'] = 0
        df['NRFI_success'] = 0

        for index, row in df.iterrows():
            try:
                top_inn = (pitcher_data[row['home_s_pitcher']] * p_wt) + (team_data[row['away_team']] * t_wt)
                bot_inn = (pitcher_data[row['away_s_pitcher']] * p_wt) + (team_data[row['home_team']] * t_wt)

                first_inn_score_percent = top_inn * bot_inn

                if first_inn_score_percent > 0.5:
                    prediction = True
                    if (row['a_1'] + row['h_1']) > 0:
                        outcome = True
                    else:
                        outcome = False
                elif first_inn_score_percent <= 0.5:
                    prediction = False
                    if (row['a_1'] + row['h_1']) > 0:
                        outcome = True
                    else:
                        outcome = False

                if prediction == outcome:
                    success = True
                else:
                    success = False

                df.loc[index, 'NRFI_predict'] = prediction
                df.loc[index, 'NRFI_outcome'] = outcome
                df.loc[index, 'NRFI_success'] = success
            except:
                continue

        df.to_csv('test_data.csv', index=False)


if __name__ == '__main__':
    analysis = NFRI(r'All_Games.csv')
    print(analysis.test_model('12-31-2022', 0.5, 0.5))