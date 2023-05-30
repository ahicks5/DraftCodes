import pandas as pd

#sport_ref_df = pd.read_csv('Sport_Reference.csv')
sport_ref_df = pd.read_csv('/var/www/html/Website/Sport_Reference.csv')

class ConnectSources:
    def __init__(self, bov_df, vsin_df, ref_df, sport_ref_df):
        self.bov_df = bov_df
        self.vsin_df = vsin_df
        self.ref_df = ref_df
        self.sport_ref_df = sport_ref_df

    def add_bov_ref_names(self):
        # merge away team
        ref_df = self.ref_df[['Bovada Names', 'Final Names']]

        df = self.bov_df.merge(ref_df, left_on='competitor_2', right_on='Bovada Names', how='left')
        df = df.rename(columns={'Final Names': 'away_team_clean'})
        df = df.drop(columns=['Bovada Names'])

        df = df.merge(ref_df, left_on='competitor_1', right_on='Bovada Names', how='left')
        df = df.rename(columns={'Final Names': 'home_team_clean'})
        df = df.drop(columns=['Bovada Names'])

        return df

    def add_vsin_ref_names(self):
        # merge away team
        ref_df = self.ref_df[['VSIN Names', 'Final Names']]

        df = self.vsin_df.merge(ref_df, left_on='away_team', right_on='VSIN Names', how='left')
        df = df.rename(columns={'Final Names': 'away_team_clean'})
        df = df.drop(columns=['VSIN Names'])

        df = df.merge(ref_df, left_on='home_team', right_on='VSIN Names', how='left')
        df = df.rename(columns={'Final Names': 'home_team_clean'})
        df = df.drop(columns=['VSIN Names'])

        return df

    def merge_all_sources(self):
        bov_df = self.add_bov_ref_names()
        bov_df['Comp_GameID'] = bov_df['away_team_clean'] + '-' + bov_df['home_team_clean']
        bov_df = bov_df.dropna(subset=['Comp_GameID'])
        bov_df.to_csv('bovada_test.csv', index=False)

        vsin_df = self.add_vsin_ref_names()
        vsin_df['Comp_GameID'] = vsin_df['away_team_clean'] + '-' + vsin_df['home_team_clean']
        vsin_df = vsin_df.dropna(subset=['Comp_GameID'])
        vsin_df.to_csv('vsin_test.csv', index=False)

        merge_df = bov_df.merge(vsin_df, on='Comp_GameID', how='left')
        merge_df = merge_df.drop(columns=['away_team_clean_y', 'home_team_clean_y'])
        merge_df = merge_df.rename(columns={'away_team_clean_x': 'away_team_clean', 'home_team_clean_x': 'home_team_clean'})

        merge_df = merge_df.dropna(subset=['Sport'])

        # drop duplicates (when multiple matchups in back to back games like baseball)
        merge_df = merge_df.drop_duplicates(subset='game_id', keep='first')

        merge_df = self.clean_sports(merge_df)

        return merge_df

    def clean_sports(self, df):
        final_df = df.merge(self.sport_ref_df, on='game_sport', how='left')

        return final_df


if __name__ == '__main__':
    con = ConnectSources(pd.read_csv('test_bov.csv'), pd.read_csv('vsin.csv'), pd.read_csv('Team_Reference.csv'), pd.read_csv('Sport_Reference.csv'))
    print(con.merge_all_sources())
    con.merge_all_sources().to_csv('merged_connection.csv', index=False)

    print(sport_ref_df.head())


