import pandas as pd


def find_ref_dfs():
    try:
        team_ref_df = pd.read_csv('/var/www/html/Website/Team_Reference.csv', encoding='ISO-8859-1')
        sport_ref_df = pd.read_csv('/var/www/html/Website/Sport_Reference.csv')
    except:
        team_ref_df = pd.read_csv('Team_Reference.csv', encoding='ISO-8859-1')
        sport_ref_df = pd.read_csv('Sport_Reference.csv')
    return team_ref_df, sport_ref_df


team_ref_df, sport_ref_df = find_ref_dfs()

class ConnectSources:
    def __init__(self, bov_df, vsin_df, espn_df, team_ref_df, sport_ref_df):
        self.bov_df = bov_df
        self.vsin_df = vsin_df
        self.espn_df = espn_df
        self.team_ref_df = team_ref_df
        self.sport_ref_df = sport_ref_df

    def merge_all_sources(self):
        # combine vsin and bovada
        vsin_df = self.vsin_df.dropna(subset=['DC_Game_ID'])
        merge_df = self.bov_df.merge(vsin_df, on='DC_Game_ID', how='left')

        # combine espn
        espn_df = self.espn_df.dropna(subset=['DC_Game_ID'])
        merge_df = merge_df.merge(espn_df, on='DC_Game_ID', how='left')

        merge_df = merge_df.drop(columns=['game_date_y', 'game_time_y'])
        merge_df = merge_df.rename(columns={'game_date_x': 'game_date', 'game_time_x': 'game_time'})

        #merge_df = merge_df.dropna(subset=['Sport'])

        # drop duplicates (when multiple matchups in back to back games like baseball)
        merge_df = merge_df.drop_duplicates(subset='game_id', keep='first')

        merge_df = self.clean_sports(merge_df)

        return merge_df

    def clean_sports(self, df):
        final_df = df.merge(self.sport_ref_df, on='game_sport', how='left')

        return final_df


if __name__ == '__main__':
    con = ConnectSources(pd.read_csv('test_bov.csv'), pd.read_csv('vsin.csv'), pd.read_csv('espn_test.csv'), pd.read_csv('Team_Reference.csv'), pd.read_csv('Sport_Reference.csv'))
    con.merge_all_sources().to_csv('merged_connection.csv', index=False)



