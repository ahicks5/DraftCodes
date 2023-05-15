import requests
from bs4 import BeautifulSoup
import pandas as pd


class VsinSharp:

    def __init__(self):
        self.url = 'https://stats.vsinstats.com/pageviewer/vsinstats.php?pageid=vsindksplits'

    @staticmethod
    def percentstring_to_percent(percent):
        return float(percent.strip('%')) / 100

    def get_vsin_sport_tables(self, soup):
        print(soup)
        sport_dict = {}
        for div in soup.findAll('div', {'id': True}):
            if 'dk-' in div['id']:
                if div['id'] == 'dk-ufc':  # SKIP UFC
                    continue
                else:
                    sport_name = div['id'].split('dk-')[1].upper()
                    print(str(div))
                    sport_df = pd.read_html(str(div))[0]
                    sport_dict[sport_name] = sport_df

        return sport_dict

    def get_data(self):
        page = requests.get(self.url)
        soup = BeautifulSoup(page.text, 'html.parser')

        # define tables
        sport_tables = self.get_vsin_sport_tables(soup)

        df_dict = {}

        for sport in sport_tables:
            df = sport_tables[sport]
            df.columns = ['Team_names', 'Spread', 'Spread_handle', 'Spread_bets', 'Total', 'Total_handle',
                          'Total_bets', 'Moneyline', 'Moneyline_handle', 'Moneyline_bets']
            game_dict = df.to_dict('records')
            game_list = self.clean_data(game_dict)

            df = pd.DataFrame(game_list)
            df['Sport'] = sport
            df_dict[sport] = df

        # combine dfs
        for key in df_dict:
            if key == list(df_dict.keys())[0]:
                main_df = df_dict[key]
            else:
                main_df = pd.concat([main_df, df_dict[key]], axis=0)

        return main_df

    def clean_data(self, games_dict):
        # same clean data function logic goes here
        pass  # replace this line with your existing clean_data function logic


if __name__ == '__main__':
    vsin_sharp = VsinSharp()
    data = vsin_sharp.get_data()
    data.to_csv('vsin.csv', index=False)
