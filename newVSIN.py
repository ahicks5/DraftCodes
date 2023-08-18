from vsinHelperFunctions import *
import mysql.connector
import requests
from bs4 import BeautifulSoup
import time
from SQL_Auth import host, user, password, database


class RefreshVSIN:
    def __init__(self):
        self.connection = mysql.connector.connect(
            host=host,  # Server, usually localhost
            user=user,  # your username, e.g., root
            password=password,  # your password
            database=database  # Name of the database
        )

    def get_vsin_links(self):
        urls = ['https://data.vsin.com/draftkings/betting-splits/?view=mlb',
                'https://data.vsin.com/draftkings/betting-splits/?view=nfl',
                'https://data.vsin.com/draftkings/betting-splits/?view=cfb']
                #'https://data.vsin.com/draftkings/betting-splits/?view=epl'
        final_links = []
        for url in urls:
            page = requests.get(url)
            soup = BeautifulSoup(page.text, 'lxml')

            all_links = soup.find_all('a')
            for link in all_links:
                a = link['href']
                if 'modalpage=dksplitsgame' in a:
                    final_links.append(f'https://data.vsin.com{a}')

        final_links = list(set(final_links))

        return final_links

    def get_teamrefs_from_db(self):
        cursor = self.connection.cursor()
        query = "SELECT * FROM team_reference"
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)

        cursor.close()

        return df

    def add_vsin_ref_names(self, df):
        # merge away team
        team_ref_df_espn = self.get_teamrefs_from_db()[['VSIN_Backend', 'Final Names']]

        df = df.merge(team_ref_df_espn, left_on='away_team', right_on='VSIN_Backend', how='left')
        df = df.rename(columns={'Final Names': 'away_team_clean'})
        df = df.drop(columns=['VSIN_Backend'])

        df = df.merge(team_ref_df_espn, left_on='home_team', right_on='VSIN_Backend', how='left')
        df = df.rename(columns={'Final Names': 'home_team_clean'})
        df = df.drop(columns=['VSIN_Backend'])

        return df

    def parse_recent_sharps(self, soup):
        # if not there
        if len(soup.find_all('table')) < 2:
            return {}

        # Find the table
        table = soup.find_all('table')[1]

        # Get the first row after the headers (the one you are interested in)
        if len(table.find_all('tr')) < 4:
            row = table.find_all('tr')[1]
        else:
            row = table.find_all('tr')[4]

        # Create dictionary for game data
        game_data = {}

        # "Opening Split" column
        opening_split = row.find_all('td')[0].text.strip()
        game_data['as_of_date'] = opening_split[0:10].strip()
        game_data['as_of_time'] = opening_split[10:].strip()

        # Team names
        teams = row.find_all('td')[1].text.strip().split('\xa0')
        game_data['away_team'] = teams[0].strip()
        game_data['home_team'] = teams[1].strip()

        # Three sections: Spread, Total, and Money
        sections = ['spread', 'total', 'money']
        for i, section in enumerate(sections):
            if section == 'total':
                game_data[f'over_{section}'], game_data[f'under_{section}'] = clean_over_under(
                    row.find_all('td')[2 + i * 3])
                game_data[f'over_{section}_handle'], game_data[f'under_{section}_handle'] = clean_percents(
                    row.find_all('td')[3 + i * 3].text)
                game_data[f'over_{section}_bets'], game_data[f'under_{section}_bets'] = clean_percents(
                    row.find_all('td')[4 + i * 3].text)

            else:
                game_data[f'away_{section}'], game_data[f'home_{section}'] = clean_spread(
                    row.find_all('td')[2 + i * 3].text)
                game_data[f'away_{section}_handle'], game_data[f'home_{section}_handle'] = clean_percents(
                    row.find_all('td')[3 + i * 3].text)
                game_data[f'away_{section}_bets'], game_data[f'home_{section}_bets'] = clean_percents(
                    row.find_all('td')[4 + i * 3].text)

        return game_data  # Return the data for this row

    def parse_summary_table(self, soup):
        # Find the table
        table = soup.find('table', {'id': 'summarytable'})

        # Get the headers
        headers = [header.text for header in table.find('thead').find_all('th')]

        # Get table rows
        rows = table.find('tbody').find_all('tr')

        # Initialize the dictionary to store the game data
        game_data = {'away_team': '', 'home_team': '', 'estimated_total': 0, 'over_under': ''}

        for idx, row in enumerate(rows):
            columns = row.find_all('td')
            team_type = 'away' if idx == 0 else 'home'

            for header, column in zip(headers, columns):
                if ('ET' in header) and (':' in header):  # Team name and game time
                    game_data['game_time'] = header
                    game_data[f'{team_type}_team'] = column.text.strip()
                elif header == 'Line':  # Line to float
                    game_data[team_type + '_line'] = column.find('div').text.strip()
                elif header == 'OU':  # Over Under
                    if team_type == 'home':
                        game_data['over_under'] = column.find('div').text.strip() if column.find('div') else ''
                elif header == 'EST Line':  # Estimated total and differential
                    if team_type == 'away':
                        o_u = column.find('div').text.strip()
                        if 'UN' in o_u or 'OV' in o_u:
                            game_data['estimated_total'] = o_u[:-2]
                            game_data['vsin_prediction'] = o_u[-2:]
                        else:
                            game_data['estimated_total'] = o_u
                    else:
                        game_data['estimated_home_ml'] = column.find('div').text.strip()
                elif header == '':
                    game_data[team_type + '_' + 'current_score'] = column.find('div').text.strip()
                else:
                    if column.find('div'):  # Check if cell has a div
                        game_data[team_type + '_' + header.lower().replace(' ', "_")] = column.find('div').text.strip()
                    else:
                        game_data[team_type + '_' + header.lower().replace(' ', "_")] = column.text.strip()

        return game_data

    def generate_new_vsin_df(self):
        print('VSIN started...')
        links = self.get_vsin_links()
        final_dicts = []
        for link in links:
            page = requests.get(link)
            soup = BeautifulSoup(page.text, 'lxml')

            summary = self.parse_summary_table(soup)
            sharps = self.parse_recent_sharps(soup)

            final_dict = {**summary, **sharps}
            final_dict['link'] = link
            final_dict['game_date'] = link.split('gameid=')[1][:-8]
            final_dicts.append(final_dict)

        df = pd.DataFrame(final_dicts)
        print('VSIN generated...')

        # add draftcodes id
        df = self.add_vsin_ref_names(df)
        df = generate_game_date_ID(df)

        post_updated_vsin(df)
        print('~~VSIN Updated to Database~~')


def update_vsin():
    start = time.time()
    v = RefreshVSIN()
    v.generate_new_vsin_df()
    end = time.time()
    print(f"VSIN time taken: {end - start:.2f} seconds")


if __name__ == '__main__':
    update_vsin()