import pandas as pd
from sqlalchemy import create_engine
from SQL_Auth import host, user, password, database
import requests
import json


def two_way_hcap(mkt):
    try:
        price0, price1 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price']
        return {'team_1_hcap': price0['handicap'],
                'team_1_hcap_odds': price0['american'],
                'team_2_hcap': price1['handicap'],
                'team_2_hcap_odds': price1['american']}
    except Exception as e:
        print(f"Error in two_way_hcap: {e}")
        return {}


def two_way_12(mkt):
    try:
        price0, price1 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price']
        return {'team_1_ml_odds': price0['american'],
                'team_2_ml_odds': price1['american']}
    except Exception as e:
        print(f"Error in two_way_ml: {e}")
        return {}


def three_way_1X2(mkt):
    price0, price1, price2 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price'], mkt['outcomes'][2][
        'price']
    return {'team_1_ml_odds': price0['american'],
            'team_2_ml_odds': price1['american'],
            'draw_ml_odds': price2['american']}


def two_way_OU(mkt):
    try:
        price0, price1 = mkt['outcomes'][0]['price'], mkt['outcomes'][1]['price']
        return {f'{mkt["outcomes"][0]["description"]}_line': price0['handicap'],
                f'{mkt["outcomes"][0]["description"]}_odds': price0['american'],
                f'{mkt["outcomes"][1]["description"]}_line': price1['handicap'],
                f'{mkt["outcomes"][1]["description"]}_odds': price1['american']}
    except Exception as e:
        print(f"Error in two_way_OU: {e}")
        try:
            description0 = mkt["outcomes"][0]["description"]
            description1 = mkt["outcomes"][1]["description"]

            return {f'{description0}_line': '',
                    f'{description0}_odds': '',
                    f'{description1}_line': '',
                    f'{description1}_odds': ''}

        except Exception:
            return {}


def parse_game_data(game):
    game_dict = {f'game_{key}': value for key, value in game.items() if not isinstance(value, list)}

    for competitor in game['competitors']:
        if competitor['home']:
            game_dict['bovada_home_team'] = competitor['name']
        elif not competitor['home']:
            game_dict['bovada_away_team'] = competitor['name']

    key_map = {
        '2W-HCAP': two_way_hcap,
        '2W-12': two_way_12,
        '3W-1X2': three_way_1X2,
        '2W-OU': two_way_OU
    }

    for mkt in game['displayGroups'][0]['markets']:
        if mkt['key'] in key_map:
            game_dict.update(key_map[mkt['key']](mkt))

    return game_dict


def get_json(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json'
    }
    response = requests.get(url, headers=headers)
    try:
        return response.json()
    except json.decoder.JSONDecodeError:
        print(f"Failed to decode JSON from response. Response text was:\n{response.text}")
        return None


def generate_game_date_ID(df):
    """use MMDDYYYY-Away-Home clean team names as ID"""
    df['DC_Game_ID'] = df['cst_game_startTime'].dt.strftime('%m%d%Y') + '-' + df['away_team_clean'] + '-' + df['home_team_clean']

    return df


def scrape_single_page(url):
    site_json = get_json(url)
    if not site_json:
        return []
    return [parse_game_data(game) for game in site_json[0]['events']]


def extract_segments(url):
    segments = url.split("/")
    return segments[1], segments[2] if len(segments) > 2 else None


def clean_bovada_df(df):
    # clean up dataframe
    # even to odds
    df = df.replace('EVEN', 100)

    # add date columns
    df['bovada_sport'], df['bovada_league'] = zip(*df['game_link'].apply(extract_segments))
    df['game_startTime'] = pd.to_datetime(df['game_startTime'], unit='ms')
    df['cst_game_startTime'] = df['game_startTime'].dt.tz_localize('UTC').dt.tz_convert('America/Chicago')
    df['cst_game_date'] = df['cst_game_startTime'].dt.date
    df['cst_game_time'] = df['cst_game_startTime'].dt.strftime('%I:%M %p')

    return df


def post_updated_bovada(df):
    # Create a SQLAlchemy engine
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    # Upload the DataFrame to MySQL as a new table
    df.to_sql(name='bovada_data', con=engine, if_exists='replace', index=False)