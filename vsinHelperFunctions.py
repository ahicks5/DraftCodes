import pandas as pd
import re
from dateutil import parser
from SQL_Auth import host, user, password, database
from sqlalchemy import create_engine
from bs4 import BeautifulSoup


def post_updated_vsin(df):
    # Create a SQLAlchemy engine
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    # Upload the DataFrame to MySQL as a new table
    df.to_sql(name='vsin_data_mlb', con=engine, if_exists='replace', index=False)


def parse_html_table(table):
    rows = []
    for row in table.find_all("tr"):
        cells = row.find_all(["th", "td"])
        cells = [cell.text for cell in cells]
        rows.append(cells)
    return pd.DataFrame(rows)


def generate_game_date_ID(df):
    """use MMDDYYYY-Away-Home clean team names as ID"""

    # Convert all values in 'game_time' to datetime objects, setting errors='coerce' to handle unconvertable values
    df['game_date'] = pd.to_datetime(df['game_date'], format='%Y%m%d', errors='coerce')

    # Create 'DC_Game_ID' column
    df['DC_Game_ID'] = df['game_date'].dt.strftime('%m%d%Y') + '-' + df['away_team_clean'] + '-' + df['home_team_clean']

    return df


def get_vsin_sport_tables(soup):
    sport_dict = {}
    for div in soup.findAll('div', {'id': True}):
        if 'dk-' in div['id']:
            if div['id'] == 'dk-ufc':  # SKIP UFC
                continue
            else:
                sport_name = div['id'].split('dk-')[1].upper()
                tables = div.find_all("table")
                if tables:
                    sport_df = parse_html_table(tables[0])
                    sport_df = sport_df.iloc[:, :10]
                    sport_dict[sport_name] = sport_df

    return sport_dict


def is_date(game_date):
    try:
        parser.parse(game_date)
        return True
    except ValueError:
        return False


def clean_vsin_df(df):
    # Step 1: Identify date rows
    # This might need to be adjusted based on the actual format of the dates
    column_a = df.columns[0]

    df['is_date'] = df[column_a].apply(is_date)

    # Step 2: Add new column with game date
    df['game_date'] = df.loc[df['is_date'], column_a]

    # Step 3: Forward fill to replace NaNs with most recent date
    df['game_date'] = df['game_date'].ffill()

    # Optionally, you might want to drop the original date rows from the dataframe
    df = df[~df['is_date']]

    # And you can drop 'is_date' column as well
    df = df.drop(columns=['is_date'])

    return df


def clean_data(games_dict):
    clean_dict = {}
    clean_dict['vsin_away_team'], clean_dict['vsin_home_team'] = clean_teams(games_dict['Team_names'])

    clean_dict['away_spread'], clean_dict['home_spread'] = clean_spread(games_dict['Spread'])

    clean_dict['away_spread_handle'], clean_dict['home_spread_handle'] = clean_percents(
        games_dict['Spread_handle'])
    clean_dict['away_spread_bets'], clean_dict['home_spread_bets'] = clean_percents(
        games_dict['Spread_bets'])

    clean_dict['total_over'], clean_dict['total_under'] = clean_total(games_dict['Total'])
    clean_dict['total_over_handle'], clean_dict['total_under_handle'] = clean_percents(
        games_dict['Total_handle'])
    clean_dict['total_over_bets'], clean_dict['total_under_bets'] = clean_percents(
        games_dict['Total_bets'])

    # protocol for moneyline
    clean_dict['away_ml'], clean_dict['home_ml'] = clean_moneyline(games_dict['Moneyline'])

    clean_dict['away_ml_handle'], clean_dict['home_ml_handle'] = clean_percents(
        games_dict['Moneyline_handle'])
    clean_dict['away_ml_bets'], clean_dict['home_ml_bets'] = clean_percents(
        games_dict['Moneyline_bets'])

    clean_dict['game_date'] = games_dict['game_date'].strip('\xa0')

    return clean_dict


def clean_teams(team_string):
    team_string = team_string.strip('\xa0')
    away_team, home_team = [team.strip() for team in team_string.split('\xa0')]

    return away_team, home_team


def clean_moneyline(ml_string):
    if ml_string == '--':
        away_ml = home_ml = ''
    else:
        away_ml, home_ml = split_at_first_non_number(ml_string)

    return away_ml, home_ml


def clean_spread(spread_string):
    if spread_string == '--':
        away_spread = ''
        home_spread = ''
    elif spread_string[-1] == '-':
        away_spread = spread_string[:-1]
        home_spread = ''
    else:
        away_spread, home_spread = split_at_first_non_number(spread_string)

    return away_spread, home_spread


def split_at_first_non_number(s):
    match = re.search("\d[^0-9\.]", s[1:])
    if match:
        split_index = match.start() + 2  # adding 2 to account for the slice offset and the non-number character
        return s[:split_index].strip(), s[split_index:].strip()
    else:
        return s, ""


def clean_percents(per_string):
    first_percent = float(per_string.split('%')[0].strip()) / 100
    second_percent = float(per_string.split('%')[1].strip()) / 100

    return first_percent, second_percent


def show_missing_refs(df, team_ref_df):
    teams_matchups = pd.concat([df['away_team'], df['home_team']]).unique()
    teams_reference = team_ref_df['VSIN Names'].unique()
    missing_teams = [team for team in teams_matchups if team not in teams_reference]

    df_missing_teams = pd.DataFrame(missing_teams, columns=['team'])
    df_missing_teams.to_csv('vsin_missing_teams.csv', index=False)


def clean_total(total_string):
    if total_string == '--':
        over = under = ''
    elif total_string[-1] == '-':
        over = total_string.split('Ov ')[1][:-1]
        under = ''
    elif total_string[:2] == '--':
        over = ''
        under = total_string.split('Un ')[1].strip()
    elif 'Un' not in total_string and 'Ov' in total_string:
        over = total_string.split('Ov ')[1].strip()
        under = ''
    elif 'Ov' not in total_string and 'Un' in total_string:
        over = ''
        under = total_string.split('Un ')[1].strip()
    else:
        over = total_string.split('Un')[0].split(' ')[1].strip()
        under = total_string.split('Un')[1].strip()

    return over, under


def clean_over_under(over_under_soup):
    divs = over_under_soup.find_all('div')

    score_1 = divs[0].text.strip()
    score_2 = divs[1].text.strip()

    return score_1, score_2
