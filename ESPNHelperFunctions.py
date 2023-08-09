from SQL_Auth import host, user, password, database
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

pregame_sport_links = {
    #'soccer': 'https://www.espn.com/soccer/schedule/_/date/',
    #'nhl': 'https://www.espn.com/nhl/schedule/_/date/',
    #'nba': 'https://www.espn.com/nba/schedule/_/date/',
    #'nfl': 'https://www.espn.com/nfl/schedule/_/date/',
    'mlb': 'https://www.espn.com/mlb/schedule/_/date/',
    #'wnba': 'https://www.espn.com/wnba/schedule/_/date/',
    #'cfb': 'https://www.espn.com/college-football/schedule/_/date'
}


def post_updated_espn(df):
    # Create a SQLAlchemy engine
    engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    # Upload the DataFrame to MySQL as a new table
    df.to_sql(name='espn_data', con=engine, if_exists='replace', index=False)


def parse_game_stats(url):
    """Parse game stats from the given URL"""
    soup = get_soup(url)
    gameID = int(url.split('gameId=')[1].strip())
    if 'match' in url:
        sport = url.split('espn.com/')[1].split('/match')[0].strip()
    elif 'game' in url:
        sport = url.split('espn.com/')[1].split('/game')[0].strip()
    else:
        sport = 'n/a'

    if soup:
        game_time = get_game_time_date(soup)
        team_1, team_2, team_1_prediction, team_2_prediction = get_teams_and_predictions(soup)

        return {
            'gameID': gameID,
            'sport': sport,
            'game_time': game_time,
            'team1': team_1,
            'team1_pred': team_1_prediction,
            'team2': team_2,
            'team2_pred': team_2_prediction
        }
    return None


def get_game_time_date(soup):
    try:
        game_time = soup.find('span', {'data-behavior': 'date_time'})['data-date']
        game_time = pd.to_datetime(game_time)
    except:
        game_time = soup.find(lambda tag: tag.name == 'div' and
                                         tag.get('class') and
                                         "GameInfo__Meta" in tag.get('class')).text.strip()
        if 'Coverage' in game_time:
            game_time = game_time.split('Coverage')[0]

        if 'TBD, ' == game_time[:5]:
            game_time = game_time.split('TBD, ')[1]

        game_time = pd.to_datetime(game_time)

    return game_time


def get_teams_and_predictions(soup):
    try:
        team_names = soup.findAll('h2',
                                  {'class': 'ScoreCell__TeamName ScoreCell__TeamName--displayName truncate db'})
        team_1 = team_names[0].text.strip()
        team_2 = team_names[1].text.strip()
    except:
        team_1 = soup.findAll('span', {'class': 'long-name'})[0].text.strip()
        team_2 = soup.findAll('span', {'class': 'long-name'})[1].text.strip()

    try:
        matchup_predictor = soup.find('div', {'class': 'matchupPredictor'})
        predictions = matchup_predictor.select('div.matchupPredictor__teamValue')

        team_1_prediction = predictions[0].text.strip()
        team_2_prediction = predictions[1].text.strip()
    except:
        team_1_prediction = None
        team_2_prediction = None

    return team_1, team_2, team_1_prediction, team_2_prediction


def get_soup(url):
    """Fetch the page content and convert it into BeautifulSoup object"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None
    return soup


def get_game_links():
    """Get all game links from the pregame sports links"""
    today_date, tom_date = today_tom_date_url()
    links = []
    for day in [today_date, tom_date]:
        for sport, espn_link in pregame_sport_links.items():
            soup = get_soup(espn_link + day)
            if soup:
                links.extend(extract_game_links(soup))
    return links


def extract_game_links(soup):
    """Extract game links from the parsed HTML"""
    links = []
    leagues = soup.select('div.ScheduleTables.mb5')
    for league in leagues:
        tbody = league.find('tbody', {'class': 'Table__TBODY'})
        games = tbody.findAll('tr')
        for game in games:
            a_links = game.findAll('a')
            for tag in a_links:
                href = tag.get('href')
                if href and 'gameId=' in href:
                    links.append(f'https://www.espn.com{href}')
    return list(set(links))


def today_tom_date_url():
    # Get today's date
    today = datetime.now()

    # Format today's date as 'YYYYMMDD'
    today_str = today.strftime('%Y%m%d')

    # Get tomorrow's date
    tomorrow = today + timedelta(days=3)

    # Format tomorrow's date as 'YYYYMMDD'
    tomorrow_str = tomorrow.strftime('%Y%m%d')

    return today_str, tomorrow_str


def generate_game_date_ID(df):
    """use MMDDYYYY-Away-Home clean team names as ID"""
    # Convert all values in 'game_time' to datetime objects, setting errors='coerce' to handle unconvertable values
    df['game_time'] = pd.to_datetime(df['game_time'], errors='coerce', utc=True)

    # Now, convert all datetime objects (both originally timezone-naive and those just converted to UTC) to 'America/Chicago'
    df.loc[df['game_time'].notna(), 'game_time'] = df.loc[df['game_time'].notna(), 'game_time'].dt.tz_convert(
        'America/Chicago')

    # Finally, remove the timezone information to make all datetime objects timezone naive
    df['game_time'] = df['game_time'].dt.tz_localize(None)

    # Create 'DC_Game_ID' column
    df['DC_Game_ID'] = df['game_time'].dt.strftime('%m%d%Y') + '-' + df['away_team_clean'] + '-' + df[
        'home_team_clean']

    return df
