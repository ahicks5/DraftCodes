import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from connectSources import find_ref_dfs
import numpy as np
import pytz

team_ref_df, sport_ref_df, espn_df, bovada_df = find_ref_dfs()


class PullESPN:
    pregame_sport_links = {
        'soccer': 'https://www.espn.com/soccer/schedule/_/date/',
        #'nhl': 'https://www.espn.com/nhl/schedule/_/date/',
        #'nba': 'https://www.espn.com/nba/schedule/_/date/',
        'nfl': 'https://www.espn.com/nfl/schedule/_/date/',
        'mlb': 'https://www.espn.com/mlb/schedule/_/date/',
        'wnba': 'https://www.espn.com/wnba/schedule/_/date/',
        'cfb': 'https://www.espn.com/college-football/schedule/_/date'
    }

    def get_soup(self, url):
        """Fetch the page content and convert it into BeautifulSoup object"""
        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return None
        return soup

    def today_tom_date_url(self):
        # Get today's date
        today = datetime.now()

        # Format today's date as 'YYYYMMDD'
        today_str = today.strftime('%Y%m%d')

        # Get tomorrow's date
        tomorrow = today + timedelta(days=300)

        # Format tomorrow's date as 'YYYYMMDD'
        tomorrow_str = tomorrow.strftime('%Y%m%d')

        return today_str, tomorrow_str

    def extract_game_links(self, soup):
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

    def get_game_links(self):
        """Get all game links from the pregame sports links"""
        today_date, tom_date = self.today_tom_date_url()
        links = []
        for day in [today_date, tom_date]:
            for sport, espn_link in self.pregame_sport_links.items():
                soup = self.get_soup(espn_link + day)
                if soup:
                    links.extend(self.extract_game_links(soup))
        return links

    def get_game_time_date(self, soup):
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

    def get_teams_and_predictions(self, soup):
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

    def generate_game_date_ID(self, df):
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

    def parse_game_stats(self, url):
        """Parse game stats from the given URL"""
        soup = self.get_soup(url)
        gameID = int(url.split('gameId=')[1].strip())
        if 'match' in url:
            sport = url.split('espn.com/')[1].split('/match')[0].strip()
        elif 'game' in url:
            sport = url.split('espn.com/')[1].split('/game')[0].strip()
        else:
            sport = 'n/a'

        if soup:
            game_time = self.get_game_time_date(soup)
            team_1, team_2, team_1_prediction, team_2_prediction = self.get_teams_and_predictions(soup)

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

    def add_espn_ref_names(self, df):
        # merge away team
        team_ref_df_espn = team_ref_df[['ESPN Names', 'Final Names']]

        df = df.merge(team_ref_df_espn, left_on='team1', right_on='ESPN Names', how='left')
        df = df.rename(columns={'Final Names': 'away_team_clean'})
        df = df.drop(columns=['ESPN Names'])

        df = df.merge(team_ref_df_espn, left_on='team2', right_on='ESPN Names', how='left')
        df = df.rename(columns={'Final Names': 'home_team_clean'})
        df = df.drop(columns=['ESPN Names'])

        return df

    def show_missing_refs(self, df, team_ref_df):
        teams_matchups = pd.concat([df['team1'], df['team2']]).unique()
        teams_reference = team_ref_df['ESPN Names'].unique()
        missing_teams = [team for team in teams_matchups if team not in teams_reference]

        df_missing_teams = pd.DataFrame(missing_teams, columns=['team'])
        df_missing_teams.to_csv('espn_missing_teams.csv', index=False)

    def get_schedule_links(self, game_link):
        espn_start = 'https://www.espn.com'
        soup = self.get_soup(game_link)
        team_names = soup.findAll('h2',
                                  {'class': 'ScoreCell__TeamName ScoreCell__TeamName--displayName truncate db'})
        away_schedule_link = espn_start + team_names[0].parent['href'].replace('team/', 'team/schedule/')
        home_schedule_link = espn_start + team_names[1].parent['href'].replace('team/', 'team/schedule/')

        return away_schedule_link, home_schedule_link

    def parse_schedule_stats(self, schedule_link):
        soup = self.get_soup(schedule_link)

        tables = pd.read_html(soup.prettify(), header=0)
        df = tables[0]
        if 'DATE' in df.columns.tolist() or 'DATE' in df.columns.tolist():
            pass
        else:
            tables = pd.read_html(soup.prettify(), header=1)
            df = tables[0]

        # clean rows - delete postponed and other header
        df = df[df['RESULT'] != 'Postponed']
        df = df[df['RESULT'] != 'TIME']
        df = df[df['RESULT'] != 'LIVE']
        mask = ~df['RESULT'].str.contains('AM|PM|Season|Preseason|RESULT')
        df = df[mask]

        # convert column to date
        current_year = pd.Timestamp.now().year
        df['DATE'] = df['DATE'] + f', {current_year}'
        df['DATE'] = pd.to_datetime(df['DATE'], format='%a, %b %d, %Y')

        # add new columns
        df['Clean_Result'] = df.apply(self.determine_result, axis=1)
        df['Away_Home'] = df.apply(self.determine_away_home, axis=1)
        df['Final_Score'] = df['RESULT'].str.split(' F/').str.get(0).str.strip()
        df['Final_Score'] = df['Final_Score'].str.strip().str.split(' OT').str.get(0)
        df[['Points_For', 'Points_Against']] = df.apply(self.extract_points, axis=1)

        if (len(df[df['Away_Home'] == 'Home']) > 2) and (len(df[df['Away_Home'] == 'Away']) > 2):
            # generate streaks
            current_streak, home_streak, away_streak = self.calculate_streaks(df)
            avg_pt_for = df['Points_For'].mean()
            avg_pt_ag = df['Points_Against'].mean()

            schedule_dict = {
                'espn_current_streak': current_streak,
                'espn_home_streak': home_streak,
                'espn_away_streak': away_streak,
                'espn_avg_pt_for': avg_pt_for,
                'espn_avg_pt_ag': avg_pt_ag
            }
        else:
            schedule_dict = {}

        return schedule_dict

    @staticmethod
    def determine_result(row):
        if 'W' in row['RESULT']:
            return 'W'
        elif 'L' in row['RESULT']:
            return 'L'
        else:
            return ''

    @staticmethod
    def determine_away_home(row):
        if '@' in row['OPPONENT']:
            return 'Away'
        elif 'vs' in row['OPPONENT']:
            return 'Home'
        else:
            return ''

    @staticmethod
    def extract_points(row):
        win_loss, scores = row['Final_Score'].split()
        points_for, points_against = map(int, scores.split('-'))

        if win_loss == 'L':
            points_for, points_against = points_against, points_for

        return pd.Series({'Points For': points_for, 'Points Against': points_against})

    @staticmethod
    def calculate_streaks(df):
        # Initialize the streak counts
        streak_count = 1
        home_streak_count = 1
        away_streak_count = 1

        # Initialize the previous results
        previous_result = df['Clean_Result'].iloc[0]
        previous_home_result = None
        previous_away_result = None

        # Iterate through the DataFrame
        for index, row in df.iterrows():
            # Current streak
            current_result = row['Clean_Result']
            if current_result == previous_result:
                streak_count += 1
            else:
                streak_count = 1
                previous_result = current_result

            # Home streak
            if row['Away_Home'] == 'Home':
                if previous_home_result is None:
                    previous_home_result = current_result
                elif current_result == previous_home_result:
                    home_streak_count += 1
                else:
                    home_streak_count = 1
                    previous_home_result = current_result

            # Away streak
            if row['Away_Home'] == 'Away':
                if previous_away_result is None:
                    previous_away_result = current_result
                elif current_result == previous_away_result:
                    away_streak_count += 1
                else:
                    away_streak_count = 1
                    previous_away_result = current_result

        # Final streaks
        current_streak = f'{previous_result}{streak_count}'
        home_streak = f'{previous_home_result}{home_streak_count}' if previous_home_result is not None else 'N/A'
        away_streak = f'{previous_away_result}{away_streak_count}' if previous_away_result is not None else 'N/A'

        return current_streak, home_streak, away_streak

    def get_schedule_info(self, link):
        try:
            away_link, home_link = self.get_schedule_links(link)
            away_dict, home_dict = self.parse_schedule_stats(away_link), self.parse_schedule_stats(home_link)

            new_away_dict = new_home_dict = {}

            for key in away_dict:
                new_away_dict['A_' + key] = away_dict[key]

            for key in home_dict:
                new_home_dict['H_' + key] = home_dict[key]

            return new_away_dict, new_home_dict
        except Exception as e:
            return {}, {}

    # FINAL PROCESS
    def assemble_espn_data(self):
        cur_df = espn_df
        links = self.get_game_links()

        # assemble game and predictions
        game_list = []

        # Define the timezone for CDT
        cdt = pytz.timezone('America/Chicago')

        for link in links:
            gameId = int(link.split('gameId=')[1].strip())

            if gameId in cur_df['gameID'].unique().tolist():
                row = cur_df[cur_df['gameID'] == gameId]
                team1_pred = row['team1_pred'].iloc[0]

                game_time = pd.to_datetime(row['game_time'].iloc[0])

                # Convert game_time to CDT if it's timezone-aware, otherwise localize it to CDT
                if game_time.tzinfo is not None:
                    game_time = game_time.astimezone(cdt)
                else:
                    game_time = cdt.localize(game_time)

                lastMod = pd.to_datetime(row['lastMod_ESPNpred'].iloc[0])

                # Get current time as timezone-aware in CDT
                current_time = datetime.now(cdt)

                # Create one_week_ahead as timezone-aware in CDT
                one_week_ahead = current_time + timedelta(weeks=2)

                # Convert lastMod to CDT if it's timezone-aware, otherwise localize it to CDT
                if lastMod.tzinfo is not None:
                    lastMod = lastMod.astimezone(cdt)
                else:
                    lastMod = cdt.localize(lastMod)

                # Create next_refresh as timezone-aware in CDT
                next_refresh = lastMod + timedelta(hours=2)

                if pd.isna(team1_pred):
                    continue
                elif (game_time > one_week_ahead) and not pd.isna(team1_pred):
                    continue
                elif current_time < next_refresh:
                    continue

            game_dict = self.parse_game_stats(link)
            game_dict['lastMod_ESPNpred'] = datetime.now(cdt).strftime("%m/%d/%Y %I:%M:%S %p")
            game_list.append(game_dict)

        pred_df = pd.DataFrame(game_list)
        pred_df = pred_df.drop_duplicates()

        # assemble schedule data

        game_list = []
        for link in links:
            gameId = int(link.split('gameId=')[1].strip())
            if gameId in cur_df['gameID'].unique().tolist():
                row = cur_df[cur_df['gameID'] == gameId]
                lastMod = pd.to_datetime(row['lastMod_ESPNsch'].iloc[0])
                current_time = datetime.now()
                if (lastMod + timedelta(hours=12)) < current_time:
                    a_sch_dict, h_sch_dict = self.get_schedule_info(link)
                    full_game_dict = {**a_sch_dict, **h_sch_dict}
                    full_game_dict['gameID'] = gameId
                    full_game_dict['lastMod_ESPNsch'] = datetime.now().strftime("%m/%d/%Y %I:%M:%S %p")
                    game_list.append(full_game_dict)
                else:
                    continue
            else:
                a_sch_dict, h_sch_dict = self.get_schedule_info(link)
                full_game_dict = {**a_sch_dict, **h_sch_dict}
                full_game_dict['gameID'] = gameId
                full_game_dict['lastMod_ESPNsch'] = datetime.now().strftime("%m/%d/%Y %I:%M:%S %p")
                game_list.append(full_game_dict)

        sch_df = pd.DataFrame(game_list)
        sch_df = sch_df.drop_duplicates()

        # combine pred and sch
        if len(pred_df) == 0:
            return cur_df
        elif len(sch_df) == 0 and len(pred_df) > 0:
            df = pred_df
        else:
            df = pd.merge(pred_df, sch_df, on='gameID', how='left')

        df = df.drop_duplicates(subset='gameID')

        # add names, add id, check missing
        df = self.add_espn_ref_names(df)
        df = self.generate_game_date_ID(df)
        self.show_missing_refs(df, team_ref_df)

        # combine old and new
        merged_df = cur_df.merge(df, on='gameID', suffixes=('', '_new'), how='outer')

        # Update values from df2 into the merged dataframe
        for column in df.columns:
            if column != 'gameID':
                original_column = column
                new_column = f'{column}_new'
                # Update the values with non-NA data from df2
                mask = merged_df[new_column].notna()
                merged_df.loc[mask, original_column] = merged_df.loc[mask, new_column]

        # Keep only the columns that were originally in df1
        final_df = merged_df[cur_df.columns]
        final_df = final_df[final_df['gameID'] != '']

        try:
            final_df.to_csv('/var/www/html/Website/ESPN_Data.csv', index=False)
        except:
            final_df.to_csv('ESPN_Data.csv', index=False)

        return final_df


if __name__ == '__main__':
    espn = PullESPN()
    espn.assemble_espn_data()