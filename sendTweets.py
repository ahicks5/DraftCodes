import tweepy
import pandas as pd
from datetime import datetime, timedelta
import pytz
import json
import random

try:
    ind_df = pd.read_csv('/var/www/html/Website/Indicator_Data.csv')
    track_path = '/var/www/html/Website/Tweet_Records.csv'
except:
    ind_df = pd.read_csv('Indicator_Data.csv')
    track_path = 'Tweet_Records.csv'


class sendTweet:
    def __init__(self):
        self.ind_df = ind_df
        self.api_key = "wpFqMU588wZBtwdzMZrrTENUJ"
        self.api_secret = "vJwkAcyOs6qIfXl4Slw7MnPT0WUOcwibr7E789e0tDGXHnZuhm"
        self.bearer_token = 'AAAAAAAAAAAAAAAAAAAAAAcPogEAAAAAGN7yDwXqR1IwYbfBlHVNFqqkekM%3Dpda3uqdLYMvLGbN1qRmJztqL3BbGSf9pDFDt2ePMcsdLBnHE5b'
        self.access_token = '1674537861410897920-JMqRCjayYGyDe3bx7g6YUsu81TztVY'
        self.access_token_secret = 'ntYMpa7hp33AMCIueO9tKl5IvEBlFeZDTWfPdgXZQu468'
        self.client_id = 'TFF1YXFBY2ZVU3pjTTdzN1VvV0s6MTpjaQ'
        self.client_secret = 'CNoFF_2gHUQB_ZgPKbkIaYSAtUNiL3zeSSO3SItpxldJiNmUeP'
        self.client = tweepy.Client(bearer_token=self.bearer_token,
                                    consumer_key=self.api_key,
                                    consumer_secret=self.api_secret,
                                    access_token=self.access_token,
                                    access_token_secret=self.access_token_secret)

    def find_tweets(self):
        track_df = pd.read_csv(track_path)
        track_df['gameID'] = track_df['gameID'].astype('int64')

        ind_df['game_startTime_cst'] = pd.to_datetime(ind_df['game_startTime_cst'])

        # Current time in CDT
        now = datetime.now(pytz.timezone('America/Chicago'))

        # Time 20 minutes from now
        time_threshold = now + timedelta(hours=1)

        filtered_df = ind_df[(ind_df['game_startTime_cst'] <= time_threshold) & (ind_df['game_startTime_cst'] > now)]
        filtered_df.to_csv("test_filter.csv", index=False)

        # only certain sports
        filtered_df = filtered_df[(filtered_df['Clean_Sport'] == 'Baseball') | (filtered_df['Clean_Sport'] == 'Basketball')]

        if len(filtered_df) == 0:
             print('No tweets sent, no games')
             return
        else:
            for index, row in filtered_df.iterrows():
                if row['game_id'] in track_df['gameID'].unique().tolist():
                    continue

                away_team = row['away_team_clean']
                home_team = row['home_team_clean']

                if away_team == 'nan' or home_team == 'nan':
                    continue

                tweet = self.generate_tweets(away_team=away_team, home_team=home_team)
                self.send_tweet(tweet)

                track_df = track_df.append({'gameID': row['game_id'], 'tweet_status': 'Complete'}, ignore_index=True)
                track_df.to_csv(track_path, index=False)

    def send_tweet(self, text):
        self.client.create_tweet(text=text)
        print('Tweet sent!')

    def generate_tweets(self, away_team, home_team):
        try:
            with open('/var/www/html/Website/tweet_bank.json', 'r', encoding='utf-8') as file:
                tweet_bank = json.load(file)
                tweet = random.choice(tweet_bank['tweet_templates']).format(away_team=away_team, home_team=home_team)
                return tweet
        except:
            with open('tweet_bank.json', 'r', encoding='utf-8') as file:
                tweet_bank = json.load(file)
                tweet = random.choice(tweet_bank['tweet_templates']).format(away_team=away_team, home_team=home_team)
                return tweet


if __name__ == '__main__':
    twt = sendTweet()
    twt.find_tweets()



