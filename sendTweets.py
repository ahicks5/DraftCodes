import tweepy
import pandas as pd
from datetime import datetime, timedelta
import pytz

try:
    ind_df = pd.read_csv('/var/www/html/Website/Indicator_Data.csv')
except:
    ind_df = pd.read_csv('Indicator_Data.csv')


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
        ind_df['game_startTime_cst'] = pd.to_datetime(ind_df['game_startTime_cst'])

        # Current time in CDT
        now = datetime.now(pytz.timezone('America/Chicago'))

        # Time 20 minutes from now
        time_threshold = now + timedelta(minutes=60)

        filtered_df = ind_df[(ind_df['game_startTime_cst'] > now) & (ind_df['game_startTime_cst'] <= time_threshold)]

        if len(filtered_df) == 0:
            print('No tweets sent, no games')
            return
        else:
            for index, row in filtered_df.iterrows():
                away_team = row['away_team_clean']
                home_team = row['home_team_clean']
                tweet = (
                    f"""🔥 Upcoming Clash! 🚨\n 
                    🔵{away_team} vs {home_team}🔴\n
                    Who's taking home the glory? 🏆\n
                    Level up your game at DraftCodes.io! 📈🎲\n
                    #MakeYourPicks #DraftCodesAdvantage #SportsBetting 🚀"""
                )
                self.send_tweet(tweet)
                print('Tweet sent!')

    def send_tweet(self, text):
        self.client.create_tweet(text=text)
        print('tweet sent!')


if __name__ == '__main__':
    twt = sendTweet()
    twt.find_tweets()



