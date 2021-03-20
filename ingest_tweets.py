import json
import os
import pyodbc
import sqlalchemy
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


class IngestTweets:

    def transform(self, tweets):
        df_final = []

        for tweet in tweets:
            try:
                df_tratado = pd.DataFrame(
                    tweet).reset_index(drop=True).iloc[:1]

                df_tratado.drop(columns=['quote_count', 'reply_count', 'retweet_count',
                                'favorite_count', 'favorited', 'retweeted', 'user', 'entities'], inplace=True)

                df_tratado['user_id'] = tweet['user']['id']
                df_tratado['user_id_str'] = tweet['user']['id_str']
                df_tratado['user_screen_name'] = tweet['user']['screen_name']
                df_tratado['user_location'] = tweet['user']['location']
                df_tratado['user_description'] = tweet['user']['description']
                df_tratado['user_protected'] = tweet['user']['protected']
                df_tratado['user_verified'] = tweet['user']['verified']
                df_tratado['user_followers_count'] = tweet['user']['followers_count']
                df_tratado['user_friends_count'] = tweet['user']['friends_count']
                df_tratado['user_created_at'] = tweet['user']['created_at']

                user_mentions = []
                for mention in tweet['entities']['user_mentions']:
                    base = mention.copy()
                    mention.pop('indices', None)
                    user_mentions.append(
                        pd.DataFrame(mention, index=[0]).rename(columns={
                            'screen_name': 'entities_screen_name',
                            'name': 'entities_name',
                            'id': 'entities_id',
                            'id_str': 'entities_id_str'
                        })
                    )
                dfs = []
                for i in user_mentions:
                    dfs.append(
                        pd.concat([df_tratado.copy(), i], axis=1)
                    )
                df_final.append(
                    pd.concat(dfs, ignore_index=True) if len(
                        dfs) > 0 else df_tratado
                )

            except:
                df_final.append(None)

        return pd.concat([i for i in df_final if i is not None], ignore_index=True)

    def load(self):
        if os.path.exists("collected_tweets_latest.txt"):
            with open("collected_tweets_latest.txt", "r") as file:
                tweets = file.readlines()

            return [json.loads(json.loads(tweet)) for tweet in tweets]
        else:
            raise Exception("Latest file was not found at root directory")

    def ingest(self, data_frame: pd.DataFrame):
        engine = sqlalchemy.create_engine(
            f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@localhost/twitter?driver=ODBC+Driver+17+for+SQL+Server")
        return data_frame.to_sql("tweets", con=engine,
                                 index=False, if_exists="append")

    def clean(self):
        if os.path.exists("collected_tweets_latest.txt"):
            os.remove("collected_tweets_latest.txt")
        return True

    def run(self):
        data = self.load()
        data_transformed = self.transform(data)
        self.ingest(data_transformed)
        self.clean()


if __name__ == "__main__":
    IngestTweets().run()
