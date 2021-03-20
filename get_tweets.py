import json
import os
from tweepy import OAuthHandler, Stream, StreamListener
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# Cadastrar as chaves de acesso
API_KEY = os.getenv("API_KEY")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")

# Definir um arquivo de saida para armazenar os tweets coletados
timestamp = datetime.now().timestamp()
out = open(f"collected_tweets_{timestamp}.txt", "w")
out_latest = open("collected_tweets_latest.txt", "w")

# Classe de conexão com o Twitter


class TweetListener(StreamListener):

    def on_data(self, data):
        itemString = json.dumps(data)
        out.write(itemString + "\n")
        out_latest.write(itemString + "\n")
        return True

    def on_error(self, status):
        print(status)


if __name__ == "__main__":
    listener = TweetListener()
    auth = OAuthHandler(API_KEY, API_SECRET_KEY)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    stream = Stream(auth, listener)

    stream.filter(track=["Trump", "Bolsonaro", "Putin"])
