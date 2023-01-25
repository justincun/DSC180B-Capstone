import os
import pulsar
import time
import random
import string
import logging
import sys
import tweepy
import pandas as pd 

api_key = "3EepSTdImZMlek3mXWKdWqa1D"
api_secret = "uiGIANeZQ26dj1MT6APw17jt4RHrYvyGU03SswuMMXwrFNnIc9"
bearer_token = r"AAAAAAAAAAAAAAAAAAAAACWXjQEAAAAAy17NsJPlF2vZUNVNulgFWy17ygo%3DUYMZFoWabm48LT5vd7tK2pLeJQJm3DUPeLz7rhbn3jnfHhnPkt"
access_token = "1277453824907595777-FHcYQNvOJsEaVxhiGFVnHCbvShudJ6"
access_token_secret = "Xff7NVpRw4mCQint8VwLGdfwSdrdDVU4b8QVZ2y7VLSQd"

client = tweepy.Client(bearer_token, api_key, api_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_token, access_token_secret)
api = tweepy.API(auth)

class Producer(object):
    """
    Create a pulsar producer that writes tweets to a topic
    """
    def __init__(self):
        self.token = os.getenv("ASTRA_STREAMING_TOKEN")
        self.service_url = os.getenv("ASTRA_STREAMING_URL")
        self.topic = os.getenv("ASTRA_TOPIC")
        self.client = pulsar.Client(self.service_url, authentication=pulsar.AuthenticationToken(self.token))
        self.producer = self.client.create_producer(self.topic)

    def produce_messages(self):
        """
        Create and send random messages
        """
        while True:

            class MyStream(tweepy.StreamingClient):
            
                tweets = []
                limit = 1000
                
                def on_connect(self):
                    print("Connected")
        
                def on_tweet(self, tweet):
                    self.tweets.append(tweet)

                    if (len(self.tweets)) == self.limit:
                        self.disconnect()
            
            stream = MyStream(bearer_token=bearer_token)
            
            ## Remove and view filters 
            #filter_id = []
            #rules = stream.get_rules()[0]
            #for i in rules:
            #    filter_id.append(i.id)
            #stream.delete_rules(filter_id)
            
            print("Tweet Filters: ", stream.get_rules())

            filters = ["remote work" , "working remotely", 
            "#remotework", "#wfh", "-apply", "-hiring"]


            for term in filters:
                stream.add_rules(tweepy.StreamRule(term))
            
            
            stream.filter(tweet_fields=["referenced_tweets"])
            columns = ['Tweet']
            data = []
            
        
            for tweet in stream.tweets:
                try:
                    data.append([tweet.text])
                except AttributeError:  
                    data.append([tweet.full_text])

            df = pd.DataFrame(data, columns=columns)


            for i in range(df.shape[0]):
                self.producer.send(df['Tweet'][i].encode('utf-8'))
                logging.info("Tweet Found! {} \n".format(df['Tweet'][i]))
        


def produce_messages():
    """
    Create an instance of the producer and fire it up to send messages until the program is terminated
    """
    producer = Producer()
    producer.produce_messages()
    producer.client.close()


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout,
                        level=logging.INFO)
    produce_messages()
