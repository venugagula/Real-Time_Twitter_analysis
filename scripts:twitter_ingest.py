# scripts/twitter_ingest.py
import json
from kafka import KafkaProducer
import tweepy
from scripts.config import TWITTER_BEARER_TOKEN, KAFKA_TOPIC, KAFKA_BROKER

class TweetStreamer(tweepy.StreamingClient):
    def __init__(self, bearer_token):
        super().__init__(bearer_token)
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    def on_tweet(self, tweet):
        if tweet.lang == "en":
            self.producer.send(KAFKA_TOPIC, {
                'id': tweet.id,
                'text': tweet.text,
                'created_at': str(tweet.created_at)
            })

if __name__ == "__main__":
    stream = TweetStreamer(TWITTER_BEARER_TOKEN)
    stream.add_rules(tweepy.StreamRule("python OR data"))
    stream.filter(tweet_fields=["created_at", "lang"])
