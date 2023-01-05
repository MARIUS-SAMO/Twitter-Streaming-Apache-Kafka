import tweepy
import json
from kafka import KafkaProducer
import time

token = "AAAAAAAAAAAAAAAAAAAAAAJXkAEAAAAAROMShkLOg6pOGwe%2B5I2rOWgS%2BV0%3DY1Zi9QUSnUb5WoX5i5dEWPOBqCQGprbH5VxOTsNCQqzV8oaykl"
client = tweepy.Client(bearer_token=token)
producer = KafkaProducer(bootstrap_servers="localhost:9092")
topic_name = "rawTwitter"
query = "covid -is:retweet"


while True:
    for tweet in tweepy.Paginator(
        client.search_recent_tweets,
        query=query,
        tweet_fields=["lang", "possibly_sensitive", "created_at"],
        max_results=100,
    ).flatten(10):
        print(tweet)
        producer.send(
            topic_name,
            json.dumps(dict(tweet), indent=4, default=str, sort_keys=True).encode(
                "utf-8"
            ),
        )

    time.sleep(60)
