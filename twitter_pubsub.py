from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import twitter_credentials
import json
from google.cloud import pubsub_v1
from google.oauth2 import service_account

import argparse
from datetime import datetime
import logging
import random
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys
# from apache_beam.transforms.window import FixedWindows
from google.oauth2 import service_account
import os
import sys

key_path = 'tweetdeck-320105-9820690ac16e.json'

credentials = service_account.Credentials.from_service_account_file(
    key_path, 
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
)

client = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = client.topic_path('tweetdeck-320105', 'twitterdata')


class StdOutListener(StreamListener):
    def on_data(self,data):
        print(data)
        message = json.dumps(data)
        client.publish(topic_path, data=message.encode('utf-8'))
        return True
    
    def on_error(self, status):
        print(status)


if __name__ == "__main__":

    auth = OAuthHandler(twitter_credentials.API_KEY, twitter_credentials.API_SECRET_KEY)
    auth.set_access_token(twitter_credentials.ACCESS_TOKEN, twitter_credentials.ACCESS_TOKEN_SECRET)
    listener = StdOutListener()
    stream = Stream(auth, listener)
    stream.filter(track=['boaz'])

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

    pipeline_options = PipelineOptions(streaming=True)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline 
            | io.ReadFromPubSub(topic="projects/tweetdeck-320105/topics/twitterdata")
            | beam.Map(print))
        print("I reached after pipeline")
        result = pipeline.run()
        result.wait_until_finish()

        
