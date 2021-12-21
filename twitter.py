from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import credentials
from pykafka import KafkaClient
import json
import sys

def get_kafka_client():
    return KafkaClient(hosts='127.0.0.1:9092')

class StdOutListener(StreamListener):
    def on_data(self,data):
        print(data)
        message = json.loads(data)
        if message['place'] is not None:
            profile_image_url = message.owner.profile_image_url

            client = get_kafka_client()
            topic = client.topics['twitterdata']
            producer = topic.get_sync_producer()
            producer.produce(data.encode('ascii'))

        print(result_json)
        sys.exit(0)
        return True
    
    def on_error(self, status):
        print(status)


if __name__ == "__main__":

    auth = OAuthHandler(credentials.API_KEY, credentials.API_SECRET_KEY)
    auth.set_access_token(credentials.ACCESS_TOKEN, credentials.ACCESS_TOKEN_SECRET)
    listener = StdOutListener()
    stream = Stream(auth, listener)
    stream.filter(track=['boaz'])
