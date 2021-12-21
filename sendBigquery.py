from datetime import datetime
import sys
import os
from google.cloud import bigquery as google_bigquery
from google.oauth2 import service_account

sample_json = {'id': '1417537550445518848', 'created_at': '2020-08', 'text': '@niki_asmr 흐아악 그럼용 ~! 거의 반수면 상태였는데 잘 살아돌아왔습니당히히 밤인데두 덥네용 더위 조심하셔야해요!', 'retweet_count': 0, 'favorite_count': 0, 'author_name': '린나 뒤 석', 'author_screen_name': 'exit_pagee', 'profile_image_url': 'http://pbs.twimg.com/profile_images/1405405533494935556/CXztEdRr_normal.jpg', 'lat': None, 'long': None, 'keyword_list': '폭염_더위', 'insertId': '1417537550445518848'}
key_path = 'tweetdeck-320105-9820690ac16e.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

credentials = service_account.Credentials.from_service_account_file(
    key_path, 
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
)
def sendQuery(element):
    element = [element]
    client = google_bigquery.Client(project='tweetdeck-320105')
    table = client.get_table('tweetdeck-320105.tweetdeck.twitter_data_gcp')

    errors = client.insert_rows_json(table, element, row_ids='id')
    if errors == []:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

sendQuery(sample_json)