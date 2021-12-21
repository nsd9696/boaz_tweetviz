import json
import numpy as np
import sys
from datetime import datetime

sample_json = {'created_at': 'Tue Jul 20 15:18:29 +0000 2021', 'id': 1417504191233155074, 'id_str': '1417504191233155074', 'text': 'boaz 0721_4', 'source': '<a href="https://mobile.twitter.com" rel="nofollow">Twitter Web App</a>', 'truncated': False, 'in_reply_to_status_id': None, 'in_reply_to_status_id_str': None, 'in_reply_to_user_id': None, 'in_reply_to_user_id_str': None, 'in_reply_to_screen_name': None, 'user': {'id': 1415286886755491844, 'id_str': '1415286886755491844', 'name': '남상대', 'screen_name': 'vvBkdv2cn3hhOoP', 'location': None, 'url': None, 'description': None, 'translator_type': 'none', 'protected': False, 'verified': False, 'followers_count': 0, 'friends_count': 0, 'listed_count': 0, 'favourites_count': 0, 'statuses_count': 19, 'created_at': 'Wed Jul 14 12:27:49 +0000 2021', 'utc_offset': None, 'time_zone': None, 'geo_enabled': False, 'lang': None, 'contributors_enabled': False, 'is_translator': False, 'profile_background_color': 'F5F8FA', 'profile_background_image_url': '', 'profile_background_image_url_https': '', 'profile_background_tile': False, 'profile_link_color': '1DA1F2', 'profile_sidebar_border_color': 'C0DEED', 'profile_sidebar_fill_color': 'DDEEF6', 'profile_text_color': '333333', 'profile_use_background_image': True, 'profile_image_url': 'http://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png', 'profile_image_url_https': 'https://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png', 'default_profile': True, 'default_profile_image': False, 'following': None, 'follow_request_sent': None, 'notifications': None, 'withheld_in_countries': []}, 'geo': None, 'coordinates': None, 'place': None, 'contributors': None, 'is_quote_status': False, 'quote_count': 0, 'reply_count': 0, 'retweet_count': 0, 'favorite_count': 0, 'entities': {'hashtags': [], 'urls': [], 'user_mentions': [], 'symbols': []}, 'favorited': False, 'retweeted': False, 'filter_level': 'low', 'lang': 'pt', 'timestamp_ms': '1626794309801', 'keyword_list': ['boaz','bigdata']}

def check_origin(json_data):
    status = None
    if "retweeted_status" in list(json_data.keys()):
        json_data = json_data['retweeted_status']
        status = 'retweeted'
    elif "quoted_status" in list(json_data.keys()):
        json_data = json_data['quoted_status']
        status = 'quoted'
    else:
        status = 'origin'
        pass
    
    return json_data,status

def get_json_result(json_data):
    json_data,status = check_origin(json_data)
    temp_json = {'id': None, 'created_at': None, 'text': None, 'retweet_count': None, 
    'favorite_count': None, 'author_name': None, 'author_screen_name': None, 'profile_image_url': None,
    'lat': None, 'long': None, 'keyword_list': None}

    temp_json['id'] = json_data['id_str']
    try:
        temp_json['lat'] = json_data['place']['bounding_box']['coordinates'][0][0][1]
    except:
        temp_json['lat'] = np.nan
    try:
        temp_json['long'] = json_data['place']['bounding_box']['coordinates'][0][0][0]
    except:
        temp_json['long'] = np.nan

    temp_json['created_at'] = datetime.strptime(sample_json['created_at'],'%a %b %d %H:%M:%S +0000 %Y')
    if status != 'origin':
        try:
            temp_json['text'] = json_data['extended_tweet']['full_text']
        except:
            temp_json['text'] = json_data['text']
    else:
        temp_json['text'] = json_data['text']
    temp_json['retweet_count'] = json_data['retweet_count']
    temp_json['favorite_count'] = json_data['favorite_count']
    temp_json['author_name'] = json_data['user']['name']
    temp_json['author_screen_name'] = json_data['user']['screen_name']
    temp_json['profile_image_url'] = json_data['user']['profile_image_url']
    keyword_list = "_".join(json_data['keyword_list'])
    temp_json['keyword_list'] = keyword_list
    return temp_json

json_result = get_json_result(sample_json)

print(json_result)


