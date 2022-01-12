import requests
import os
import json
import pandas as pd
import numpy as np
import csv
import datetime
import dateutil.parser
import unicodedata
import time
import random
from dotenv import load_dotenv, find_dotenv


def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))


def auth():
    return os.environ.get("BEARER_TOKEN")


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def create_url(keyword, start_date, end_date, max_results=10):
    search_url = "https://api.twitter.com/2/tweets/search/all"  # Change to the endpoint you want to collect data from

    # change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)


def connect_to_endpoint(url, headers, params, next_token=None):
    params['next_token'] = next_token  # params object received from create_url function
    response = requests.request("GET", url, headers=headers, params=params)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def append_news_to_csv(json_response, fileName):

    #A counter variable
    counter = 0

    #Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    #Loop through each tweet
    for tweet in json_response['data']:
        
        # We will create a variable for each since some of the keys might not exist for some tweets
        # So we will account for that

        # 1. Author ID
        author_id = tweet['author_id']

        # 2. Time created
        created_at = dateutil.parser.parse(tweet['created_at'])

        # 3. Geolocation
        if ('geo' in tweet):   
            geo = tweet['geo']['place_id']
        else:
            geo = " "

        # 4. Tweet and Conversation ID
        tweet_id = tweet['id']
        conversation_id = tweet['conversation_id']

        # 5. Language
        lang = tweet['lang']

        # 6. Tweet metrics
        retweet_count = tweet['public_metrics']['retweet_count']
        reply_count = tweet['public_metrics']['reply_count']
        like_count = tweet['public_metrics']['like_count']
        quote_count = tweet['public_metrics']['quote_count']

        # 7. source
        source = tweet['source']

        # 8. Tweet text
        text = tweet['text']
        
        # Assemble all data in a list
        res = [tweet_id, conversation_id, author_id, created_at, geo, lang, like_count, quote_count, reply_count, retweet_count, source, text]
        
        # Append the result to the CSV file
        csvWriter.writerow(res)
        counter += 1

    # When done, close the CSV file
    csvFile.close()


def append_tweet_to_csv(json_response, news_account_id, fileName):
    # A counter variable
    counter = 0

    # Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    # Loop through each tweet
    for tweet in json_response['data']:

        # We will create a variable for each since some of the keys might not exist for some tweets
        # So we will account for that

        # 1. Author ID
        author_id = tweet['author_id']

        # 2. Time created
        created_at = dateutil.parser.parse(tweet['created_at'])

        # 3. Geolocation
        if ('geo' in tweet):   
            geo = tweet['geo']['place_id']
        else:
            geo = " "

        # 4. Tweet and Conversation ID
        is_quote = tweet['referenced_tweets'][0]['type'] == 'quoted' if 'referenced_tweets' in tweet.keys() else False
        tweet_id = tweet['id']
        conversation_id = tweet['conversation_id'] if not is_quote else tweet['referenced_tweets'][0]['id']
        in_reply_to_user_id = tweet['in_reply_to_user_id'] if 'in_reply_to_user_id' in tweet.keys() else news_account_id        

        # 5. Language
        lang = tweet['lang']

        # 6. Tweet metrics
        retweet_count = tweet['public_metrics']['retweet_count']
        reply_count = tweet['public_metrics']['reply_count']
        like_count = tweet['public_metrics']['like_count']
        quote_count = tweet['public_metrics']['quote_count']

        # 7. source
        source = tweet['source']

        # 8. Tweet text
        text = tweet['text']

        # Assemble all data in a list
        res = [tweet_id, conversation_id, author_id, in_reply_to_user_id, is_quote, created_at, geo, lang, like_count, quote_count, reply_count, retweet_count, source, text]

        # Append the result to the CSV file
        csvWriter.writerow(res)
        counter += 1

    # When done, close the CSV file
    csvFile.close()


def append_user_to_csv(json_response, fileName):
    # A counter variable
    counter = 0

    # Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    # Loop through each tweet
    for user in json_response['includes']['users']:
        # 1. Author ID, Username and DisplayName
        author_id = user['id']
        username = user['username']
        display_name = user['name']

        # 2. Description
        description = user['description']

        # 3. Verified
        verified = user['verified']

        # 4. Time user created
        created_at = dateutil.parser.parse(user['created_at'])

        # 5. User metrics
        followers_count = user['public_metrics']['followers_count']
        following_count = user['public_metrics']['following_count']
        tweet_count = user['public_metrics']['tweet_count']
        listed_count = user['public_metrics']['listed_count']

        # Assemble all data in a list
        res = [author_id, username, display_name, description, verified, created_at, followers_count, following_count,
               tweet_count, listed_count]

        # Append the result to the CSV file
        csvWriter.writerow(res)
        counter += 1

    # When done, close the CSV file
    csvFile.close()


def append_place_to_csv(json_response, fileName):
    # A counter variable
    counter = 0

    # Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    # Loop through each tweet
    for place in json_response['includes']['places']:
        # Features
        geo = place['id']
        full_name = place['full_name']
        place_type = place['place_type']
        name = place['name']
        country_code = place['country_code']

        # Assemble all data in a list
        res = [geo, full_name, place_type, name, country_code]

        # Append the result to the CSV file
        csvWriter.writerow(res)
        counter += 1

    # When done, close the CSV file
    csvFile.close()