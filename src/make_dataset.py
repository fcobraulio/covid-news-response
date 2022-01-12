import os
import time
import random
import datetime
import argparse
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from consts.consts import Constants as CONSTS
from collect.collect import (
    auth, split, create_headers, create_url, connect_to_endpoint,
    append_tweet_to_csv, append_user_to_csv, append_place_to_csv, append_news_to_csv
)


def get_args():
    parser = argparse.ArgumentParser(
        description='Code for the data collection.'
    )

    parser.add_argument(
        '-i',
        '--input_data',
        type=str,
        required=True,
        help='Path to the input csv data.'
    )
    parser.add_argument(
        '-t',
        '--data_type',
        type=str,
        required=True,
        # choices=['news', 'replies'],
        help='Type of data. Should be news or replies.'
    )
    parser.add_argument(
        '-sd',
        '--start_date',
        type=str,
        required=True,
        # nargs='?',
        # const='2020-01-01',
        help='Start date for collection.'
    )
    parser.add_argument(
        '-ed',
        '--end_date',
        type=str,
        required=True,
        help='End date for collection.'
    )

    return parser.parse_args()


def main():

    # get input arguments
    global pool, search
    args = get_args()

    #
    INPUT_DATA_FILE = args.input_data
    COLLECT_DATA_TYPE = args.data_type
    START_DATE = args.start_date + f'T00:00:00.000Z'
    END_DATE = args.end_date + f'T23:59:59.000Z'
    BEARER_TOKEN = auth()
    HEADERS = create_headers(BEARER_TOKEN)
    MAX_COUNT = 10000000
    MAX_RESULTS = 500

    #
    logger = logging.getLogger(__name__)
    logger.info(fr'Start collecting {COLLECT_DATA_TYPE} tweets.')

    # read relevant datasets
    news_tweets = pd.read_csv(os.path.join(INPUT_DATA_FILE, f'news_tweets.csv'))
    news_accounts = pd.read_csv(os.path.join(INPUT_DATA_FILE, f'covid_users.csv'))
    conversations = pd.read_csv(os.path.join(INPUT_DATA_FILE, f'tweets_replies.csv'), 
                                usecols=['conversation_id']).conversation_id.unique()
    logger.info('Finished reading relevant data.')

    # keep only the relevant news accounts
    tmp = news_tweets.author_id.value_counts().rename('amount').rename_axis('author_id').reset_index()
    news_accounts = news_accounts[news_accounts.author_id.isin(tmp.author_id)].drop_duplicates('author_id').merge(
        tmp).sort_values('amount', ascending=False)

    # get selected conversations
    if COLLECT_DATA_TYPE == 'replies':
        conversations = list(
            news_tweets[
                (news_tweets.author_id.isin(
                    news_accounts[news_accounts.username.isin(CONSTS.priority_news)].author_id.values)) &
                ~(news_tweets.conversation_id.isin(conversations)) &
                (news_tweets.reply_count > 0)
                ].conversation_id)
        random.shuffle(conversations)
        pool = list(split(conversations, int(len(conversations) / 24)))
    elif COLLECT_DATA_TYPE == 'news':
        pool = news_accounts.username.unique()

    # loop variables
    total_pool_tweets = n_requests = n_instances = err_count = result_count = count = 0
    start_time = time.time()
    valid = False
    next_token = None

    # loop through pool
    for instance in pool:

        # select search query
        if COLLECT_DATA_TYPE == 'replies':
            search = "conversation_id:" + " OR conversation_id:".join(
                [str(s) for s in instance]) + " lang:en is:reply -is:retweet"
        elif COLLECT_DATA_TYPE == 'news':
            search = f"context:123.1220701888179359745 from:{instance} lang:en -is:retweet -is:reply"

        #
        total_instance_tweets = 0
        flag = True

        # collect tweets within an instance
        while flag:

            # check if max_count reached
            if count >= MAX_COUNT:
                break

            # check for API response error
            while not valid:
                try:
                    url = create_url(search, START_DATE, END_DATE, MAX_RESULTS)
                    json_response = connect_to_endpoint(url[0], HEADERS, url[1], next_token)
                    result_count = json_response['meta']['result_count']
                    n_requests += 1
                    valid = True
                    err_count = 0
                except Exception as e:
                    err_count += 1
                    time.sleep(2 ^ err_count)
                    logger.info(fr"---- Request error #{err_count}: {e}")
            valid = False

            # check if it is the final request for this instance
            next_token = json_response['meta']['next_token'] if 'next_token' in json_response['meta'] else None

            # collect tweets
            if result_count is not None and result_count > 0:
                if COLLECT_DATA_TYPE == 'replies':
                    append_tweet_to_csv(json_response, os.path.join(INPUT_DATA_FILE, f'tweets_replies.csv'))
                    append_user_to_csv(json_response, os.path.join(INPUT_DATA_FILE, f'users_replies.csv'))
                    if 'places' in json_response['includes'].keys():
                        append_place_to_csv(json_response, os.path.join(INPUT_DATA_FILE, f'places_replies.csv'))
                if COLLECT_DATA_TYPE == 'news':
                    append_news_to_csv(json_response, os.path.join(INPUT_DATA_FILE, f'news_tweets.csv'))
                count += result_count
                total_instance_tweets += result_count
                logger.info(
                    fr'Just collect {result_count} tweets | Instance tweets: {total_instance_tweets}' +
                    fr'| Pool tweets: {total_pool_tweets} | Request #{n_requests} | Time cap: {int(time.time() - start_time)}.'
                )
                time.sleep(1)

            # if it is the last request, set flags
            if next_token is None:
                flag = False

            # control API time caps
            t = time.time() - start_time
            if n_requests == 300:
                if t < 900:
                    time.sleep(900 - t)
                start_time = time.time()
                n_requests = 0

        n_instances += 1
        total_pool_tweets += total_instance_tweets


if __name__ == '__main__':

    # get parent dir
    project_dir = Path(__file__).resolve().parents[1]

    # logging settings
    logsdir = os.path.join(project_dir,'src/logs/')
    logfile = os.path.join(logsdir, datetime.datetime.now().strftime("%Y%m%d%H%M%S") + '_make_dataset.log')
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=logfile, level=logging.INFO, format=log_fmt, filemode='w')

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
