import boto3
import botocore
import logging
import os
import re
import requests
import tablib
from chalice import Chalice
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from chalicelib import common
from datetime import datetime, timezone


app = Chalice(app_name='football_predictions')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

s3 = boto3.resource('s3')

num_days = int(os.environ['FIXTURES_DAYS'])


@app.schedule('cron(0 18 * * ? *)')
def get_fixtures(event):

    # Set up the requests Session with retries
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 500, 502, 503, 504 ])
    s.mount('http://', HTTPAdapter(max_retries=retries))
    s.mount('https://', HTTPAdapter(max_retries=retries))

    # Retrieve the fixtures from the web api
    url = os.environ['FIXTURES_URL']
    LOGGER.info("Fetching fixtures from %s", url)
    r = s.get(url, verify=False, timeout=240)

    # Load the csv into a tablib Dataset
    data = tablib.Dataset()
    data.csv = r.text

    # Add column which represents the 'football-data' division
    data.lpush_col(_get_division, header='Div')
    del data['Country']
    del data['League']

    # Remove the rows for leagues we don't want (i.e. ones not in ODDSPORTAL_DIVISIONS)
    keepers = [i for i, row in enumerate(data) if row[0] is not None]
    subset = data.subset(rows=keepers)

    s3_bucket = os.environ['S3_BUCKET']
    s3_key = 'fixtures/fixtures.csv'
    LOGGER.info("Writing fixtures to %s/%s", s3_bucket, s3_key)
    s3.Object(s3_bucket, s3_key).put(Body=subset.export('csv'))

    LOGGER.info("Done.")


@app.schedule('rate(1 hour)')
def fetch_historical_data(event):

    base_url_main = 'https://www.football-data.co.uk/mmz4281/'
    base_url_new  = 'https://www.football-data.co.uk/new/'
    latest_season = os.environ['LATEST_SEASON']
    previous_seasons = os.environ['PREVIOUS_SEASONS']
    main_leagues = os.environ['MAIN_LEAGUES']
    new_leagues = os.environ['NEW_LEAGUES']
    s3_bucket = os.environ['S3_BUCKET']
    s3_prefix = os.environ['S3_PREFIX_HISTORICAL']

    n_updates = 0

    # Check each 'main' league for updates and overwrite if necessary
    for league in [l.strip() for l in main_leagues.split(',')]:
        n_updates += _update_historical_data(s3_bucket, s3_prefix, base_url_main, league,
                                             latest_season=latest_season,
                                             previous_seasons=previous_seasons)
    # Check each 'new' league for updates and overwrite if necessary
    for league in [l.strip() for l in new_leagues.split(',')]:
        n_updates += _update_historical_data(s3_bucket, s3_prefix,
                                             base_url_new, league)
    return {
        'statusCode': 200,
        'body': "Number of updates: {}".format(n_updates)
    }


@app.on_s3_event(bucket=os.environ['S3_BUCKET'], prefix='fixtures/',
                 suffix='.csv', events=['s3:ObjectCreated:*'])
def make_predictions(event):
    print("Looks like there's a new fixtures file: {}/{}".format(event.bucket, event.key))

    # TODO complete


def _update_historical_data(s3_bucket, s3_prefix, base_url, league, latest_season=None, previous_seasons=None):

        # Check last-modified time of our copy of the file
        s3object = _get_historical_data_from_s3(s3_bucket, s3_prefix, league)
        our_last_modified = _get_last_modified(s3object)

        # Download the data for the latest season
        url = base_url
        if latest_season:
            url = base_url + latest_season
        latest_data = _download_football_data(url, league)

        # Compare last-modified dates; if latest_data is not newer, then nothing to do
        # (Not sure if I'm doing the right thing here with UTC -- prob. should be BST)
        their_last_modified = datetime.strptime(
            latest_data.headers['Last-Modified'], "%a, %d %b %Y %H:%M:%S %Z").astimezone(timezone.utc)
        LOGGER.info("%s last modified: %s", latest_data.url, their_last_modified)

        if our_last_modified is not None and our_last_modified >= their_last_modified:
            LOGGER.info("No changes for %s.", league)
            return 0

        # Get the previous seasons' data (for 'main' leagues only), combine it with latest season's data, and write to S3
        data = ''
        if previous_seasons:
            for season in sorted([s.strip() for s in previous_seasons.split(',')]):
                url = base_url + season
                past_season = _download_football_data(url, league)
                data += past_season.text
        data += latest_data.text

        LOGGER.info("Writing data to %s/%s/%s.csv", s3_bucket, s3_prefix, league)
        s3object.put(Body=data)

        return 1


def _get_division(row):
    country_and_league = row[0], row[1]
    if country_and_league in common.ODDSPORTAL_DIVISIONS:
        return common.ODDSPORTAL_DIVISIONS[country_and_league]
    else:
        return None


def _get_historical_data_from_s3(bucket, prefix, league):
    key = prefix.rstrip('/') + '/' + league + '.csv'
    return s3.Object(bucket, key)


def _get_last_modified(s3object):
    last_modified = None
    try:
        s3object.load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ['403', '404']:
            # The object does not exist.
            LOGGER.info("%s/%s does not exist", s3object.bucket_name, s3object.key)
        else:
            # Something else has gone wrong.
            raise
    else:
        # The object does exist
        last_modified = s3object.last_modified

    LOGGER.info("%s/%s last modified: %s" , s3object.bucket_name, s3object.key, last_modified)
    return last_modified


def _download_football_data(base_url, league):
    # e.g https://www.football-data.co.uk/mmz4281/2021/E0.csv,
    # or https://www.football-data.co.uk/new/AUT.csv
    url = base_url.rstrip('/') + '/' + league + '.csv'
    response = requests.get(url)
    response.raise_for_status()
    if response.status_code != 200:
        raise Exception("Can't download from %s: status_code=%d, reason=%s" %
                        (url, response.status_code, response.reason))
    return response
