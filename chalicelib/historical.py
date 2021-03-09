import boto3
import botocore
import logging
import os
import requests
from chalice import Blueprint
from datetime import datetime, timezone
import tablib


history = Blueprint(__name__)

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

s3 = boto3.resource('s3')


@history.schedule('cron(0 1,13 * * ? *)')
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
        data = tablib.Dataset()
        data.headers = ['Div', 'Season', 'Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR', 'PSH', 'PSD', 'PSA', 'AvgH', 'AvgD', 'AvgA']
        if previous_seasons:
            for season in sorted([s.strip() for s in previous_seasons.split(',')]):
                url = base_url + season
                past_season_data = _download_football_data(url, league)
                [data.append(row) for row in _extract_data(past_season_data, league, season)]

        [data.append(row) for row in _extract_data(latest_data, league, latest_season)]

        LOGGER.info("Writing data to %s/%s/%s.csv", s3_bucket, s3_prefix, league)
        s3object.put(Body=data.export('csv'))

        return 1


def _extract_data(season_data, league, season):
    data = tablib.Dataset()
    data.csv = season_data.text
    div = [league] * data.height
    season = [season] * data.height
    dates = data['Date']
    home_teams = data['Home'] if 'Home' in data.headers else data['HomeTeam']
    away_teams = data['Away'] if 'Away' in data.headers else data['AwayTeam']
    home_goals = data['HG'] if 'HG' in data.headers else data['FTHG']
    away_goals = data['AG'] if 'AG' in data.headers else data['FTAG']
    results = data['Res'] if 'Res' in data.headers else data['FTR']
    pshs = data['PH'] if 'PH' in data.headers else data['PSH']
    psds = data['PD'] if 'PD' in data.headers else data['PSD']
    psas = data['PA'] if 'PA' in data.headers else data['PSA']
    avgH = data['AvgH'] if 'AvgH' in data.headers else data['BbAvH']
    avgD = data['AvgD'] if 'AvgD' in data.headers else data['BbAvD']
    avgA = data['AvgA'] if 'AvgA' in data.headers else data['BbAvA']

    return zip(div, season, dates, home_teams, away_teams, home_goals, away_goals, results, pshs, psds, psas, avgH, avgD, avgA)


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
