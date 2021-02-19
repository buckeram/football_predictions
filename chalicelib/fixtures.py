import boto3
import logging
import os
import requests
import tablib
from chalice import Blueprint
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from . import common


fixtures = Blueprint(__name__)

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

s3 = boto3.resource('s3')


@fixtures.schedule('cron(0 18 * * ? *)')
def get_fixtures(event):

    # Set up the requests Session with retries
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 500, 502, 503, 504 ])
    s.mount('http://', HTTPAdapter(max_retries=retries))
    s.mount('https://', HTTPAdapter(max_retries=retries))

    # Retrieve the fixtures from the web api
    num_days = None
    if 'FIXTURES_DAYS' in os.environ:
        num_days = os.environ['FIXTURES_DAYS']
    url = os.environ['FIXTURES_URL']
    if num_days is not None:
        url += "?days={}".format(num_days)
    LOGGER.info("Fetching fixtures from %s", url)
    r = s.get(url, verify=False, timeout=240)

    # Load the csv into a tablib Dataset
    data = tablib.Dataset()
    data.csv = r.text
    _change_headers(data)

    # Add column which represents the 'football-data' division
    data.lpush_col(_get_division, header='Div')
    del data['Country']
    del data['League']

    # Remove the rows for leagues we don't want (i.e. ones not in ODDSPORTAL_DIVISIONS)
    keepers = [i for i, row in enumerate(data) if row[0] is not None]
    subset = data.subset(rows=keepers)

    subset.insert_col(5, _convert_home_team_name, 'HomeTeam')
    subset.insert_col(6, _convert_away_team_name, 'AwayTeam')
    del subset['oddsportalHomeTeam']
    del subset['oddsportalAwayTeam']

    s3_bucket = os.environ['S3_BUCKET']
    s3_key = 'fixtures/fixtures.csv'
    LOGGER.info("Writing fixtures to %s/%s", s3_bucket, s3_key)
    s3.Object(s3_bucket, s3_key).put(Body=subset.export('csv'))

    LOGGER.info("Done.")


def _get_division(row):
    country_and_league = row[0], row[1]
    if country_and_league in common.ODDSPORTAL_DIVISIONS:
        return common.ODDSPORTAL_DIVISIONS[country_and_league]
    else:
        return None

def _change_headers(data):
    new_headers = []
    for header in data.headers:
        if header.endswith('Team'):
            new_headers.append('oddsportal' + header)
        else:
            new_headers.append(header)

    data.headers = new_headers

def _convert_home_team_name(row):
    div, home_team = row[0], row[3]
    return common.get_team_name(home_team, div)

def _convert_away_team_name(row):
    div, away_team = row[0], row[4]
    return common.get_team_name(away_team, div)
