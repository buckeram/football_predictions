import boto3
import logging
import datetime
import proxyscrape
import re
import tablib
from chalice import Chalice, Rate
from requests_html import HTMLSession


app = Chalice(app_name='football_predictions')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

s3 = boto3.resource('s3')

fieldnames = ['Country', 'League', 'Date', 'Time', 'HomeTeam', 'AwayTeam', 'AvgH', 'AvgD', 'AvgA']
num_days = 2 # How many days worth of matches to retrieve

session = HTMLSession()
collector = proxyscrape.create_collector('fixtures-proxy', ['http', 'https'])


@app.schedule(Rate(10, unit=Rate.MINUTES))
def get_fixtures(event):

    collector = proxyscrape.get_collector('fixtures-proxy')
    http_proxy = collector.get_proxy({'type': 'http'})
    https_proxy = collector.get_proxy({'type': 'https'})
    LOGGER.info("HTTP Proxy: %s", http_proxy)
    LOGGER.info("HTTPS Proxy: %s", https_proxy)
    proxies = {
        'http': "http://{}:{}".format(http_proxy.host, http_proxy.port),
        'https': "http://{}:{}".format(https_proxy.host, https_proxy.port)
    }

    data = tablib.Dataset()
    data.headers = fieldnames

    for i in range(num_days):

        day = datetime.date.today() + datetime.timedelta(days=i+1)
        url = "https://www.oddsportal.com/matches/soccer/%s/" % day.strftime("%Y%m%d")
        LOGGER.info("Fetching fixtures from %s", url)

        r = session.get(url, proxies=proxies, timeout=60, verify=False)
        r.html.render(wait=5.0, sleep=20, retries=10, timeout=120)

        table_matches = r.html.find('div#table-matches > table.table-main', first=True)
        league = ""
        for tr in table_matches.find('tr'):
            if 'class' in tr.attrs:
                css_class = tr.attrs['class']
                if css_class:
                    if 'dark' in css_class:
                        country, division = tr.find('th.tl', first=True).text.split('»')
                    elif 'odd' in css_class:
                        fixture = tr.text.replace('postp.', '')
                        fixture = re.sub(r'\n+', '\t', fixture)
                        time, match, home_us, draw_us, away_us = fixture.split('\t')[:5]
                        match_date = day.strftime("%d/%m/%Y")
                        home_eu = us_odds_to_decimal(home_us)
                        draw_eu = us_odds_to_decimal(draw_us)
                        away_eu = us_odds_to_decimal(away_us)
                        home_team, away_team = match.split(' - ')

                        data.append([country, division, match_date, time,
                                     home_team.strip(), away_team.strip(),
                                     home_eu, draw_eu, away_eu])

    s3_bucket = os.environ['S3_BUCKET']
    s3_key = 'fixtures/fixtures.csv'
    LOGGER.info("Writing fixtures to %s/%s", s3_bucket, s3_key)
    s3.Object(s3_bucket, s3_key).put(Body=data.export('csv'))


def us_odds_to_decimal(odds_str):

    if '.' in odds_str:
        # Not US odds
        return float(odds_str)

    us_odds = int(odds_str)
    eu_odds = 0.0
    if us_odds > 0:
        eu_odds = 1 + (us_odds / 100)
    else:
        eu_odds = 1 - (100 / us_odds)

    return eu_odds


# @app.route('/')
# def index():
#     return {'hello': 'world'}
# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
