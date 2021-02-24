import boto3
import json
import logging
import numpy as np
import os
import re
import requests
import tablib
import textwrap
from datetime import datetime
from chalice import Blueprint
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from chalicelib import common
from datetime import datetime, timezone
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from collections import Counter, defaultdict, deque
from . import common


predictions = Blueprint(__name__)

s3 = boto3.resource('s3')
ses = boto3.client('ses')

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

s3_historical_prefix = os.environ['S3_PREFIX_HISTORICAL']
main_leagues = os.environ['MAIN_LEAGUES']
new_leagues = os.environ['NEW_LEAGUES']

to_email_address = os.environ["EMAIL_RECIPIENT"]
from_email_address = os.environ["EMAIL_SENDER"]

Classifier = LinearRegression


@predictions.on_s3_event(bucket=os.environ['S3_BUCKET'], prefix='fixtures/',
                 suffix='.csv', events=['s3:ObjectCreated:*'])
def make_predictions(event):
    LOGGER.info("Looks like there's a new fixtures file: %s/%s", event.bucket, event.key)

    # Read the fixtures into CSV
    fixtures_file = s3.Object(event.bucket, event.key).get()['Body'].read()
    fixtures = tablib.Dataset()
    fixtures.csv = fixtures_file.decode()
    fixtures = fixtures.sort('Div')

    # Get the unique list of leagues ('Div')
    leagues = sorted(set(fixtures['Div']))

    # For each div:
    #   Read div CSV
    #   Train home/draw/away classifiers for each div
    #   Make predictions for the div
    # Send email containing predictions

    all_classifiers = dict()
    classifier_scores = dict()

    all_predictions = []

    for league in leagues:
        LOGGER.info("League: %s", league)
        if league not in main_leagues and league not in new_leagues:
            LOGGER.info("Skipping league %s", league)
            continue

        past_league_matches = get_historical_data_from_s3(event.bucket, league)
        if past_league_matches is None:
            LOGGER.info("Hmm, no matches for league '%s'", league)
            continue

        league_predictions = dict()
        league_predictions['div'] = league
        league_predictions['league'] = common.DIVISIONS_TO_LEAGUES[league]

        home_clf, draw_clf, away_clf, best_home_score, best_draw_score, best_away_score, home_best_fname, draw_best_fname, away_best_fname = train_classifiers(past_league_matches, league)
        league_predictions['home_score'] = best_home_score
        league_predictions['draw_score'] = best_draw_score
        league_predictions['away_score'] = best_away_score
        league_predictions['home_fname'] = home_best_fname
        league_predictions['draw_fname'] = draw_best_fname
        league_predictions['away_fname'] = away_best_fname

        classifiers = {'home_clf': home_clf, 'draw_clf': draw_clf, 'away_clf': away_clf,
            'home_fname': home_best_fname, 'draw_fname': draw_best_fname, 'away_fname': away_best_fname}
        classifier_scores = {'home': best_home_score, 'draw': best_draw_score, 'away': best_away_score}

        next_league_fixtures = get_league_data(fixtures, league)
        next_matches = zip(next_league_fixtures['HomeTeam'], next_league_fixtures['AwayTeam'])

        predictions = predict(past_league_matches, league, classifiers, next_matches)
        league_predictions['predictions'] = predictions
        all_predictions.append(league_predictions)

    predictions_json = json.dumps(all_predictions)
    email_all_predictions(predictions_json)
    email_best_predictions(predictions_json)
    email_draw_predictions(predictions_json)


def email_all_predictions(all_predictions_json):
    league_template = textwrap.dedent("""
    ==================================================
    %s: %s
    ==================================================
    Home (%s) %.2f
    Away (%s) %.2f
    Draw (%s) %.2f

    """)
    match_template = textwrap.dedent("""
    %s v. %s
    --------------------------------------------------
    Home: %.2f   (%.2f)
    Draw: %.2f   (%.2f)
    Away: %.2f   (%.2f)

    """)

    all_predictions = json.loads(all_predictions_json)
    mail_message = ""

    for league in all_predictions:
        mail_message += league_template % (league['div'], league['league'],
                                           league['home_fname'], league['home_score'],
                                           league['away_fname'], league['away_score'],
                                           league['draw_fname'], league['draw_score'])

        for prediction in league['predictions']:
            mail_message += match_template % (prediction['home_team'], prediction['away_team'],
                                              prediction['home_pct'], prediction['home_odds'],
                                              prediction['draw_pct'], prediction['draw_odds'],
                                              prediction['away_pct'], prediction['away_odds'])


    LOGGER.info("Football predictions (Home/Away) \n%s", mail_message)
    # send_mail("Football predictions (Home/Away)", mail_message)


def email_best_predictions(all_predictions_json):
    league_template = textwrap.dedent("""

    ==================================================
    %s: %s
    ==================================================
    """)

    match_template = textwrap.dedent("""
    %s v. %s
    --------------------------------------------------
    %s: %.2f   (%.2f)

    """)

    cutoff = 65
    if "PREDICTION_CUTOFF" in os.environ:
        cutoff = int(os.environ['PREDICTION_CUTOFF'])

    all_predictions = json.loads(all_predictions_json)
    mail_message = ""

    for div_predictions in all_predictions:
        div = div_predictions['div']
        league = div_predictions['league']
        top_predictions = []

        for prediction in div_predictions['predictions']:
            home_team, away_team = prediction['home_team'], prediction['away_team']
            if prediction['home_pct'] >= cutoff:
                top_predictions.append({'home': home_team, 'away': away_team, 'winner': home_team, 'pct': prediction['home_pct'], 'odds': prediction['home_odds']})
            if prediction['away_pct'] >= cutoff:
                top_predictions.append({'home': home_team, 'away': away_team, 'winner': away_team, 'pct': prediction['away_pct'], 'odds': prediction['away_odds']})

        if len(top_predictions) > 0:
            mail_message += league_template % (div, league)
            for p in top_predictions:
                mail_message += match_template % (p['home'], p['away'], p['winner'], p['pct'], p['odds'])

    if mail_message:
        send_mail("Top football predictions (cutoff = %.2f%%)" % cutoff, mail_message)


def email_draw_predictions(all_predictions_json):
    league_template = textwrap.dedent("""

    ==================================================
    %s: %s
    ==================================================
    """)

    match_template = textwrap.dedent("""
    %s v. %s
    --------------------------------------------------
    Draw: %.2f   (%.2f)

    """)

    cutoff = 30
    if 'DRAW_CUTOFF' in os.environ:
        cutoff = int(os.environ['DRAW_CUTOFF'])

    all_predictions = json.loads(all_predictions_json)
    mail_message = ""

    for div_predictions in all_predictions:
        div = div_predictions['div']
        league = div_predictions['league']
        draw_predictions = []

        for prediction in div_predictions['predictions']:
            home_team, away_team = prediction['home_team'], prediction['away_team']
            if prediction['draw_pct'] >= cutoff:
                home_pct = prediction['home_pct']
                away_pct = prediction['away_pct']
                home_away_diff = abs(home_pct - away_pct)
                if (home_pct >= 20 and home_pct < 50) and (away_pct >= 20 and away_pct < 50) and (home_away_diff < 15):
                    draw_predictions.append({'home': home_team, 'away': away_team, 'pct': prediction['draw_pct'], 'odds': prediction['draw_odds']})

        if len(draw_predictions) > 0:
            mail_message += league_template % (div, league)
            for p in draw_predictions:
                mail_message += match_template % (p['home'], p['away'], p['pct'], p['odds'])

    if mail_message:
        send_mail("Football draw predictions (cutoff = %.2f%%)" % cutoff, mail_message)


def send_mail(subject, mail_message):
    LOGGER.info("Sending mail (subject: %s)...", subject)

    response = ses.send_email(
        Destination={
            'ToAddresses': [ to_email_address ]
        },
        Message={
            'Body': {
                'Text': {
                    'Charset': 'UTF-8',
                    'Data': mail_message,
                }
            },
            'Subject': {
                'Charset': 'UTF-8',
                'Data': subject,
            }
        },
        Source=from_email_address
    )

    LOGGER.info("Mail sent: %s", response)



def get_historical_data_from_s3(bucket, league):
    key = s3_historical_prefix.rstrip('/') + '/' + league + '.csv'
    matches_file = s3.Object(bucket, key).get()['Body'].read()
    # LOGGER.info("%s", matches_file)
    matches = tablib.Dataset()
    # matches.load(matches_file, format='csv')
    matches.csv = matches_file.decode()
    return matches


def train_classifiers(data, league):

    league_data = get_league_data(data, league)
    add_ppg_fields(league_data)

    home_classifiers = dict()
    draw_classifiers = dict()
    away_classifiers = dict()
    home_scores = dict()
    draw_scores = dict()
    away_scores = dict()

    for fname in ['PpgDiff', 'HomeAwayPpgDiff']:
        home_diffs, home_pcts = get_percentages_for_diffs(league_data, fname, 'H')
        draw_diffs, draw_pcts = get_percentages_for_diffs(league_data, fname, 'D')
        away_diffs, away_pcts = get_percentages_for_diffs(league_data, fname, 'A')

        X_home, y_home = np.array(home_diffs).reshape(-1, 1), home_pcts
        home_classifiers[fname] = make_pipeline(StandardScaler(), Classifier()).fit(X_home, y_home)
        home_scores[fname] = home_classifiers[fname].score(X_home, y_home)

        X_away, y_away = np.array(away_diffs).reshape(-1, 1), away_pcts
        away_classifiers[fname] = make_pipeline(StandardScaler(), Classifier()).fit(X_away, y_away)
        away_scores[fname] = away_classifiers[fname].score(X_away, y_away)

        X_draw, y_draw = np.array(draw_diffs).reshape(-1, 1), draw_pcts
        draw_classifiers[fname] = make_pipeline(StandardScaler(), Classifier()).fit(X_draw, y_draw)
        draw_scores[fname] = draw_classifiers[fname].score(X_draw, y_draw)

    home_best_fname = sorted(home_scores, key=home_scores.__getitem__, reverse=True)[0]
    away_best_fname = sorted(away_scores, key=away_scores.__getitem__, reverse=True)[0]
    draw_best_fname = sorted(draw_scores, key=draw_scores.__getitem__, reverse=True)[0]
    home_clf = home_classifiers[home_best_fname]
    away_clf = away_classifiers[away_best_fname]
    draw_clf = draw_classifiers[draw_best_fname]

    best_home_score = home_scores[home_best_fname]
    best_draw_score = draw_scores[draw_best_fname]
    best_away_score = away_scores[away_best_fname]

    return home_clf, draw_clf, away_clf, \
        best_home_score, best_draw_score, best_away_score,\
        home_best_fname, draw_best_fname, away_best_fname


def get_league_data(data, league):
    league_data = tablib.Dataset()
    [league_data.append(row) for row in data if row[0] == league]
    if type(data) == tablib.Dataset:
        league_data.headers = data.headers
    return league_data


def add_ppg_fields(data):

    n_matches_as_home_team = defaultdict(lambda: 0)
    n_matches_as_away_team = defaultdict(lambda: 0)
    points_as_home_team = defaultdict(lambda: 0)
    points_as_away_team = defaultdict(lambda: 0)
    goals = defaultdict(lambda: 0)

    home_team_ppg_at_home = []
    home_team_overall_ppg = []
    away_team_ppg_away = []
    away_team_overall_ppg = []
    home_team_goals_per_game = []
    away_team_goals_per_game = []

    for row in zip(data['HomeTeam'], data['AwayTeam'], data['FTR'], data['FTHG'], data['FTAG']):

        home_team, away_team, ftr, home_goals, away_goals = row

        if n_matches_as_home_team[home_team] > 0:
            home_team_ppg_at_home.append(points_as_home_team[home_team] / n_matches_as_home_team[home_team])
        else:
            home_team_ppg_at_home.append(0.0)

        home_team_total_matches = n_matches_as_home_team[home_team] + n_matches_as_away_team[home_team]
        if home_team_total_matches > 0:
            home_team_overall_ppg.append((points_as_home_team[home_team] + points_as_away_team[home_team]) / home_team_total_matches)
            home_team_goals_per_game.append(goals[home_team] / home_team_total_matches)
        else:
            home_team_overall_ppg.append(0.0)
            home_team_goals_per_game.append(0.0)

        if n_matches_as_away_team[away_team] > 0:
            away_team_ppg_away.append(points_as_away_team[away_team] / n_matches_as_away_team[away_team])
        else:
            away_team_ppg_away.append(0.0)

        away_team_total_matches = n_matches_as_home_team[away_team] + n_matches_as_away_team[away_team]
        if away_team_total_matches > 0:
            away_team_overall_ppg.append((points_as_home_team[away_team] + points_as_away_team[away_team]) / away_team_total_matches)
            away_team_goals_per_game.append(goals[away_team] / away_team_total_matches)
        else:
            away_team_overall_ppg.append(0.0)
            away_team_goals_per_game.append(0.0)

        n_matches_as_home_team[home_team] += 1
        n_matches_as_away_team[away_team] += 1
        points_as_home_team[home_team] += points_for_home_team(ftr)
        points_as_away_team[away_team] += points_for_away_team(ftr)
        goals[home_team] += int(home_goals) if home_goals else 0
        goals[away_team] += int(away_goals) if away_goals else 0

    data.append_col(home_team_ppg_at_home, header='HomeTeamPpgAtHome')
    data.append_col(home_team_overall_ppg, header='HomeTeamOverallPpg')
    data.append_col(away_team_ppg_away, header='AwayTeamPpgAway')
    data.append_col(away_team_overall_ppg, header='AwayTeamOverallPpg')
    data.append_col(home_team_goals_per_game, header='HomeTeamGpg')
    data.append_col(away_team_goals_per_game, header='AwayTeamGpg')

    ppg_diff = get_diff(data['HomeTeamOverallPpg'], data['AwayTeamOverallPpg'])
    data.append_col(ppg_diff, header='PpgDiff')
    home_away_diff = get_diff(data['HomeTeamPpgAtHome'], data['AwayTeamPpgAway'])
    data.append_col(home_away_diff, header='HomeAwayPpgDiff')
    gpg_total = get_sum(data['HomeTeamGpg'], data['AwayTeamGpg'])
    data.append_col(gpg_total, header='GpgTotal')
    gpg_diff = get_diff(data['HomeTeamGpg'], data['AwayTeamGpg'])
    data.append_col(gpg_diff, header='GpgDiff')


def points_for_home_team(ftr):
    if ftr == 'H':
        return 3
    elif ftr == 'D':
        return 1
    else:
        return 0


def points_for_away_team(ftr):
    if ftr == 'A':
        return 3
    elif ftr == 'D':
        return 1
    else:
        return 0


def points_for_team(fname, ftr):
    if fname == 'HomeTeam':
        return points_for_home_team(ftr)
    else:
        return points_for_away_team(ftr)


def get_diff(col_a, col_b):
    return [x - y for x, y in zip(col_a, col_b)]


def get_sum(col_a, col_b):
    return [x + y for x, y in zip(col_a, col_b)]


def get_percentages_for_diffs(data, fname, ftr):
    pcts = []
    diffs = []
    # Skip the first 50 matches to allow data to 'settle down'
    start, end = np.quantile(data[fname][50:], [0.05, 0.95])
    step = 0.1
    fname_idx = data.headers.index(fname)
    for i in np.arange(start, end, step):
        keepers = [idx for idx, row in enumerate(data) if row[fname_idx] >= i and row[fname_idx] < i+step]
        subset = data.subset(rows=keepers)
        vc = Counter(subset['FTR'])
        h, d, a = 0, 0, 0
        n = 0
        if 'H' in vc:
            h = vc['H']
        if 'D' in vc:
            d = vc['D']
        if 'A' in vc:
            a = vc['A']

        total = h + d + a

        if ftr == 'H':
            n = h
        elif ftr == 'D':
            n = d
        elif ftr == 'A':
            n = a

        if n > 0:
            pcts.append(n / total * 100.0 if total > 0 else 0.0)
            diffs.append(i)

    return diffs, pcts


def get_latest_overall_ppg_for_team(data, league, team):
    league_data = get_league_data(data, league)
    add_ppg_fields(league_data)

    home_teams = league_data['HomeTeam']
    away_teams = league_data['AwayTeam']

    # How many matches has the team played overall?
    home_counts = home_teams.count(team)
    away_counts = away_teams.count(team)
    n = home_counts + away_counts

    # Find whether the team's last match was home or away, and its index in df
    if team not in home_teams:
        print("Weird... {} not in home_teams".format(team))
        print("home_teams:", home_teams)
    last_home_game_idx = len(home_teams) - home_teams[::-1].index(team) - 1
    last_away_game_idx = len(away_teams) - away_teams[::-1].index(team) - 1

    # Use n and the last match to calculate the 'next' PPG for the team
    previous_ppg, points = 0, 0
    full_time_results = league_data['FTR']
    if last_home_game_idx > last_away_game_idx:
        points = points_for_home_team(league_data['FTR'][last_home_game_idx])
        previous_ppg = league_data['HomeTeamOverallPpg'][last_home_game_idx]
    else:
        points = points_for_away_team(league_data['FTR'][last_away_game_idx])
        previous_ppg = league_data['AwayTeamOverallPpg'][last_away_game_idx]

    return (previous_ppg * n + points) / (n + 1)


def get_latest_home_or_away_ppg_for_team(data, div, team, fname):
    league_data = get_league_data(data, div)
    add_ppg_fields(league_data)

    # How many matches has the team played at home/away?
    n = league_data[fname].count(team)

    # Get the index of the last game that the team played home/away
    teams = league_data[fname]
    last_game_idx = len(teams) - teams[::-1].index(team) - 1

    # Use n and the last match to calculate the 'next' PPG for the team
    points = points_for_team(fname, league_data['FTR'][last_game_idx])
    previous_ppg = league_data[fname + 'OverallPpg'][last_game_idx]

    return (previous_ppg * n + points) / (n + 1)


def get_latest_home_ppg_for_team(data, div, team):
    return get_latest_home_or_away_ppg_for_team(data, div, team, 'HomeTeam')


def get_latest_away_ppg_for_team(data, div, team):
    return get_latest_home_or_away_ppg_for_team(data, div, team, 'AwayTeam')


def predict(data, league, classifiers, matches):
    league_data = get_league_data(data, league)
    subset_rows = list(range(len(league_data) - 200, len(league_data)))
    league_data = league_data.subset(rows=subset_rows)

    output = ""
    predictions = []

    for match in matches:

        home_team, away_team = match
        home_team = common.get_team_name(home_team, league)
        away_team = common.get_team_name(away_team, league)

        home_team_overall_ppg = get_latest_overall_ppg_for_team(league_data, league, home_team)
        away_team_overall_ppg = get_latest_overall_ppg_for_team(league_data, league, away_team)
        diff_overall_ppg = home_team_overall_ppg - away_team_overall_ppg

        home_team_home_ppg = get_latest_home_ppg_for_team(league_data, league, home_team)
        away_team_away_ppg = get_latest_away_ppg_for_team(league_data, league, away_team)
        diff_home_away_ppg = home_team_home_ppg - away_team_away_ppg

        home_diff = diff_overall_ppg if classifiers['home_fname'] == 'PpgDiff' else diff_home_away_ppg
        away_diff = diff_overall_ppg if classifiers['away_fname'] == 'PpgDiff' else diff_home_away_ppg
        draw_diff = diff_overall_ppg if classifiers['draw_fname'] == 'PpgDiff' else diff_home_away_ppg

        home = classifiers['home_clf'].predict([[home_diff]])[0]
        away = classifiers['away_clf'].predict([[away_diff]])[0]
        draw = classifiers['draw_clf'].predict([[draw_diff]])[0]

        prediction = dict()
        prediction['home_team'] = home_team
        prediction['away_team'] = away_team
        prediction['home_pct'] = home
        prediction['home_odds'] = 1/home*100
        prediction['draw_pct'] = draw
        prediction['draw_odds'] = 1/draw*100
        prediction['away_pct'] = away
        prediction['away_odds'] = 1/away*100

        predictions.append(prediction)

        output = """
        %s: %s
        --------------------------------------------------
        Home: %.2f   (%.2f)
        Draw: %.2f   (%.2f)
        Away: %.2f   (%.2f)

        """ % (league, match, home, 100/home, draw, 100/draw, away, 100/away)
        output = textwrap.dedent(output)
        print(output, "\n")

    return predictions
