import json
import logging
import os
import re
import textwrap
from . import common

# The idea for this comes from https://www.financial-spread-betting.com/sports/Goals-betting-system.html

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

main_leagues = re.split(r'[,\s]+', os.environ['MAIN_LEAGUES'])
# Dropping these leagues; they're less predictable...
for league in ['SP1', 'SP2']:
    main_leagues.remove(league)


def predict_over_under(event):
    LOGGER.info("Looks like there's a new fixtures file: %s/%s", event.bucket, event.key)

    # Read the fixtures into CSV
    fixtures = common.get_fixtures_from_s3(event.bucket, event.key)
    # Get the unique list of leagues ('Div')
    leagues = sorted(set(fixtures['Div']))

    all_predictions = []

    for league in leagues:
        LOGGER.info("League: %s", league)
        if league and league not in main_leagues:
            LOGGER.info("Skipping league %s", league)
            continue

        past_league_matches = common.get_historical_data_from_s3(event.bucket, league)
        if past_league_matches is None:
            LOGGER.info("Hmm, no matches for league '%s'", league)
            continue

        league_predictions = dict()
        league_predictions['div'] = league
        league_predictions['league'] = common.DIVISIONS_TO_LEAGUES[league]

        next_league_fixtures = common.get_league_data(fixtures, league)
        next_matches = zip(next_league_fixtures['HomeTeam'], next_league_fixtures['AwayTeam'],
                           next_league_fixtures['Date'], next_league_fixtures['Time'])

        league_predictions['predictions'] = predict(league, past_league_matches, next_matches)

        all_predictions.append(league_predictions)

    predictions_json = json.dumps(all_predictions)
    email_predictions(predictions_json)


def email_predictions(all_predictions_json):
    league_template = textwrap.dedent("""

    ==================================================
    %s: %s
    ==================================================
    """)

    match_template = """
    %s %s : %s -- %s v. %s
    """

    all_predictions = json.loads(all_predictions_json)
    mail_message = ""

    for div_predictions in all_predictions:
        mail_message += league_template % (div_predictions['div'], div_predictions['league'])

        for prediction in div_predictions['predictions']:
            outcome = "over" if prediction['is_over_25'] == True else "under"
            mail_message += match_template % (prediction['date'], prediction['time'],
                                              outcome, prediction['home_team'], prediction['away_team'])

    LOGGER.info("Football predictions (Over/Under 2.5 goals) \n%s", mail_message)
    common.send_mail("Football predictions (Over/Under 2.5 goals)", mail_message)


def predict(league, past_data, matches):
    n = 3 # This is the number of previous matches to consider when making a prediction

    league_data = common.get_league_data(past_data, league)
    latest_season = os.environ['LATEST_SEASON']
    league_data = get_subset_for_season(league_data, latest_season)

    predictions = []

    for match in matches:
        home_team, away_team, date, time = match
        home_team = common.get_team_name(home_team, league)
        away_team = common.get_team_name(away_team, league)

        # These are tablib Datasets
        home_team_past_matches = get_past_matches(home_team, league_data, is_home=True)
        if len(home_team_past_matches) < n:
            continue
        away_team_past_matches = get_past_matches(away_team, league_data, is_home=False)
        if len(away_team_past_matches) < n:
            continue

        home_team_matches_home_goals = [int(x) for x in home_team_past_matches['FTHG'][-n:]]
        home_team_matches_away_goals = [int(x) for x in home_team_past_matches['FTAG'][-n:]]
        home_team_match_total_goals = sum(home_team_matches_home_goals) + sum(home_team_matches_away_goals)
        home_goals = zip(home_team_matches_home_goals, home_team_matches_away_goals)
        is_home_team_over25 = len(list(filter(lambda x: x[0] + x[1] > 2.5, home_goals))) >= 2

        away_team_matches_home_goals = [int(x) for x in away_team_past_matches['FTHG'][-n:]]
        away_team_matches_away_goals = [int(x) for x in away_team_past_matches['FTAG'][-n:]]
        away_team_match_total_goals = sum(away_team_matches_home_goals) + sum(away_team_matches_away_goals)
        away_goals = zip(away_team_matches_home_goals, away_team_matches_away_goals)
        is_away_team_over25 = len(list(filter(lambda x: x[0] + x[1] > 2.5, away_goals))) >= 2
        is_away_team_previous_game_over25 = away_team_matches_home_goals[-1] + away_team_matches_away_goals[-1] > 2
        is_away_team_scored_in_previous_games = len(list(filter(lambda x: x > 0, away_team_matches_away_goals))) >=2

        prediction = dict()
        prediction['date'] = date
        prediction['time'] = time
        prediction['home_team'] = home_team
        prediction['away_team'] = away_team

        if home_team_match_total_goals >= 7 and is_home_team_over25 \
                and away_team_match_total_goals >= 7 and is_away_team_over25 \
                and is_away_team_previous_game_over25 \
                and is_away_team_scored_in_previous_games:

            prediction['is_over_25'] = True

        else:

            prediction['is_over_25'] = False

        predictions.append(prediction)

        outcome = "over" if prediction['is_over_25'] == True else "under"
        output = """
        %s %s : %s -- %s  %s v. %s
        """ % (date, time, outcome, league, home_team, away_team)
        output = textwrap.dedent(output)
        print(output, "\n")

    return predictions


def get_subset_for_season(league_data, season):
    subset_rows = []
    for i in range(league_data.height):
        row = league_data[i]
        if row[1] == season:
            subset_rows.append(i)

    return league_data.subset(rows=subset_rows)


def get_past_matches(team, data, is_home=True):
    team_col = 3 if is_home else 4
    rows = []
    for i in range(data.height):
        row = data[i]
        if row[team_col] == team:
            rows.append(i)

    return data.subset(rows=rows)