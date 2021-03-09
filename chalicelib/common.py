import boto3
import logging
import tablib
import os


# A mapping from oddsportal.com leagues to football-data.co.uk's 'Div'
ODDSPORTAL_DIVISIONS = {
    ('England', 'Premier League'): 'E0',
    ('England', 'Championship'): 'E1',
    ('England', 'League One'): 'E2',
    ('England', 'League Two'): 'E3',
    ('England', 'National League'): 'EC',

    ('Austria', 'Tipico Bundesliga'): 'AUT',
    ('Belgium', 'Jupiler League'): 'B1',
    ('Brazil', 'Serie A'): 'BRA',
    ('Denmark', 'Superliga'): 'DNK',
    ('France', 'Ligue 1'): 'F1',
    ('France', 'Ligue 2'): 'F2',
    ('Germany', 'Bundesliga'): 'D1',
    ('Germany', '2. Bundesliga'): 'D2',
    ('Greece', 'Super League'): 'G1',
    ('Ireland', 'Premier Division'): 'IRL',
    ('Italy', 'Serie A'): 'I1',
    ('Italy', 'Serie B'): 'I2',
    ('Mexico', 'Liga MX'): 'MEX',
    ('Netherlands', 'Eredivisie'): 'N1',
    ('Norway', 'Eliteserien'): 'NOR',
    ('Poland', 'Ekstraklasa'): 'POL',
    ('Portugal', 'Primeira Liga'): 'P1',
    ('Romania', 'Liga 1'): 'ROU',
    ('Russia', 'Premier League'): 'RUS',
    ('Scotland', 'Premiership'): 'SC0',
    ('Spain', 'LaLiga'): 'SP1',
    ('Spain', 'LaLiga2'): 'SP2',
    ('Sweden', 'Allsvenskan'): 'SWE',
    ('Switzerland', 'Super League'): 'SWZ',
    ('Turkey', 'Super Lig'): 'T1',
    ('China', 'Super League'): 'CHN', # check
    ('Japan', 'J1 League'): 'JPN' # check
}

DIVISIONS_TO_LEAGUES = {
    'E0': 'England Premiership',
    'E1': 'England Championship',
    'E2': 'England League 1',
    'E3': 'England League 2',
    'EC': 'England National League',
    'AUT': 'Austria Tipico Bundesliga',
    'B1': 'Belgium Jupiler League',
    'BRA': 'Brazil Serie A',
    'DNK': 'Denmark Superligaen',
    'D1': 'Germany Bundesliga',
    'D2': 'Germany Bundesliga 2',
    'F1': 'France Ligue 1',
    'F2': 'France Ligue 2',
    'G1': 'Greece Super League 1',
    'IRL': 'Ireland Premier Division',
    'I1': 'Italy Serie A',
    'I2': 'Italy Serie B',
    'MEX': 'Mexico Clausura/Liga MX',
    'N1': 'Netherlands Eredivisie',
    'NOR': 'Norway Eliteserien',
    'POL': 'Poland Ekstraklasa',
    'P1': 'Portugal Primeira Liga',
    'ROU': 'Romania Liga 1',
    'RUS': 'Russia Premier League',
    'SC0': 'Scotland Premiership',
    'SP1': 'Spain LaLiga',
    'SP2': 'Spain LaLiga 2',
    'SWE': 'Sweden Allsvenskan',
    'SWZ': 'Switzerland Super League',
    'T1': 'Turkey 1 Lig',
    'CHN': 'China Super League',
    'JPN': 'Japan J1 League'
}

# Mapping of oddsportal team names to football-data teams
ODDSPORTAL_TEAMS = {
    'B1': {
        'KV Mechelen': 'Mechelen',
        'Cercle Brugge KSV': 'Cercle Brugge',
        'St. Liege': 'Standard',
        'St. Truiden': 'St Truiden',
        'Club Brugge KV': 'Club Brugge',
        'Leuven': 'Oud-Heverlee Leuven'
    },
    'E0': {
        'Manchester Utd': 'Man United',
        'Sheffield Utd': 'Sheffield United',
        'Manchester City': 'Man City'
    },
    'E1': {
        'Sheffield Wed': 'Sheffield Weds',
        'Nottingham': "Nott'm Forest"
    },
    'EC': {
        'Solihull Moors': 'Solihull',
        'Dag & Red': 'Dag and Red',
        'Dover': 'Dover Athletic',
        'Notts Co': 'Notts County',
        'FC Halifax': 'Halifax',
        "King's Lynn": "King\x92s Lynn",
        "King’s Lynn": "King\x92s Lynn"
    },
    'E2': {
        'Peterborough': 'Peterboro',
        'Bristol Rovers': 'Bristol Rvs',
        'Oxford Utd': 'Oxford',
        'MK Dons': 'Milton Keynes Dons',
        'Fleetwood': 'Fleetwood Town'
    },
    'E3': {
        'Crawley': 'Crawley Town',
        'Cambridge Utd': 'Cambridge',
        'Newport': 'Newport County',
        'Bradford City': 'Bradford'
    },
    'F2': {
        'AC Ajaccio': 'Ajaccio'
    },
    'D1': {
        'Arminia Bielefeld': 'Bielefeld',
        'B. Monchengladbach': "M'gladbach",
        'Eintracht Frankfurt': 'Ein Frankfurt',
        'Schalke': 'Schalke 04',
        'Bayer Leverkusen': 'Leverkusen',
        'Hertha Berlin': 'Hertha',
    },
    'D2': {
        'Karlsruher': 'Karlsruhe',
        'Aue': 'Erzgebirge Aue',
        'Dusseldorf': 'Fortuna Dusseldorf',
        'VfL Osnabruck': 'Osnabruck',
        'St. Pauli': 'St Pauli',
        'Hamburger SV': 'Hamburg'
    },
    'G1': {
        'AEL Larissa': 'Larisa',
        'AEK Athens FC': 'AEK',
        'Smyrnis': 'Apollon',
        'Volos': 'Volos NFC',
        'Olympiacos Piraeus': 'Olympiakos'
    },
    'I1': {
        'AC Milan': 'Milan',
        'AS Roma': 'Roma'
    },
    'I2': {
        'Entella': 'Virtus Entella',
        'L.R. Vicenza': 'Vicenza'
    },
    'N1': {
        'Venlo': 'VVV Venlo',
        'Sittard': 'For Sittard',
        'PSV': 'PSV Eindhoven'
    },
    'P1': {
        'Sporting': 'Sp Lisbon',
        'Ferreira': 'Pacos Ferreira',
        'Vitoria Guimaraes': 'Guimaraes',
        'SC Farense': 'Farense',
        'Braga': 'Sp Braga',
        'FC Porto': 'Porto'
    },
    'ROU': {
        'Univ. Craiova': 'U Craiova',
        'FCSB': 'FC Steaua Bucuresti'
    },
    'SC0': {
        'Dundee Utd': 'Dundee United',
        'Dundee': 'Dundee United',
        'St. Mirren': 'St Mirren'
    },
    'SP1': {
        'Cadiz CF': 'Cadiz',
        'Atl. Madrid': 'Ath Madrid',
        'Celta Vigo': 'Celta',
        'Real Sociedad': 'Sociedad',
        'Granada CF': 'Granada'
    },
    'SP2': {
        'Rayo Vallecano': 'Vallecano',
        'Gijon': 'Sp Gijon',
        'Espanyol': 'Espanol',
        'R. Oviedo': 'Oviedo'
    },
    'T1': {
        'Basaksehir': 'Buyuksehyr',
        'Goztepe': 'Goztep'
    }
}


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

s3 = boto3.resource('s3')
ses = boto3.client('ses')

s3_historical_prefix = os.environ['S3_PREFIX_HISTORICAL']
to_email_address = os.environ["EMAIL_RECIPIENT"]
from_email_address = os.environ["EMAIL_SENDER"]


def get_team_name(name, div):
    if div in ODDSPORTAL_TEAMS and name in ODDSPORTAL_TEAMS[div]:
        return ODDSPORTAL_TEAMS[div][name]
    else:
        return name


def get_historical_data_from_s3(bucket, league):
    key = s3_historical_prefix.rstrip('/') + '/' + league + '.csv'
    matches_file = s3.Object(bucket, key).get()['Body'].read()
    matches = tablib.Dataset()
    matches.csv = matches_file.decode()
    return matches


def get_fixtures_from_s3(bucket, key):
    # Read the fixtures into CSV
    fixtures_file = s3.Object(bucket, key).get()['Body'].read()
    fixtures = tablib.Dataset()
    fixtures.csv = fixtures_file.decode()
    fixtures = fixtures.sort('Div')
    return fixtures


def get_league_data(data, league):
    league_data = tablib.Dataset()
    [league_data.append(row) for row in data if row[0] == league]
    if type(data) == tablib.Dataset:
        league_data.headers = data.headers
    return league_data


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
