# A mapping from oddsportal.com leagues to football-data.co.uk's 'Div'
ODDSPORTAL_DIVISIONS = {
    ('England', 'Premier League'): 'E0',
    # ('England', 'Championship'): 'E1',
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
    ('Sweden', 'Allsvenskan'): 'SWE',
    ('Switzerland', 'Super League'): 'SWZ',
    ('Turkey', 'Super Lig'): 'T1',
    ('China', 'Super League'): 'CHN', # check
    ('Japan', 'J1 League'): 'JPN' # check
}

# Mapping of oddsportal team names to football-data teams
ODDSPORTAL_TEAMS = {
    'B1': {
        'KV Mechelen': 'Mechelen',
        'Cercle Brugge KSV': 'Cercle Brugge'
    },
    'E0': {
        'Manchester Utd': 'Man United',
        'Sheffield Utd': 'Sheffield United',
        'Manchester City': 'Man City'
    },
    'E1': {
        'Sheffield Wed': 'Sheffield Weds'
    },
    'EC': {
        'Solihull Moors': 'Solihull',
        'Dag & Red': 'Dag and Red',
        'Dover': 'Dover Athletic',
        'Notts Co': 'Notts County',
        'FC Halifax': 'Halifax'
    },
    'E2': {
        'Peterborough': 'Peterboro',
        'Bristol Rovers': 'Bristol Rvs',
        'Oxford Utd': 'Oxford'
    },
    'E3': {
        'Crawley': 'Crawley Town',
        'Cambridge Utd': 'Cambridge',
        'Newport': 'Newport County'
    },
    'F2': {
        'AC Ajaccio': 'Ajaccio'
    },
    'D1': {
        'Arminia Bielefeld': 'Bielefeld',
        'B. Monchengladbach': "M'gladbach",
        'Eintracht Frankfurt': 'Ein Frankfurt',
        'Schalke': 'Schalke 04'
    },
    'G1': {
        'AEL Larissa': 'Larisa',
        'AEK Athens FC': 'AEK',
        'Smyrnis': 'Apollon'
    },
    'N1': {
        'Venlo': 'VVV Venlo',
        'Sittard': 'For Sittard'
    },
    'P1': {
        'Sporting': 'Sp Lisbon',
        'Ferreira': 'Pacos Ferreira',
        'Vitoria Guimaraes': 'Guimaraes',
        'SC Farense': 'Farense'
    },
    'ROU': {
        'Univ. Craiova': 'U Craiova',
        'FCSB': 'FC Steaua Bucuresti'
    },
    'SP1': {
        'Cadiz CF': 'Cadiz',
        'Atl. Madrid': 'Ath Madrid',
        'Celta Vigo': 'Celta'
    },
    'T1': {
        'Basaksehir': 'Buyuksehyr'
    }
}


def get_team_name(name, div):
    if div in ODDSPORTAL_TEAMS and name in ODDSPORTAL_TEAMS[div]:
        return ODDSPORTAL_TEAMS[div][name]
    else:
        return name
