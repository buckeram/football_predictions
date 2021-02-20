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
        'Schalke': 'Schalke 04',
        'Bayer Leverkusen': 'Leverkusen',
        'Hertha Berlin': 'Hertha',

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
        'SC Farense': 'Farense',
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
    'T1': {
        'Basaksehir': 'Buyuksehyr',
        'Goztepe': 'Goztep'
    }
}


def get_team_name(name, div):
    if div in ODDSPORTAL_TEAMS and name in ODDSPORTAL_TEAMS[div]:
        return ODDSPORTAL_TEAMS[div][name]
    else:
        return name
