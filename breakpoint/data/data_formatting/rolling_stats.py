import pandas as pd
from datetime import datetime

def add_stats(row, player_stats):
    for k in player_stats:
        if k[0] == row[0]:
            k[3:] = [sum(x) for x in zip(k[3:], row[3:])]
            return
    player_stats.append(row)

def create_individual_stats(row, type):
    if type == 'win':
        return [
            row['winner_name'],
            row['winner_id'],
            row['tourney_date'],
            int(row['w_ace']),
            int(row['w_df']),
            int(row['w_svpt']),
            int(row['w_1stIn']),
            int(row['w_1stWon']),
            int(row['w_2ndWon']),
            int(row['w_SvGms']),
            int(row['w_bpSaved']),
            int(row['w_bpFaced']),
            int(row['l_ace']),
            int(row['l_df']),
            int(row['l_svpt']),
            int(row['l_1stIn']),
            int(row['l_1stWon']),
            int(row['l_2ndWon']),
            1,  # win indicator
            1 # Match
        ]
    else:
        return [
            row['loser_name'],
            row['loser_id'],
            row['tourney_date'],
            int(row['l_ace']),
            int(row['l_df']),
            int(row['l_svpt']),
            int(row['l_1stIn']),
            int(row['l_1stWon']),
            int(row['l_2ndWon']),
            int(row['l_SvGms']),
            int(row['l_bpSaved']),
            int(row['l_bpFaced']),
            int(row['w_ace']),
            int(row['w_df']),
            int(row['w_svpt']),
            int(row['w_1stIn']),
            int(row['w_1stWon']),
            int(row['w_2ndWon']),
            0,  # loss indicator
            1 # Match
        ]

def calculate_rolling_averages(df):
    columns_to_normalize = df.columns[3:-1]
    for column in columns_to_normalize:
        df[column + '_avg'] = df[column] / df['matches']
    return df

def player_year_to_date(player_one, date, mw):
    dateend = datetime.strptime(str(date), "%Y%m%d")
    datestart = datetime(dateend.year-1, dateend.month, dateend.day)
    if mw == 'm':   
        prefix = 'atp'
        input_path = 'csvs/ATP (Mens)/tennis_atp/'
    else:
        prefix = 'wta'
        input_path = 'csvs/WTA (Womens)/tennis_wta/'

    all_matches = []

    for yr in range(datestart.year, dateend.year+1):
        file_path = f"{input_path}{prefix}_matches_{yr}.csv"
        df = pd.read_csv(file_path, parse_dates=['tourney_date'])

        # Filter relevant matches
        df = df[
            ((df['winner_name'] == player_one) | (df['loser_name'] == player_one)) & 
            (df['tourney_date'] > datestart) & 
            (df['tourney_date'] < dateend) & 
            ~(
                df.iloc[:, 23].str.contains('W') | 
                df.iloc[:, 23].str.contains('R') |
                (df.iloc[:, 27:45].isnull().values.any(axis=1))
            )
        ]
        all_matches.append(df)

    matches_df = pd.concat(all_matches).sort_values(by='tourney_date')

    player_stats = []
    for _, row in matches_df.iterrows():
        if row['winner_name'] == player_one:
            add_stats(create_individual_stats(row, 'win'), player_stats)
        if row['loser_name'] == player_one:
            add_stats(create_individual_stats(row, 'lose'),player_stats)

    new_header = [
        'player_name', 'player_id', 'tourney_date', 'aces', 'dfs', 'svpt', 'firstin', 'firstwon', 
        'secondwon', 'sv_gms', 'bp_saved', 'bp_faced', 'vaces', 'vdfs', 'retpt', 'vfirstin', 
        'vfirstwon', 'vsecondwon', 'win', 'matches'
    ]

    player_stats_df = pd.DataFrame(player_stats, columns=new_header)
    return calculate_rolling_averages(player_stats_df)


def all_year_to_date(date, mw):
    dateend = datetime.strptime(str(date), "%Y%m%d")
    datestart = datetime(dateend.year-1, dateend.month, dateend.day)
    if mw == 'm':   
        prefix = 'atp'
        input_path = 'csvs/ATP (Mens)/tennis_atp/'
    else:
        prefix = 'wta'
        input_path = 'csvs/WTA (Womens)/tennis_wta/'

    all_matches = []

    for yr in range(datestart.year, dateend.year+1):
        file_path = f"{input_path}{prefix}_matches_{yr}.csv"
        df = pd.read_csv(file_path, parse_dates=['tourney_date'])

        # Filter relevant matches
        df = df[
            (df['tourney_date'] > datestart) & 
            (df['tourney_date'] < dateend) & 
            ~(
                df.iloc[:, 23].str.contains('W') | 
                df.iloc[:, 23].str.contains('R') |
                (df.iloc[:, 27:45].isnull().values.any(axis=1))
            )
        ]
        all_matches.append(df)

    matches_df = pd.concat(all_matches).sort_values(by='tourney_date')

    player_stats = []
    for _, row in matches_df.iterrows():
        add_stats(create_individual_stats(row, 'win'), player_stats)
        add_stats(create_individual_stats(row, 'lose'),player_stats)

    new_header = [
        'player_name', 'player_id', 'tourney_date', 'aces', 'dfs', 'svpt', 'firstin', 'firstwon', 
        'secondwon', 'sv_gms', 'bp_saved', 'bp_faced', 'vaces', 'vdfs', 'retpt', 'vfirstin', 
        'vfirstwon', 'vsecondwon', 'win', 'matches'
    ]

    player_stats_df = pd.DataFrame(player_stats, columns=new_header)
    return calculate_rolling_averages(player_stats_df)

# Example usage
# print(player_year_to_date("Novak Djokovic", "20220524", "m"))
# print(all_year_to_date("20220524", "m"))
