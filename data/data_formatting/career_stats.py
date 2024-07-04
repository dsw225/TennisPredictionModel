import pandas as pd
from datetime import datetime

## Original file provided by Jeff Sackman Github 2021
## https://github.com/JeffSackmann/tennis_atp/blob/master/examples/query_player_season_totals.py
## Altered for training model by Dan Warnick 2024

def add_c_stats(row, player_stats):
    for k in player_stats:
        if k[0] == row[0]:
            k[1:] = [sum(x) for x in zip(k[1:], row[1:])]
            return
    player_stats.append(row)

def create_career_stats(row, type):
    if type == 'win':
        return [
            row['winner_name'],
            1,  # Match
            1, # Win
            0, # Loss
            1 if row['loser_hand'] == 'L' else 0,
            1 if row['loser_hand'] == 'L' else 0,
            1 if row['loser_hand'] == 'R' else 0,
            1 if row['loser_hand'] == 'R' else 0,

            1 if row['surface'] == 'Hard' else 0,
            1 if row['surface'] == 'Hard' else 0,
            1 if row['surface'] == 'Clay' else 0,
            1 if row['surface'] == 'Clay' else 0,
            1 if row['surface'] == 'Grass' else 0,
            1 if row['surface'] == 'Grass' else 0
        ]
    else:
        return [
            row['loser_name'],
            1,  # Match
            0, # Win
            1, # Loss
            1 if row['winner_hand'] == 'L' else 0,
            0,
            1 if row['winner_hand'] == 'R' else 0,
            0,

            1 if row['surface'] == 'Hard' else 0,
            0,
            1 if row['surface'] == 'Clay' else 0,
            0,
            1 if row['surface'] == 'Grass' else 0,
            0
        ]


def career_stats(date, mw):
    dateend = datetime.strptime(date, "%Y%m%d")
    datestart = datetime(1990, 1, 1) # For now
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
                df.iloc[:, 23].str.contains('R')
            )
        ]
        all_matches.append(df)

    matches_df = pd.concat(all_matches).sort_values(by='tourney_date')

    player_stats = []
    for _, row in matches_df.iterrows():
        add_c_stats(create_career_stats(row, 'win'), player_stats)
        add_c_stats(create_career_stats(row, 'lose'), player_stats)

    new_header = ['Player', 'Matches', 'Wins', 'Losses', 'Vs Lefty Matches', 'Vs Lefty Wins', 'Vs Righty Matches', 'Vs Righty Wins', 
          'Hard Matches', 'Hard Wins', 'Clay Matches', 'Clay Wins', 'Grass Matches', 'Grass Wins']

    player_stats_df = pd.DataFrame(player_stats, columns=new_header)
    player_stats_df = player_stats_df[player_stats_df['Matches']>0] #player_stats_df = player_stats_df[player_stats_df['Matches']>50]
    return player_stats_df

# print(career_stats('20220101','m'))