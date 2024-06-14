import csv
from collections import Counter
from datetime import datetime
import pandas as pd

## Original file provided by Jeff Sackman Github 2021
## https://github.com/JeffSackmann/tennis_atp/blob/master/examples/query_player_season_totals.py
## Adjusted/changed for training model by Dan Warnick 2024

## Aggregate the match results in the csv files provided at
## https://github.com/JeffSackmann/tennis_atp and
## https://github.com/JeffSackmann/tennis_wta
## to create "player-season" rate stats, e.g. Ace% for Roger Federer in
## 2015 or SPW% for Sara Errani in 2021.

mw = 'm'            ## 'm' = men, 'w' = women
yrstart = 2022      ## first season to start calculate totals
yrend = 2023        ## last season to calculate totals, will stop at each date
match_min = 10      ## minimum number of matches (with matchstats)
                    ## a player must have to be included for a given year

if mw == 'm':   
    prefix = 'atp'
    input_path = 'csvs/ATP (Mens)/tennis_atp/'
else:
    prefix = 'wta'
    input_path = 'csvs/WTA (Womens)/tennis_wta/'

output_path = 'player_season_totals_' + prefix + '_' + str(yrstart) + '_' + str(yrend) + '.csv'

def convert_date(date_str):
    """
    Converts a date string in the format YYYYMMDD to a datetime object.
    """
    return datetime.strptime(date_str, "%Y%m%d")

header = ['Player', 'Latest_Up', 'Matches', 'Wins', 'Losses', 'Aces', 'DoubleFs', 'Servept', 'Firstin', 'Firstwon', 'Secondwon',
          'w_SvGms', 'w_bpSaved', 'w_bpFaced', 'vaces', 'vdfs', 
          'retpt', 'vfirstin', 'vfirstwon', 'vsecondwon', 'avg_ork']
player_db =[header]

header = ['Player', 'Year', 'Matches', 'Wins', 'Losses', 'Win%',
          'Ace%', 'DF%', '1stIn', '1st%', '2nd%',
          'SPW%', 'RPW%', 'TPW%', 'DomRatio', 'AVGOrk']
player_seasons = [header]

# Placeholder for all match data
all_matches = []

for yr in range(yrstart, yrend + 1):
    matches = [row for row in csv.reader(open(input_path + prefix + '_matches_' + str(yr) + '.csv'))]
    matches = [k for k in matches if 'W' not in k[23] and 'R' not in k[23]]
    all_matches.extend(matches)

# Convert all matches to DataFrame
matches_df = pd.DataFrame(all_matches, columns=[
    'tourney_id', 'tourney_name', 'surface', 'draw_size', 'tourney_level', 'tourney_date', 
    'match_num', 'winner_id', 'winner_seed', 'winner_entry', 'winner_name', 'winner_hand', 
    'winner_ht', 'winner_ioc', 'winner_age', 'winner_rank', 'winner_rank_points', 'loser_id', 
    'loser_seed', 'loser_entry', 'loser_name', 'loser_hand', 'loser_ht', 'loser_ioc', 
    'loser_age', 'loser_rank', 'loser_rank_points', 'score', 'best_of', 'round', 'minutes', 
    'w_ace', 'w_df', 'w_svpt', 'w_1stin', 'w_1stwon', 'w_2ndwon', 'w_sv_gms', 'w_bp_saved', 
    'w_bp_faced', 'l_ace', 'l_df', 'l_svpt', 'l_1stin', 'l_1stwon', 'l_2ndwon', 'l_sv_gms', 
    'l_bp_saved', 'l_bp_faced'
])

# Convert tourney_date to datetime
matches_df['tourney_date'] = pd.to_datetime(matches_df['tourney_date'], format='%Y%m%d')

# Create a combined player stats DataFrame
def get_player_stats(row, player_type):
    if player_type == 'winner':
        return {
            'player_name': row['winner_name'],
            'player_id': row['winner_id'],
            'opponent_name': row['loser_name'],
            'date': row['tourney_date'],
            'aces': int(row['w_ace']),
            'dfs': int(row['w_df']),
            'svpt': int(row['w_svpt']),
            'firstin': int(row['w_1stin']),
            'firstwon': int(row['w_1stwon']),
            'secondwon': int(row['w_2ndwon']),
            'sv_gms': int(row['w_sv_gms']),
            'bp_saved': int(row['w_bp_saved']),
            'bp_faced': int(row['w_bp_faced']),
            'vaces': int(row['l_ace']),
            'vdfs': int(row['l_df']),
            'retpt': int(row['l_svpt']),
            'vfirstin': int(row['l_1stin']),
            'vfirstwon': int(row['l_1stwon']),
            'vsecondwon': int(row['l_2ndwon']),
            'win': 1
        }
    else:
        return {
            'player_name': row['loser_name'],
            'player_id': row['loser_id'],
            'opponent_name': row['winner_name'],
            'date': row['tourney_date'],
            'aces': int(row['l_ace']),
            'dfs': int(row['l_df']),
            'svpt': int(row['l_svpt']),
            'firstin': int(row['l_1stin']),
            'firstwon': int(row['l_1stwon']),
            'secondwon': int(row['l_2ndwon']),
            'sv_gms': int(row['l_sv_gms']),
            'bp_saved': int(row['l_bp_saved']),
            'bp_faced': int(row['l_bp_faced']),
            'vaces': int(row['w_ace']),
            'vdfs': int(row['w_df']),
            'retpt': int(row['w_svpt']),
            'vfirstin': int(row['w_1stin']),
            'vfirstwon': int(row['w_1stwon']),
            'vsecondwon': int(row['w_2ndwon']),
            'win': 0
        }

# Compile all player stats into one DataFrame
player_stats = []

for _, row in matches_df.iterrows():
    player_stats.append(get_player_stats(row, 'winner'))
    player_stats.append(get_player_stats(row, 'loser'))

player_stats_df = pd.DataFrame(player_stats)

# Sort by player_id and date
player_stats_df = player_stats_df.sort_values(by=['player_id', 'date'])

# Calculate rolling averages
def calculate_rolling_averages(group):
    stats = ['aces', 'dfs', 'svpt', 'firstin', 'firstwon', 'secondwon', 
             'sv_gms', 'bp_saved', 'bp_faced', 'vaces', 'vdfs', 
             'retpt', 'vfirstin', 'vfirstwon', 'vsecondwon']
    rolling_averages = group[stats].shift(1).expanding().mean()
    return rolling_averages

# Apply the function to each group
grouped = player_stats_df.groupby('player_id')
rolling_averages = grouped.apply(calculate_rolling_averages).reset_index(drop=True)

# Merge the rolling averages back to the original dataframe
player_stats_df = pd.concat([player_stats_df, rolling_averages.add_suffix('_avg')], axis=1)

# Save the result to a CSV file
player_stats_df.to_csv('csvs/Generated/' + output_path, index=False)
