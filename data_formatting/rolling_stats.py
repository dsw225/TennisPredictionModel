import csv
from collections import Counter
from datetime import datetime
import pandas as pd

## Creates rolling stats/averages for players for any year gap
## Inspired by Jeff Sackman 2021, Dan Warnick 2024
## https://github.com/JeffSackmann/tennis_atp/blob/master/examples/query_player_season_totals.py

## Aggregate the match results in the csv files provided at
## https://github.com/JeffSackmann/tennis_atp and
## https://github.com/JeffSackmann/tennis_wta]

mw = 'm'            ## 'm' = men, 'w' = women
yrstart = 2020      ## first season to start calculate totals
yrend = 2023        ## last season to calculate totals, will stop at each date
match_min = 10      ## minimum number of matches (with matchstats) ADD LATER

if mw == 'm':   
    prefix = 'atp'
    input_path = 'csvs/ATP (Mens)/tennis_atp/'
else:
    prefix = 'wta'
    input_path = 'csvs/WTA (Womens)/tennis_wta/'

output_path = 'player_rolling_totals_' + prefix + '_' + str(yrstart) + '_' + str(yrend) + '.csv'

def convert_date(date_str):
    return datetime.strptime(date_str, "%Y%m%d")

# Placeholder for all match data
all_matches = []

# Loop through the years in the specified range
for yr in range(yrstart, yrend + 1):
    # Construct the file path for the current year
    file_path = f"{input_path}{prefix}_matches_{yr}.csv"
    
    # Open and read the CSV file
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        
        next(reader, None)
        
        matches = [k[:5] + [datetime.strptime(k[5], "%Y%m%d")] + k[6:] for k in reader if 'W' not in k[23] and 'R' not in k[23] and all(k[i] for i in range(27, 45))]
        ##Below is the function above but less efficient
        # matches = [row for row in reader if 'W' not in row[23] and 'R' not in row[23]]

        # matches = [k for k in matches if '' not in k[27:45]] 

        # matches = [k[:5] + [datetime.strptime(k[5], "%Y%m%d")] + k[6:] for k in matches]
        
        all_matches.append(matches)

concatenated_matches = [match for year_matches in all_matches for match in year_matches]

matches_df = pd.DataFrame(concatenated_matches, columns=[
    'tourney_id','tourney_name','surface','draw_size',
    'tourney_level','tourney_date','match_num','winner_id','winner_seed',
    'winner_entry','winner_name','winner_hand','winner_ht','winner_ioc',
    'winner_age','loser_id','loser_seed','loser_entry','loser_name','loser_hand',
    'loser_ht','loser_ioc','loser_age','score','best_of','round','minutes','w_ace',
    'w_df','w_svpt','w_1stIn','w_1stWon','w_2ndWon','w_SvGms','w_bpSaved','w_bpFaced',
    'l_ace','l_df','l_svpt','l_1stIn','l_1stWon','l_2ndWon','l_SvGms','l_bpSaved',
    'l_bpFaced','winner_rank','winner_rank_points','loser_rank','loser_rank_points'

])

matches_df = matches_df.sort_values(by=['tourney_date'])

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

player_stats = []

def add_stats(row):
    temp = datetime(1900, 1, 1)
    for k in reversed(player_stats):
        if k[0] == row[0]:
            row[3:] = [sum(x) for x in zip(k[3:], row[3:])]
            player_stats.append(row)
            return
    player_stats.append(row)

for _, row in matches_df.iterrows():
    add_stats(create_individual_stats(row, 'win'))
    add_stats(create_individual_stats(row, 'lose'))

new_header = [
    'player_name', 'player_id', 'tourney_date', 'aces', 'dfs', 'svpt', 'firstin', 'firstwon', 'secondwon', 
        'sv_gms', 'bp_saved', 'bp_faced', 'vaces', 'vdfs', 
        'retpt', 'vfirstin', 'vfirstwon', 'vsecondwon', 'win', 'matches'
]

player_stats_df = pd.DataFrame(player_stats, columns=new_header)

# Sort by player_id and date
player_stats_df = player_stats_df.sort_values(by=['player_name', 'tourney_date'])

# Calculate rolling averages
def calculate_rolling_averages(df):
    df_copy = df.copy()
    
    # Specify the range of columns to normalize (3:-2) i.e., from the 4th to the 2nd last column
    columns_to_normalize = df_copy.columns[3:-1]
    
    # Normalize each specified column by the 'matches' column
    for column in columns_to_normalize:
        df_copy[column + '_avg'] = df_copy[column] / df_copy['matches']
    
    return df_copy

# Save the result to a CSV file
calculate_rolling_averages(player_stats_df).to_csv('csvs/Generated/' + output_path, index=False)
