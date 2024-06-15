import csv
from collections import Counter
from datetime import datetime
import pandas as pd

## Original file provided by Jeff Sackman Github 2021
## https://github.com/JeffSackmann/tennis_atp/blob/master/examples/query_player_season_totals.py
## Adjusted/changed for training model by Dan Warnick 2024

## Aggregate the match results in the csv files provided at
## https://github.com/JeffSackmann/tennis_atp and
## https://github.com/JeffSackmann/tennis_wta]

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

output_path = 'player_temp_totals_' + prefix + '_' + str(yrstart) + '_' + str(yrend) + '.csv'

def convert_date(date_str):
    return datetime.strptime(date_str, "%Y%m%d")

# header = ['Player', 'Latest_Up', 'Matches', 'Wins', 'Losses', 'Aces', 'DoubleFs', 'Servept', 'Firstin', 'Firstwon', 'Secondwon',
#           'w_SvGms', 'w_bpSaved', 'w_bpFaced', 'vaces', 'vdfs', 
#           'retpt', 'vfirstin', 'vfirstwon', 'vsecondwon', 'avg_ork']
# player_db =[header]

# header = ['Player', 'Year', 'Matches', 'Wins', 'Losses', 'Win%',
#           'Ace%', 'DF%', '1stIn', '1st%', '2nd%',
#           'SPW%', 'RPW%', 'TPW%', 'DomRatio', 'AVGOrk']
# player_seasons = [header]

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

player_stats = pd.DataFrame()
def create_individual_stats(row, type):
    if(type=='win'):
        return {
            'player_name': row['winner_name'],
            'player_id': row['winner_id'],
            'opponent_name': row['loser_name'],
            'date': row['tourney_date'],
            'aces': int(row['w_ace']),
            'dfs': int(row['w_df']),
            'svpt': int(row['w_svpt']),
            'firstin': int(row['w_1stIn']),
            'firstwon': int(row['w_1stWon']),
            'secondwon': int(row['w_2ndWon']),
            'sv_gms': int(row['w_SvGms']),
            'bp_saved': int(row['w_bpSaved']),
            'bp_faced': int(row['w_bpFaced']),
            'vaces': int(row['l_ace']),
            'vdfs': int(row['l_df']),
            'retpt': int(row['l_svpt']),
            'vfirstin': int(row['l_1stIn']),
            'vfirstwon': int(row['l_1stWon']),
            'vsecondwon': int(row['l_2ndWon']),
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
            'firstin': int(row['l_1stIn']),
            'firstwon': int(row['l_1stWon']),
            'secondwon': int(row['l_2ndWon']),
            'sv_gms': int(row['l_SvGms']),
            'bp_saved': int(row['l_bpSaved']),
            'bp_faced': int(row['l_bpFaced']),
            'vaces': int(row['w_ace']),
            'vdfs': int(row['w_df']),
            'retpt': int(row['w_svpt']),
            'vfirstin': int(row['w_1stIn']),
            'vfirstwon': int(row['w_1stWon']),
            'vsecondwon': int(row['w_2ndWon']),
            'win': 0
        }

player_stats = []

for _, row in matches_df.iterrows():
    player_stats.append(create_individual_stats(row, 'win'))
    player_stats.append(create_individual_stats(row, 'lose'))

# print(player_stats[4])

player_stats_df = pd.DataFrame(player_stats)

# Sort by player_id and date
player_stats_df = player_stats_df.sort_values(by=['player_name', 'date'])

# Calculate rolling averages
def calculate_rolling_averages(group):
    stats = ['aces', 'dfs', 'svpt', 'firstin', 'firstwon', 'secondwon', 
             'sv_gms', 'bp_saved', 'bp_faced', 'vaces', 'vdfs', 
             'retpt', 'vfirstin', 'vfirstwon', 'vsecondwon']
    rolling_averages = group[stats].shift(1).expanding().mean()
    return rolling_averages

# Apply the function to each group
grouped = player_stats_df.groupby('player_name')
rolling_averages = grouped.apply(calculate_rolling_averages).reset_index(drop=True)

# Merge the rolling averages back to the original dataframe
player_stats_df = pd.concat([player_stats_df, rolling_averages.add_suffix('_avg')], axis=1)

# Save the result to a CSV file
player_stats_df.to_csv('csvs/Generated/' + output_path, index=False)
