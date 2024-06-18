import csv
from collections import Counter
from datetime import datetime
import pandas as pd

## Creates rolling stats/averages for one player for one year
## Inspired by Jeff Sackman 2021, Dan Warnick 2024
## https://github.com/JeffSackmann/tennis_atp/blob/master/examples/query_player_season_totals.py

## Aggregate the match results in the csv files provided at
## https://github.com/JeffSackmann/tennis_atp and
## https://github.com/JeffSackmann/tennis_wta]

#Params

def add_stats(row, player_stats, player_name):
    if row[0] == player_name:
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

# Calculate rolling averages
def calculate_rolling_averages(df):
    df_copy = df.copy()
    
    # Specify the range of columns to normalize (3:-2) i.e., from the 4th to the 2nd last column
    columns_to_normalize = df_copy.columns[3:-1]
    
    # Normalize each specified column by the 'matches' column
    for column in columns_to_normalize:
        df_copy[column + '_avg'] = df_copy[column] / df_copy['matches']
    
    return df_copy

def player_year_to_date(player_one, date, mw):
    dateend = datetime.strptime(date, "%Y%m%d")
    datestart = datetime(dateend.year-1, dateend.month, dateend.day)
    match_min = 10      ## minimum number of matches (with matchstats) ADD LATER


    if mw == 'm':   
        prefix = 'atp'
        input_path = 'csvs/ATP (Mens)/tennis_atp/'
    else:
        prefix = 'wta'
        input_path = 'csvs/WTA (Womens)/tennis_wta/'

    yrstart = datestart.year      ## first season to start calculate totals
    yrend = dateend.year        ## last season to calculate totals, will stop at each date

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
            
            matches = []
            for row in reader:
                if 'W' in row[23] or 'R' in row[23] or '' in row[27:45] or (player_one != row[10] and player_one != row[18]):
                    continue
                date = datetime.strptime(row[5], "%Y%m%d")
                if datestart < date < dateend:
                    matches.append(row[:5] + [date] + row[6:])

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

    player_stats = []

    for _, row in matches_df.iloc[::-1].iterrows():
        add_stats(create_individual_stats(row, 'win'), player_stats, player_one)
        add_stats(create_individual_stats(row, 'lose'), player_stats, player_one)

    new_header = [
        'player_name', 'player_id', 'tourney_date', 'aces', 'dfs', 'svpt', 'firstin', 'firstwon', 'secondwon', 
            'sv_gms', 'bp_saved', 'bp_faced', 'vaces', 'vdfs', 
            'retpt', 'vfirstin', 'vfirstwon', 'vsecondwon', 'win', 'matches'
    ]

    player_stats_df = pd.DataFrame(player_stats, columns=new_header)


    # Return results
    return calculate_rolling_averages(player_stats_df)

print(player_year_to_date("Novak Djokovic", "20220524", "m"))