import breakpoint.rolling_stats as rolling_stats
import career_stats
import pandas as pd
import csv
from random import random
from datetime import datetime

async def create_matchup(match, mw, career_stats):
    winner = match['winner_name']
    loser = match['loser_name']
    whand = match['winner_hand']
    lhand = match['loser_hand']
    date = match['tourney_date']
    surface = match['surface']

    winner_stats = rolling_stats.player_year_to_date(winner, date, mw)
    loser_stats = rolling_stats.player_year_to_date(loser, date, mw)
    winner_career = career_stats[career_stats['Player'] == winner]
    loser_career = career_stats[career_stats['Player'] == loser]

    # Dictionary - change later
    dictionary = {
        'Hard': ('Hard Wins', 'Hard Matches'),
        'Clay': ('Clay Wins', 'Clay Matches'),
        'Grass': ('Grass Wins', 'Grass Matches'),
        'L': ('Vs Lefty Wins', 'Vs Lefty Matches'),
        'R': ('Vs Righty Wins', 'Vs Righty Matches')
    }

    wins, matches = dictionary.get(surface, (None, None))
    w_surface_win_ratio = float(winner_career[wins].iloc[0]) / winner_career[matches].iloc[0] if wins and matches else 0
    l_surface_win_ratio = float(loser_career[wins].iloc[0]) / loser_career[matches].iloc[0] if wins and matches else 0
    whWins, whMatches = dictionary.get(lhand, (None, None))
    lhWins, lhMatches = dictionary.get(whand, (None, None))
    w_hand_win_ratio = float(winner_career[whWins].iloc[0]) / winner_career[whMatches].iloc[0] if whWins and whMatches else 0
    l_hand_win_ratio = float(loser_career[lhWins].iloc[0]) / loser_career[lhMatches].iloc[0] if lhWins and lhMatches else 0

    # Randomize which side wins
    if(round(random()) > 0):
        return [
                date, surface, #Temporary
                winner, loser, w_surface_win_ratio, w_hand_win_ratio,
                *winner_stats.iloc[0, 20:].tolist(),

                l_surface_win_ratio, l_hand_win_ratio,
                *loser_stats.iloc[0, 20:].tolist(),
                
                1 #For 1st player win
        ]
    else:
        return [
                date, surface,
                loser, winner, l_surface_win_ratio, l_hand_win_ratio,
                *loser_stats.iloc[0, 20:].tolist(),


                w_surface_win_ratio, w_hand_win_ratio,
                *winner_stats.iloc[0, 20:].tolist(),
                
                0 #For 1st player loss
        ]






def data_set_creator(yr, mw):
    if mw == 'm':   
        prefix = 'atp'
        input_path = 'csvs/ATP (Mens)/tennis_atp/'
    else:
        prefix = 'wta'
        input_path = 'csvs/WTA (Womens)/tennis_wta/'

    file_path = f"{input_path}{prefix}_matches_{yr}.csv"
    df = pd.read_csv(file_path) # df = pd.read_csv(file_path, parse_dates=['tourney_date'])

    df = df[
            ~(
                df.iloc[:, 23].str.contains('W') | 
                df.iloc[:, 23].str.contains('R') |
                (df.iloc[:, 27:45].isnull().values.any(axis=1))
            )
        ]

    total_career_stats = career_stats.career_stats((str(yr) + "12" + "30"), mw)

    all_matches = []

    for i, row in df.iterrows():
        try:
            all_matches.append(create_matchup(row, mw, total_career_stats))
            print(str(i) + " games processed")
        except Exception as e:
            print(f"An error occurred: {e}")
            pass
        
    new_header = [
        'date', 'surface', 'player_one', 'p1_surface_win_ratio', 'p1_hand_win_ratio', 'aces_p1_avg', 'dfs_p1_avg', 'svpt_p1_avg', 'firstin_p1_avg', 'firstwon_p1_avg', 
        'secondwon_p1_avg', 'sv_gms_p1_avg', 'bp_saved_p1_avg', 'bp_faced_p1_avg', 'vaces_p1_avg', 'vdfs_p1_avg', 'retpt_p1_avg', 'vfirstin_p1_avg', 
        'vfirstwon_p1_avg', 'vsecondwon_p1_avg', 'win_p1_avg',                  'player_two', 'p2_surface_win_ratio', 'p2_hand_win_ratio', 'aces_p2_avg', 'dfs_p2_avg', 'svpt_p2_avg', 'firstin_p2_avg', 'firstwon_p2_avg', 
        'secondwon_p2_avg', 'sv_gms_p2_avg', 'bp_saved_p2_avg', 'bp_faced_p2_avg', 'vaces_p2_avg', 'vdfs_p2_avg', 'retpt_p2_avg', 'vfirstin_p2_avg', 
        'vfirstwon_p2_avg', 'vsecondwon_p2_avg', 'win_p2_avg', 'p1_win?'
    ]

    player_stats_df = pd.DataFrame(all_matches, columns=new_header)

    output_path = 'stats_' + prefix + '_' + str(yr) + '.csv'

    player_stats_df.to_csv('csvs/Generated/' + output_path, index=False)
    print("Done")
