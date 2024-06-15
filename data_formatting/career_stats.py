import csv
from collections import Counter

## Original file provided by Jeff Sackman Github 2021
## https://github.com/JeffSackmann/tennis_atp/blob/master/examples/query_player_season_totals.py
## Altered for training model by Dan Warnick 2024

mw = '2'            ## 'm' = men, 'w' = women
yrend = 2023        ## last season to calculate totals
match_min = 40      ## minimum number of matches (with matchstats)
                    ## a player must have to be included for a given year

if mw == 'm':   
    prefix = 'atp'
    input_path = 'csvs/ATP (Mens)/tennis_atp/'
    yrstart = 1995 ## first season to calculate totals
else:
    prefix = 'wta'
    input_path = 'csvs/WTA (Womens)/tennis_wta/'
    yrstart = 1995 ## first season to calculate totals - NOTE WTA game stats began in 2016, however longest active player is - Venus 1994

output_path = 'player_career_totals_' + prefix + '_' + str(yrstart) + '_' + str(yrend) + '.csv'

header = ['Player', 'Matches', 'Wins', 'Losses', 'Vs Lefty Matches', 'Vs Lefty Wins', 'Vs Righty Matches', 'Vs Right Wins', 
          'Hard Matches', 'Hard Wins', 'Clay Matches', 'Clay Wins', 'Grass Matches', 'Grass Wins']
player_seasons = [header]

#Add stats so no duplicates
def add_stats(row):
    for k in player_seasons:
        if k[0] == row[0]:
            k[1:] = [sum(x) for x in zip(k[1:], row[1:])]
            return
    player_seasons.append(row)

for yr in range(yrstart, yrend + 1):
    ## load one year of match results
    matches = [row for row in csv.reader(open(input_path + prefix + '_matches_' + str(yr) + '.csv'))]
    ## exclude incomplete/unplayed matches (e.g. "W/O" or "RET" in score]
    matches = [k for k in matches if 'W' not in k[23] and 'R' not in k[23]]
    ## make list of all players with a result
    players = [k[10] for k in matches] + [k[18] for k in matches]
    ## limit list of players to those with at least match_min matches
    ## For now we will leave with no max, will adjust after
    qualifs = [k for k, v in Counter(players).items()] #Change for python3 iteritems() -> items(0)
    # qualifs = [k for k, v in Counter(players).items() if v >= match_min] #Change for python3 iteritems() -> items(0)

    for pl in qualifs:
        ## find all of the players matches
        pmatches = [k for k in matches if pl in [k[10], k[18]]]
        ## make matrix of their stats (different columns depending if they won or lost)

        match_count = len(pmatches)

        # Initialize counters
        clay_matches = clay_wins = 0
        hard_matches = hard_wins = 0
        grass_matches = grass_wins = 0
        vs_L_matches = vs_L_wins = 0
        vs_R_matches = vs_R_wins = 0
        wins = 0

        # Iterate through matches once
        for k in pmatches:
            if k[10] == pl:
                wins += 1
                if k[19] == 'L': 
                    vs_L_wins += 1
                    vs_L_matches += 1
                elif k[19] == 'R': # Some data has unknown hand so we must check for right
                    vs_R_wins += 1
                    vs_R_matches += 1
            else:
                vs_L_matches += 1 if k[11] == 'L' else 0
                vs_R_matches += 1 if k[11] == 'R' else 0
                    
            if k[2] == 'Clay':
                clay_matches += 1
                if k[10] == pl:
                    clay_wins += 1
            elif k[2] == 'Hard':
                hard_matches += 1
                if k[10] == pl:
                    hard_wins += 1
            elif k[2] == 'Grass':
                grass_matches += 1
                if k[10] == pl:
                    grass_wins += 1

        losses = match_count - wins
        
        row = [pl, match_count, wins, losses, vs_L_matches,
               vs_L_wins, vs_R_matches, vs_R_wins, hard_matches, hard_wins,
               clay_matches, clay_wins, grass_matches, grass_wins]
        add_stats(row)

header = ['Player', 'Matches', 'Hand Preference', 'Hard +/- %', 'Clay +/- %', 'Grass +/- %']
player_career_ratios = [header]

for k in player_seasons:
    if k[0] == 'Player' or k[1]<match_min or k[2]<1:  # Skip header row and not met matches or any wins
        continue
    # Calculate factors
    (pl, match_count, wins, losses, vs_L_matches, 
    vs_L_wins, vs_R_matches, vs_R_wins, hard_matches, hard_wins, 
    clay_matches, clay_wins, grass_matches, grass_wins) = k

    match_count = int(match_count)
    wins = int(wins)
    losses = int(losses)
    vs_L_matches = int(vs_L_matches)
    vs_L_wins = int(vs_L_wins)
    vs_R_matches = int(vs_R_matches)
    vs_R_wins = int(vs_R_wins)
    hard_matches = int(hard_matches)
    hard_wins = int(hard_wins)
    clay_matches = int(clay_matches)
    clay_wins = int(clay_wins)
    grass_matches = int(grass_matches)
    grass_wins = int(grass_wins)

    win_perc = wins / float(match_count) if match_count > 0 else 0

    # Surface Specialization
    clay_factor = (clay_wins / float(clay_matches)) / win_perc - 1 if clay_matches >= 1 else 0
    hard_factor = (hard_wins / float(hard_matches)) / win_perc - 1 if hard_matches >= 1 else 0
    grass_factor = (grass_wins / float(grass_matches)) / win_perc - 1 if grass_matches >= 1 else 0
    lefty_factor = (vs_L_wins / float(vs_L_matches)) if vs_L_matches >= 1 else -1
    righty_factor = (vs_R_wins / float(vs_R_matches)) if vs_R_matches >= 1 else -1
    # lefty_factor = ((vs_L_wins / float(vs_L_matches)) - win_perc) / win_perc if vs_L_matches >= 1 else 0
    # righty_factor = ((vs_R_wins / float(vs_R_matches)) - win_perc) / win_perc if vs_R_matches >= 1 else 0
    preference_r_l = righty_factor-lefty_factor if vs_L_matches >= 30 and vs_R_matches >= 30 else 0
    row = [pl, match_count, preference_r_l, hard_factor, clay_factor, grass_factor]

    player_career_ratios.append(row)

with open('csvs/Generated/' + output_path, 'w', newline='') as results:
    writer = csv.writer(results)
    for row in player_career_ratios:
        writer.writerow(row)



