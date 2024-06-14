import csv
from collections import Counter

## Original file provided by Jeff Sackman Github 2021
## https://github.com/JeffSackmann/tennis_atp/blob/master/examples/query_player_season_totals.py
## Adjusted/changed for training model by Dan Warnick 2024

## Aggregate the match results in the csv files provided at
## https://github.com/JeffSackmann/tennis_atp and
## https://github.com/JeffSackmann/tennis_wta
## to create "player-season" rate stats, e.g. Ace% for Roger Federer in
## 2015 or SPW% for Sara Errani in 2021.

mw = 'm'            ## 'm' = men, 'w' = women
yrstart = 2023      ## first season to calculate totals
yrend = 2023        ## last season to calculate totals
match_min = 10      ## minimum number of matches (with matchstats)
                    ## a player must have to be included for a given year

if mw == 'm':   
    prefix = 'atp'
    input_path = 'csvs/ATP (Mens)/tennis_atp/'
else:
    prefix = 'wta'
    input_path = 'csvs/WTA (Womens)/tennis_wta/'

output_path = 'player_season_totals_' + prefix + '_' + str(yrstart) + '_' + str(yrend) + '.csv'

header = ['Player', 'Year', 'Matches', 'Wins', 'Losses', 'Win%',
          'Ace%', 'DF%', '1stIn', '1st%', '2nd%',
          'SPW%', 'RPW%', 'TPW%', 'DomRatio', 'Hard +/- %', 'Clay +/- %', 'Grass +/- %']
player_seasons = [header]

for yr in range(yrstart, yrend + 1):
    ## load one year of match results
    matches = [row for row in csv.reader(open(input_path + prefix + '_matches_' + str(yr) + '.csv'))]
    ## exclude incomplete/unplayed matches (e.g. "W/O" or "RET" in score]
    matches = [k for k in matches if 'W' not in k[23] and 'R' not in k[23]]
    ## exclude matches without stats
    # matches = [k for k in matches if '' not in [k[27], k[36]]]
    ## make list of all players with a result
    players = [k[10] for k in matches] + [k[18] for k in matches]
    ## limit list of players to those with at least match_min matches
    qualifs = [k for k, v in Counter(players).items() if v >= match_min] #Change for python3 iteritems() -> items(0)
    
    #Changed Name for ID first

    for pl in qualifs:
        ## find all of the players matches
        pmatches = [k for k in matches if pl in [k[10], k[18]]]
        ## make matrix of their stats (different columns depending if they won or lost)
        pstats = [k[27:36] + k[36:45] if pl == k[10] else k[36:45] + k[27:36] for k in pmatches]
        # pdata = [k[10] if pl == k[7] else k[18] for k in pmatches]
        ## make row for their aggregate counting stats, starting with number of matches [with stats]
        sum_row = [len(pstats)]
        for i in range(len(pstats[0])):
            this_stat = sum([int(k[i]) for k in pstats if k[i].isdigit()])
            sum_row.append(this_stat)
        ## calculate aggregates
        match_count = len(pmatches)
        wins = len([k for k in pmatches if k[10] == pl])
        losses = match_count - wins
        win_perc = wins / float(match_count)        
        
        # surface stat
        # Initialize counters
        clay_matches = clay_wins = 0
        hard_matches = hard_wins = 0
        grass_matches = grass_wins = 0

        # Iterate through matches once
        for k in pmatches:
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

        # Calculate factors
        clay_factor = (clay_wins / float(clay_matches)) / win_perc - 1 if clay_matches >= 5 else 0
        hard_factor = (hard_wins / float(hard_matches)) / win_perc - 1 if hard_matches >= 5 else 0
        grass_factor = (grass_wins / float(grass_matches)) / win_perc - 1 if grass_matches >= 5 else 0


        ## readable names for serve stats
        aces, dfs, svpt, firstin, firstwon, secondwon = sum_row[1:7]
        ## calculate common rate stats
        ace_rate = aces / float(svpt)
        df_rate = dfs / float(svpt)
        firstin_rate = firstin / float(svpt)
        first_win = firstwon / float(firstin)
        second_win = secondwon / (svpt - float(firstin))
        spw = firstwon + secondwon
        spw_rate = spw / float(svpt)

        ## raw return stats
        vaces, vdfs, retpt, vfirstin, vfirstwon, vsecondwon = sum_row[10:16]
        ## calculate more aggregates
        rpw = retpt - vfirstwon - vsecondwon
        rpw_rate = rpw / float(retpt)

        tpw_rate = (spw + rpw) / (float(svpt) + retpt)
        dom_ratio = rpw_rate / (1 - spw_rate)

        row = [pl, yr, match_count, wins, losses, win_perc,
               ace_rate, df_rate, firstin_rate, first_win, second_win,
               spw_rate, rpw_rate, tpw_rate, dom_ratio, hard_factor, clay_factor, grass_factor]
        player_seasons.append(row)

with open('csvs/Generated/' + output_path, 'w', newline='') as results:
    writer = csv.writer(results)
    for row in player_seasons:
        writer.writerow(row)
