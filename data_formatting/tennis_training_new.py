import csv
from collections import Counter
from datetime import datetime

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
    # Convert to datetime object
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    # Convert to readable format for now
    date_obj = date_obj.strftime("%B %d, %Y") 
    return date_obj

header = ['Player', 'Latest_Up', 'Matches', 'Wins', 'Losses', 'Aces', 'DoubleFs', 'Servept', 'Firstin', 'Firstwon', 'Secondwon',
          'w_SvGms', 'w_bpSaved', 'w_bpFaced', 'vaces', 'vdfs', 
               'retpt', 'vfirstin', 'vfirstwon', 'vsecondwon', 'avg_ork']
player_db =[header]

header = ['Player', 'Year', 'Matches', 'Wins', 'Losses', 'Win%',
          'Ace%', 'DF%', '1stIn', '1st%', '2nd%',
          'SPW%', 'RPW%', 'TPW%', 'DomRatio', 'AVGOrk']
player_seasons = [header]

def add_stats(row, start):
    """
    Combines arrays at start
    """
    for k in player_db:
        if k[0] == row[0]:
            k[start:] = [sum(x) for x in zip(k[start:], row[start:])]
            k[1] = row[1]
            return
    player_db.append(row)

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
        wins = 0     
        
        avg_ork = 0
        missing_rk = 0

        
        for k in pmatches:
            date = convert_date(k[5])
            if k[10] == pl:
                wins += 1
                if k[45].isdigit():
                    avg_ork += int(k[45])
                else:
                    missing_rk += 1
            else:
                if k[47].isdigit():
                    avg_ork += int(k[47])
                else:
                    missing_rk += 1

        # Remove missing ranks from data
        # avg_ork = (avg_ork) / float(match_count-missing_rk)
        losses = match_count - wins
        # win_perc = wins / float(match_count)   

        ## readable names for serve stats
        aces, dfs, svpt, firstin, firstwon, secondwon, w_SvGms, w_bpSaved, w_bpFaced, vaces, vdfs, retpt, vfirstin, vfirstwon, vsecondwon= sum_row[1:16]
        ## calculate common rate stats
        # ace_rate = aces / float(svpt)
        # df_rate = dfs / float(svpt)
        # firstin_rate = firstin / float(svpt)
        # first_win = firstwon / float(firstin)
        # second_win = secondwon / (svpt - float(firstin))
        # spw = firstwon + secondwon
        # spw_rate = spw / float(svpt)

        # ## raw return stats
        # vaces, vdfs, retpt, vfirstin, vfirstwon, vsecondwon = sum_row[10:16]
        # ## calculate more aggregates
        # rpw = retpt - vfirstwon - vsecondwon
        # rpw_rate = rpw / float(retpt)

        ## STATS TO ADD / CONSIDER
        ## SETS % GAMES % HOLD % BRK % TB W %
        ## Serve + Return Rating and add player rank
        ## Maybe add per match stats ie. aces/match

        # tpw_rate = (spw + rpw) / (float(svpt) + retpt)
        # dom_ratio = rpw_rate / (1 - spw_rate)

        row = [pl, date, match_count, wins, losses, aces, dfs, svpt, firstin, 
               firstwon, secondwon, w_SvGms, w_bpSaved, w_bpFaced, vaces, vdfs, 
               retpt, vfirstin, vfirstwon, vsecondwon, avg_ork]
        add_stats(row, 2)

with open('csvs/Generated/' + output_path, 'w', newline='') as results:
    writer = csv.writer(results)
    for row in player_db:
        writer.writerow(row)
