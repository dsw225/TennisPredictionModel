import pandas as pd
import aiofiles
import asyncio
from datetime import datetime, timedelta
from math import pow, copysign, floor, ceil
import traceback
import numpy as np

START_RATING = 1500
RATING_SCALE = 480.0
K_FACTOR = 32.0
K_FUNCTION_AMPLIFIER = 10.0
K_FUNCTION_AMPLIFIER_GRADIENT = 63.0
K_FUNCTION_MULTIPLIER = 2.0 * (K_FUNCTION_AMPLIFIER - 1.0)

DELTA_RATING_CAP = 200.0
# TB_K_FACTOR = .005 # Tune
# SERVE_RETURN_K_FACTOR = .005 # Tune
MIN_MATCHES = 10

# RECENT_K_FACTOR = 2.0
# POINT_K_FACTOR = .01 # Tune
# GAME_K_FACTOR = 0.0556
# SET_K_FACTOR = 0.5

TB_K_FACTOR = 1.5 # Tune
SERVE_RETURN_K_FACTOR = 2 # Tune

POINT_K_FACTOR = 2 # Tune
GAME_K_FACTOR = 1
SET_K_FACTOR = 1

async def gather_elos(df: pd.DataFrame):
    # Basic filter conditions
    conditions = ~(
        df.iloc[:, 28].isnull() |
        df.iloc[:, list(range(35, 42)) + list(range(48, 56))].isnull()
    ).any(axis=1)
    
    df = df[conditions]

    matches_df = df.sort_values(by='tourney_date').reset_index(drop=True)

    combined_names = pd.concat([matches_df['winner_name'], matches_df['loser_name']])

    players_to_elo = combined_names.drop_duplicates().tolist()

    new_header = ['player', 'last_date', 'match_number', 'matches_played', 'elo_rating', 'point_elo_rating', 'game_elo_rating', 
                  'set_elo_rating', 'service_game_elo_rating', 'return_game_elo_rating', 'tie_break_elo_rating']

    data = {
        'player': players_to_elo,   
        'last_date': [datetime(1900, 1, 1)] * len(players_to_elo),
        'match_number': [0] * len(players_to_elo),
        'matches_played': [0] * len(players_to_elo),
        'elo_rating': [START_RATING] * len(players_to_elo),
        'point_elo_rating': [START_RATING] * len(players_to_elo),
        'game_elo_rating': [START_RATING] * len(players_to_elo),
        'set_elo_rating': [START_RATING] * len(players_to_elo),
        'service_game_elo_rating': [START_RATING] * len(players_to_elo),
        'return_game_elo_rating': [START_RATING] * len(players_to_elo),
        'tie_break_elo_rating': [START_RATING] * len(players_to_elo),
    }

    global players_elo
    players_elo = pd.DataFrame(data, columns=new_header)

    tasks = []
    for index, row in matches_df.iterrows():
        if index % 1000 == 0:
            print(f"Processing Elos @ Match indexes: {index} - {index+1000}")
        tasks.append(update_elos(row))
    
    await asyncio.gather(*tasks)

    players_elo['last_date'] = pd.to_datetime(players_elo['last_date'])
    players_elo = players_elo[~(
                (players_elo['matches_played'] < MIN_MATCHES)
            )]
    players_elo = players_elo.sort_values(by='elo_rating', ascending=False)
    return players_elo

async def update_elos(row):
    try:
        player_a = players_elo[players_elo['player'] == row["winner_name"]]
        player_b = players_elo[players_elo['player'] == row["loser_name"]]

        if player_a.empty or player_b.empty:
            print("Error: One of the players not found in players_elo DataFrame")
            return

        idxA = player_a.index[0]
        idxB = player_b.index[0]

        sets = 0
    
        sets += 1 if not np.isnan(row['w1']) and not np.isnan(row['l1']) else 0
        sets += 1 if not np.isnan(row['w2']) and not np.isnan(row['l2']) else 0
        sets += 1 if not np.isnan(row['w3']) and not np.isnan(row['l3']) else 0
        sets += 1 if not np.isnan(row['w4']) and not np.isnan(row['l4']) else 0
        sets += 1 if not np.isnan(row['w5']) and not np.isnan(row['l5']) else 0

        w_games, l_games = 0, 0
        w_sets, l_sets = 0, 0
        tie_breaks_won_winner, tie_breaks_won_loser = 0, 0

        for i in range(1, sets + 1):
            if not np.isnan(row[f"w{i}"]) and not np.isnan(row[f"l{i}"]):
                if row[f"w{i}"] == 7 and row[f"l{i}"] == 6:
                    tie_breaks_won_winner += 1
                if row[f"l{i}"] == 7 and row[f"w{i}"] == 6:
                    tie_breaks_won_loser += 1
                if row[f"w{i}"] > row[f"l{i}"]:
                    w_sets += 1
                else:
                    l_sets += 1
                w_games += row[f"w{i}"]
                l_games += row[f"l{i}"]
            
        deciding_set = True if sets == 3 and row['best_of'] == 3 or sets == 5 and row['best_of'] == 5 else False

        tie_breaks_played = tie_breaks_won_winner + tie_breaks_won_loser

        points_sets_games_elo(idxA, idxB, row, w_sets, l_sets, w_games, l_games)
        tb_elo(idxA, idxB, row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played, deciding_set)
        return_serve_elo(idxA, idxB, row)
        primary_elo(idxA, idxB, row)
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pass


def update_dataframe(player_idx, col, value):
    players_elo.at[player_idx, col] = value

def primary_elo(idxA, idxB, row):
    rA = players_elo.at[idxA, 'elo_rating']
    rB = players_elo.at[idxB, 'elo_rating']

    delta = delta_rating(rA, rB, "N/A")

    rA_new = new_rating(rA, delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rB_new = new_rating(rB, -delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

    match_date = row['tourney_date']
    match_num = row['match_num']
    
    update_dataframe(idxA, 'elo_rating', rA_new)
    update_dataframe(idxA, 'last_date', match_date)
    update_dataframe(idxA, 'match_number', match_num)
    update_dataframe(idxA, 'matches_played', players_elo.at[idxA, 'matches_played'] + 1)

    update_dataframe(idxB, 'elo_rating', rB_new)
    update_dataframe(idxB, 'last_date', match_date)
    update_dataframe(idxB, 'match_number', match_num)
    update_dataframe(idxB, 'matches_played', players_elo.at[idxB, 'matches_played'] + 1)

# Stolen and changed from https://github.com/mcekovic/tennis-crystal-ball/blob/master/tennis-stats/src/main/java/org/strangeforest/tcb/stats/model/elo/EloCalculator.java need to implement
def points_sets_games_elo(idxA, idxB, row, w_sets, l_sets, w_games, l_games):
    rAset = players_elo.at[idxA, 'set_elo_rating']
    rBset = players_elo.at[idxB, 'set_elo_rating']

    rAgame = players_elo.at[idxA, 'game_elo_rating']
    rBgame = players_elo.at[idxB, 'game_elo_rating']

    rApoint = players_elo.at[idxA, 'point_elo_rating']
    rBpoint = players_elo.at[idxB, 'point_elo_rating']

    wDeltaPoint = delta_rating(rApoint, rBpoint, 'N/A')
    lDeltaPoint = 1 - wDeltaPoint
    wDeltaSet = delta_rating(rAset, rBset, 'N/A')
    lDeltaSet = 1 - wDeltaSet
    wDeltaGame = delta_rating(rAgame, rBgame, 'N/A')
    lDeltaGame = 1 - wDeltaGame

    w_return_points = row['l_svpt'] - row['l_1stWon'] - row['l_2ndWon']
    l_return_points = row['w_svpt'] - row['w_1stWon'] - row['w_2ndWon']
    w_serve_points = row['w_1stWon'] + row['w_2ndWon']
    l_serve_points = row['l_1stWon'] + row['l_2ndWon']
    w_points = w_serve_points + w_return_points
    l_points = l_serve_points + l_return_points

    # Alternate Method - slightly worse
    # deltaPointNew =  POINT_K_FACTOR * (wDeltaPoint * w_points - lDeltaPoint * l_points)
    # deltaSetNew = SET_K_FACTOR * (wDeltaSet * w_sets - lDeltaSet * l_sets)
    # deltaGameNew = GAME_K_FACTOR * (wDeltaGame * w_games - lDeltaGame * l_games)

    deltaPointNew = POINT_K_FACTOR * (wDeltaPoint * (w_points/(w_points+l_points)) - lDeltaPoint * (l_points/(w_points+l_points)))
    deltaSetNew =  SET_K_FACTOR * (wDeltaSet * (w_sets/(w_sets+l_sets)) - lDeltaSet * (l_sets/(w_sets+l_sets)))
    deltaGameNew = GAME_K_FACTOR * (wDeltaGame * (w_games/(w_games+l_games)) - lDeltaGame * (l_games/(w_games+l_games)))

    update_dataframe(idxA, 'point_elo_rating', new_rating(rApoint, deltaPointNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))
    update_dataframe(idxB, 'point_elo_rating', new_rating(rBpoint, -deltaPointNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))

    update_dataframe(idxA, 'set_elo_rating', new_rating(rAset, deltaSetNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))
    update_dataframe(idxB, 'set_elo_rating', new_rating(rBset, -deltaSetNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))

    update_dataframe(idxA, 'game_elo_rating', new_rating(rAgame, deltaGameNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))
    update_dataframe(idxB, 'game_elo_rating', new_rating(rBgame, -deltaGameNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))

def tb_elo(idxA, idxB, row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played, deciding_set):
    rAtb = players_elo.at[idxA, 'tie_break_elo_rating']
    rBtb = players_elo.at[idxB, 'tie_break_elo_rating']

    player_a_pressure_rating = pressure_rating(row['w_bpFaced'], row['w_bpSaved'], row['l_bpFaced'], row['l_bpSaved'], tie_breaks_won_winner, tie_breaks_played)
    player_b_pressure_rating = pressure_rating(row['l_bpFaced'], row['l_bpSaved'], row['w_bpFaced'], row['w_bpSaved'], tie_breaks_won_loser, tie_breaks_played) # 300 - player_a

    w_delta = delta_rating(rAtb, rBtb, "N/A")
    l_delta = 1 - w_delta

    # new_delta = TB_K_FACTOR * (w_delta * player_a_pressure_rating - l_delta * player_b_pressure_rating)

    # ODD Return
    if player_a_pressure_rating > 0:
        new_delta = TB_K_FACTOR * (w_delta * (player_a_pressure_rating/(player_a_pressure_rating+player_b_pressure_rating)) - l_delta * (player_b_pressure_rating/(player_a_pressure_rating+player_b_pressure_rating)))

        update_dataframe(idxA, 'tie_break_elo_rating', new_rating(rAtb, new_delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))
        update_dataframe(idxB, 'tie_break_elo_rating', new_rating(rBtb, -new_delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))

def return_serve_elo(idxA, idxB, row):
    surface = row['surface']

    rAservice = players_elo.at[idxA, 'service_game_elo_rating']
    rBservice = players_elo.at[idxB, 'service_game_elo_rating']

    rAreturn = players_elo.at[idxA, 'return_game_elo_rating']
    rBreturn = players_elo.at[idxB, 'return_game_elo_rating']

    playerA_serveRating = serve_rating(row['w_svpt'], row['w_1stIn'], row['w_1stWon'], row['w_2ndWon'], row['w_bpFaced'], row['w_bpSaved'], row['w_SvGms'])
    playerB_serveRating = serve_rating(row['l_svpt'], row['l_1stIn'], row['l_1stWon'], row['l_2ndWon'], row['l_bpFaced'], row['l_bpSaved'], row['l_SvGms'])

    playerA_returnRating = 100 - playerB_serveRating
    playerB_returnRating = 100 - playerA_serveRating

    ratio = return_to_serve_ratio(surface)

    # delta_aServe = delta_rating(rAservice, rBreturn, "N/A")
    # delta_bReturn = 1 - delta_aServe
    # new_delta_aServe = SERVE_RETURN_K_FACTOR * (delta_aServe * playerA_serveRating - delta_bReturn * playerB_returnRating * ratio)

    # delta_bServe = delta_rating(rBservice, rAreturn, "N/A")
    # delta_aReturn = 1 - delta_bServe
    # new_delta_bServe = SERVE_RETURN_K_FACTOR * (delta_bServe * playerB_serveRating - delta_aReturn * playerA_returnRating * ratio)

    # Percentage Based
    delta_aServe = delta_rating(rAservice, rBreturn, "N/A")
    delta_bReturn = 1 - delta_aServe
    new_delta_aServe = SERVE_RETURN_K_FACTOR * (delta_aServe * (playerA_serveRating/(playerA_serveRating + playerB_returnRating * ratio)) - delta_bReturn * (playerB_returnRating * ratio/(playerA_serveRating + playerB_returnRating * ratio)))

    delta_bServe = delta_rating(rBservice, rAreturn, "N/A")
    delta_aReturn = 1 - delta_bServe
    new_delta_bServe = SERVE_RETURN_K_FACTOR * (delta_bServe * (playerB_serveRating/(playerB_serveRating + playerA_returnRating * ratio)) - delta_aReturn * (playerA_returnRating * ratio/(playerB_serveRating + playerA_returnRating * ratio)))

    update_dataframe(idxA, 'service_game_elo_rating', new_rating(rAservice, new_delta_aServe, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))
    update_dataframe(idxB, 'service_game_elo_rating', new_rating(rBservice, new_delta_bServe, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))

    update_dataframe(idxA, 'return_game_elo_rating', new_rating(rAreturn, -new_delta_bServe, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))
    update_dataframe(idxB, 'return_game_elo_rating', new_rating(rBreturn, -new_delta_aServe, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A"))

    # TESTER
    # global grass_avg
    # global clay_avg
    # global hard_avg
    # global total_avg

    # if surface == 'Grass':
    #     grass_avg[0] += playerA_serveRating + playerB_serveRating
    #     grass_avg[1] += playerB_returnRating + playerA_returnRating
    # elif surface == 'Clay':
    #     clay_avg[0] += playerA_serveRating + playerB_serveRating
    #     clay_avg[1] += playerB_returnRating + playerA_returnRating
    # elif surface == 'Hard':
    #     hard_avg[0] += playerA_serveRating + playerB_serveRating
    #     hard_avg[1] += playerB_returnRating + playerA_returnRating

    # # Update the total average
    # total_avg[0] += playerA_serveRating + playerB_serveRating
    # total_avg[1] += playerB_returnRating + playerA_returnRating


    # # Print the results
    # print(f"Clay Avg: {clay_avg[0]/clay_avg[1]}")
    # print(f"Grass Avg: {grass_avg[0]/grass_avg[1]}")
    # print(f"Hard Avg: {hard_avg[0]/hard_avg[1]}")
    # print(f"All Avg: {total_avg[0]/total_avg[1]}")


def k_factor(level, tourney_name, round, best_of, outcome):
    k = K_FACTOR
    if "G" == level:
        k *= 1.0
    elif "Tour Finals" in tourney_name:
        k *= .9
    elif "M" in level:
        k *= .85
    elif "Olympics" in tourney_name:
        k *= .8
    elif "A" in level:
        k *= .7
    else:
        k *=.65

    # Match round adjustment is: Final 100%, Semi-Final 90%, Quarter-Final and Round-Robin 85%, Rounds of 16 and 32 80%, Rounds of 64 and 128 75% and For Bronze Medal 95%
    round_factors = {
        "F": 1.0, "BR": 0.95, "SF": 0.90, "QF": 0.85, "R16": 0.80, "R32": 0.80,
        "R64": 0.75, "R128": 0.75, "RR": 0.85
    }

    k *= round_factors.get(round, 1.0)
    
    if best_of < 5:
        k *= 0.90

    # if outcome == "W/O":
    #     k *= 0.50
    
    return k

def delta_rating(winner_rating, loser_rating, outcome):
    if outcome == "ABD":
        return 0.0
    delta = 1.0 / (1.0 + pow(10.0, (winner_rating - loser_rating) / RATING_SCALE))
    return delta

def new_rating(rating, delta, level, tourney_name, round, best_of, outcome):
    return rating + cap_delta_rating(k_factor(level, tourney_name, round, best_of, outcome) * delta * k_function(rating))

def cap_delta_rating(delta):
    return copysign(min(abs(delta), DELTA_RATING_CAP), delta)

def k_function(rating):
    return 1.0 + K_FUNCTION_MULTIPLIER / (1.0 + pow(2.0, (rating - START_RATING) / K_FUNCTION_AMPLIFIER_GRADIENT))

def elo_win_probability(elo_rating1, elo_rating2):
    return 1.0 / (1.0 + pow(10.0, (elo_rating2 - elo_rating1) / RATING_SCALE))

def return_to_serve_ratio(surface):
    surface_ratios = {
        "Hard": 2.0778, "Clay": 1.8099, "Grass": 2.2716
    }
    return surface_ratios.get(surface, 2.0098)

# # Ultimate Tennis Serve Rating = Ace % - Double Faults % + 1st Serve % + 1st Serve Points Won % + 2nd Serve Points Won % + Break Points Saved % + Service Games Won %
# # ATP Serve Rating Official = Aces - Double Faults + 1st Serve % + 1st Serve Points Won % + 2nd Serve Points Won % + Service Games Won %
def serve_rating(sv_pt, firstServe, firstServeWon, secondServeWon, bp_faced, bp_saved, sv_gms):
    secondServe = sv_pt - firstServe
    firstServeWon_pct = firstServeWon/firstServe * 100 if firstServe > 0 else 72
    secondServeWon_pct = secondServeWon/secondServe * 100 if secondServe > 0 else 50
    service_games_won_pct = (sv_gms - (bp_faced - bp_saved))/ sv_gms * 100 if sv_gms > 0 else 79
    return service_games_won_pct #firstServeWon_pct + secondServeWon_pct + service_games_won_pct

# # Ultimate Tennis Tie Break Elo = wDelta * wTBs - lDelta * lTBs
# # ATP Serve Rating Official = Break Points Converted % + Break Points Saved % + Tie Breaks Won % + Deciding Sets %
def pressure_rating(bp_faced, bp_saved, bp_faced_oppo, bp_saved_oppo, tb_won, tb_total):
    bp_saved_pct = bp_saved/bp_faced * 100 if bp_faced > 0 else 0
    bp_converted_pct = (1 - bp_saved_oppo/bp_faced_oppo) * 100 if bp_faced_oppo > 0 else 0
    tb_won_pct = tb_won/tb_total * 100 if tb_total > 0 else 0
    return tb_won_pct #bp_saved_pct + bp_converted_pct + tb_won_pct
