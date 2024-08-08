from math import pow, copysign, floor, ceil
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np

START_RATING = 1400
RATING_SCALE = 480.0
K_FACTOR = 32.0
K_FUNCTION_AMPLIFIER = 10.0
K_FUNCTION_AMPLIFIER_GRADIENT = 63.0
K_FUNCTION_MULTIPLIER = 2.0 * (K_FUNCTION_AMPLIFIER - 1.0)

DELTA_RATING_CAP = 200.0
MIN_MATCHES = 5
RECENT_WEEKS = 130
LATEST_WEEKS = 56
NOW_WEEKS = 12
RECENT_K_GAIN_FACTOR = 2

# K_Factors in case adding league signifigance in future
TB_K_FACTOR = 1
SERVE_RETURN_K_FACTOR = 1
ACE_K_FACTOR = 1
POINT_K_FACTOR = 1
GAME_K_FACTOR = 1
SET_K_FACTOR = 1

def get_score_stats(row):
    sets = 0

    sets += 1 if not np.isnan(pd.to_numeric(row['w1'], errors='coerce')) and not np.isnan(pd.to_numeric(row['l1'], errors='coerce')) else 0
    sets += 1 if not np.isnan(pd.to_numeric(row['w2'], errors='coerce')) and not np.isnan(pd.to_numeric(row['l2'], errors='coerce')) else 0
    sets += 1 if not np.isnan(pd.to_numeric(row['w3'], errors='coerce')) and not np.isnan(pd.to_numeric(row['l3'], errors='coerce')) else 0
    sets += 1 if not np.isnan(pd.to_numeric(row['w4'], errors='coerce')) and not np.isnan(pd.to_numeric(row['l4'], errors='coerce')) else 0
    sets += 1 if not np.isnan(pd.to_numeric(row['w5'], errors='coerce')) and not np.isnan(pd.to_numeric(row['l5'], errors='coerce')) else 0

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

    tie_breaks_played = tie_breaks_won_winner + tie_breaks_won_loser

    return w_games, l_games, w_sets, l_sets, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played

def primary_elo(rA, rB, row):
    delta = delta_rating(rA, rB, "N/A")

    rA_new = new_rating(rA, delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rB_new = new_rating(rB, -delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

    return rA_new, rB_new

# Stolen and changed from https://github.com/mcekovic/tennis-crystal-ball/blob/master/tennis-stats/src/main/java/org/strangeforest/tcb/stats/model/elo/EloCalculator.java need to implement
def points_sets_games_elo(rAset, rBset, rAgame, rBgame, rApoint, rBpoint, row, w_sets, l_sets, w_games, l_games):
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

    deltaPointNew = POINT_K_FACTOR * (wDeltaPoint * (w_points/(w_points+l_points)) - lDeltaPoint * (l_points/(w_points+l_points))) if w_points+l_points > 0 else 0
    deltaSetNew =  SET_K_FACTOR * (wDeltaSet * (w_sets/(w_sets+l_sets)) - lDeltaSet * (l_sets/(w_sets+l_sets))) if w_sets+w_sets > 0 else 0
    deltaGameNew = GAME_K_FACTOR * (wDeltaGame * (w_games/(w_games+l_games)) - lDeltaGame * (l_games/(w_games+l_games))) if w_sets+l_sets > 0 else 0

    rApointNew = new_rating(rApoint, deltaPointNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rBpointNew = new_rating(rBpoint, -deltaPointNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rAsetNew = new_rating(rAset, deltaSetNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rBsetNew = new_rating(rBset, -deltaSetNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rAgameNew = new_rating(rAgame, deltaGameNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rBgameNew = new_rating(rBgame, -deltaGameNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    
    return rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew

def tb_elo(rAtb, rBtb, row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played):
    tb_winner = tie_breaks_won_winner/tie_breaks_played if tie_breaks_played > 0 else 0
    tb_loser = tie_breaks_won_loser/tie_breaks_played if tie_breaks_played > 0 else 0

    w_delta = delta_rating(rAtb, rBtb, "N/A")
    l_delta = 1 - w_delta

    rAtbNew, rBtbNew = rAtb, rBtb

    if tie_breaks_played > 0:
        new_delta = TB_K_FACTOR * (w_delta * tb_winner - l_delta * tb_loser)

        rAtbNew = new_rating(rAtb, new_delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
        rBtbNew = new_rating(rBtb, -new_delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

    return rAtbNew, rBtbNew

def return_serve_elo(rAservice, rBservice, rAreturn, rBreturn, row):
    surface = row['surface']

    playerA_serveRating = serve_rating(row['w_bpFaced'], row['w_bpSaved'], row['w_SvGms'])
    playerB_serveRating = serve_rating(row['l_bpFaced'], row['l_bpSaved'], row['l_SvGms'])

    playerA_returnRating = 100 - playerB_serveRating
    playerB_returnRating = 100 - playerA_serveRating

    ratio = return_to_serve_ratio(surface)

    # Percentage Based
    delta_aServe = delta_rating(rAservice, rBreturn, "N/A")
    delta_bReturn = 1 - delta_aServe
    new_delta_aServe = SERVE_RETURN_K_FACTOR * (delta_aServe * (playerA_serveRating/(playerA_serveRating + playerB_returnRating * ratio)) - delta_bReturn * (playerB_returnRating * ratio/(playerA_serveRating + playerB_returnRating * ratio)))

    delta_bServe = delta_rating(rBservice, rAreturn, "N/A")
    delta_aReturn = 1 - delta_bServe
    new_delta_bServe = SERVE_RETURN_K_FACTOR * (delta_bServe * (playerB_serveRating/(playerB_serveRating + playerA_returnRating * ratio)) - delta_aReturn * (playerA_returnRating * ratio/(playerB_serveRating + playerA_returnRating * ratio)))

    rAserviceNew = new_rating(rAservice, new_delta_aServe, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rBserviceNew = new_rating(rBservice, new_delta_bServe, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rAreturnNew = new_rating(rAreturn, -new_delta_bServe, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rBreturnNew = new_rating(rBreturn, -new_delta_aServe, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

    return rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew

def ace_elo(rAace, rBace, rAaceReturn, rBaceReturn, row):
    surface = row['surface']

    # surface_ratios = {
    #     "Hard": .877, "Clay": 1.3889, "Grass": .78125
    # }
    # ratio = surface_ratios.get(surface, 3.5727)

    ratio = 1

    wAcePct = (row['w_ace']/row['w_svpt']) * ratio
    lAcePct = (row['l_ace']/row['l_svpt']) * ratio

    return base_competing_elo(rAace, rBace, rAaceReturn, rBaceReturn, wAcePct, lAcePct, row)

def first_won_elo(rAfw, rBfw, rAvFw, rBcFw, row):
    surface = row['surface']

    ratio = 1 #Calc avg later

    w1stIn = row['w_1stWon'] * ratio
    l1stIn = row['l_1stWon'] * ratio

    return base_competing_elo(rAfw, rBfw, rAvFw, rBcFw, w1stIn, l1stIn, row)

def second_won_elo(rAfw, rBfw, rAvFw, rBcFw, row):
    surface = row['surface']

    ratio = 1 #Calc avg later

    w1stIn = row['w_2ndWon'] * ratio
    l1stIn = row['l_2ndWon'] * ratio

    return base_competing_elo(rAfw, rBfw, rAvFw, rBcFw, w1stIn, l1stIn, row)

def base_competing_elo(rAfirst, rBfirst, rAsecond, rBsecond, aPct, bPct, row):
    aVpct = 1 - bPct
    bVpct = 1 - aPct

    aDelta = delta_rating(rAfirst, rBsecond, "N/A")
    bDelta = delta_rating(rBfirst, rAsecond, "N/A")

    aVdelta = 1 - bDelta
    bVdelta = 1 - aDelta

    new_delta_aAce = (aDelta * aPct) - (bVdelta * bVpct)
    new_delta_bAce = (bDelta * bPct) - (aVdelta * aVpct)

    rAfirstNew = new_rating(rAfirst, new_delta_aAce, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rBfirstNew = new_rating(rBfirst, new_delta_bAce, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rAsecondNew = new_rating(rAsecond, -new_delta_bAce, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rBsecondNew = new_rating(rBsecond, -new_delta_aAce, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

    return rAfirstNew, rBfirstNew, rAsecondNew, rBsecondNew

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

    k *= round_factors.get(round, .75)
    
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
        "Hard": 3.8045, "Clay": 2.9240, "Grass": 4.6229
    }
    return surface_ratios.get(surface, 3.5727)

# # Ultimate Tennis Serve Rating = Ace % - Double Faults % + 1st Serve % + 1st Serve Points Won % + 2nd Serve Points Won % + Break Points Saved % + Service Games Won %
# # ATP Serve Rating Official = Aces - Double Faults + 1st Serve % + 1st Serve Points Won % + 2nd Serve Points Won % + Service Games Won %
def serve_rating(bp_faced, bp_saved, sv_gms):
    service_games_won_pct = (sv_gms - (bp_faced - bp_saved))/ sv_gms * 100 if sv_gms > 0 else 79
    return service_games_won_pct 
