from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from render.utils.glicko2.glicko2 import *

DELTA_RATING_CAP = 200.0
MIN_MATCHES = 5

# K_Factors in case adding league significance in future
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

def primary_glicko(rA: Rating, rB: Rating):
    # rA.pre_rating_rd()
    # rB.pre_rating_rd()

    outcome_list = [1]
    rA_new, rB_new = new_rating_glicko2(rA, rB, outcome_list)
    return rA_new, rB_new

def points_sets_games_glicko(rAset: Rating, rBset: Rating, rAgame: Rating, rBgame: Rating, rApoint: Rating, rBpoint: Rating, row, w_sets, l_sets, w_games, l_games):
    # rAset.pre_rating_rd()
    # rBset.pre_rating_rd()
    # rAgame.pre_rating_rd()
    # rBgame.pre_rating_rd()
    # rApoint.pre_rating_rd()
    # rBpoint.pre_rating_rd()

    # wDeltaPoint = delta_rating(rApoint, rBpoint)
    # lDeltaPoint = 1 - wDeltaPoint
    # wDeltaSet = delta_rating(rAset, rBset)
    # lDeltaSet = 1 - wDeltaSet
    # wDeltaGame = delta_rating(rAgame, rBgame)
    # lDeltaGame = 1 - wDeltaGame

    w_return_points = row['l_svpt'] - row['l_1stWon'] - row['l_2ndWon']
    l_return_points = row['w_svpt'] - row['w_1stWon'] - row['w_2ndWon']
    w_serve_points = row['w_1stWon'] + row['w_2ndWon']
    l_serve_points = row['l_1stWon'] + row['l_2ndWon']
    w_points = w_serve_points + w_return_points
    l_points = l_serve_points + l_return_points

    deltaPointNew = w_points/(w_points+l_points) if w_points+l_points > 0 else 0
    deltaSetNew = w_sets/(w_sets+l_sets) if w_sets+w_sets > 0 else 0
    deltaGameNew = w_games/(w_games+l_games) if w_sets+l_sets > 0 else 0

    rApointNew, rBpointNew = new_rating_glicko2(rApoint, rBpoint, [deltaPointNew])
    rAsetNew, rBsetNew = new_rating_glicko2(rAset, rBset, [deltaSetNew])
    rAgameNew, rBgameNew = new_rating_glicko2(rAgame, rBgame, [deltaGameNew])

    return rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew

def tb_glicko(rAtb: Rating, rBtb: Rating, tie_breaks_won_winner, tie_breaks_played):
    # rAtb.pre_rating_rd()
    # rAtb.pre_rating_rd()
    tb_winner = tie_breaks_won_winner / tie_breaks_played if tie_breaks_played > 0 else 0

    rAtbNew, rBtbNew = rAtb, rBtb

    if tie_breaks_played > 0:
        rAtbNew, rBtbNew = new_rating_glicko2(rAtb, rBtb, [tb_winner])

    return rAtbNew, rBtbNew

def return_serve_glicko(rAservice: Rating, rBservice: Rating, rAreturn: Rating, rBreturn: Rating, row):
    # rAservice.pre_rating_rd()
    # rBservice.pre_rating_rd()
    # rBservice.pre_rating_rd()
    # rBreturn.pre_rating_rd()
    surface = row['surface']

    playerA_serveRating = serve_rating(row['w_bpFaced'], row['w_bpSaved'], row['w_SvGms'])
    playerB_serveRating = serve_rating(row['l_bpFaced'], row['l_bpSaved'], row['l_SvGms'])

    playerA_returnRating = 100 - playerB_serveRating
    playerB_returnRating = 100 - playerA_serveRating

    ratio = return_to_serve_ratio(surface)

    # delta_aServe = delta_rating(rAservice, rBreturn)
    # delta_bReturn = 1 - delta_aServe
    new_delta_aServe = (playerA_serveRating/(playerA_serveRating + playerB_returnRating * ratio)) - (playerB_returnRating * ratio/(playerA_serveRating + playerB_returnRating * ratio))

    # delta_bServe = delta_rating(rBservice, rAreturn)
    # delta_aReturn = 1 - delta_bServe
    new_delta_bServe = (playerB_serveRating/(playerB_serveRating + playerA_returnRating * ratio)) - (playerA_returnRating * ratio/(playerB_serveRating + playerA_returnRating * ratio))

    rAserviceNew, rBreturnNew = new_rating_glicko2(rAservice, rBreturn, [new_delta_aServe])
    rBserviceNew, rAreturnNew = new_rating_glicko2(rBservice, rAreturn, [new_delta_bServe])

    return rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew

def ace_glicko(rAace: Rating, rBace: Rating, rAaceReturn: Rating, rBaceReturn: Rating, row):
    surface = row['surface']

    #Surface minimizer later
    ratio = 1

    wAcePct = (row['w_ace']/row['w_svpt']) * ratio
    lAcePct = (row['l_ace']/row['l_svpt']) * ratio

    return base_competing_glicko(rAace, rBace, rAaceReturn, rBaceReturn, wAcePct, lAcePct)

def first_won_glicko(rAfw: Rating, rBfw: Rating, rAvFw: Rating, rBcFw: Rating, row):
    surface = row['surface']

    #Surface minimizer later
    ratio = 1

    w1stIn = row['w_1stWon'] * ratio
    l1stIn = row['l_1stWon'] * ratio

    return base_competing_glicko(rAfw, rBfw, rAvFw, rBcFw, w1stIn, l1stIn)

def second_won_glicko(rAfw: Rating, rBfw: Rating, rAvFw: Rating, rBcFw: Rating, row):
    surface = row['surface']

    #Surface minimizer later
    ratio = 1

    w1stIn = row['w_2ndWon'] * ratio
    l1stIn = row['l_2ndWon'] * ratio

    return base_competing_glicko(rAfw, rBfw, rAvFw, rBcFw, w1stIn, l1stIn)

def base_competing_glicko(rAfirst: Rating, rBfirst: Rating, rAsecond: Rating, rBsecond: Rating, aPct, bPct):
    # rAfirst.pre_rating_rd()
    # rBfirst.pre_rating_rd()
    # rAsecond.pre_rating_rd()
    # rBsecond.pre_rating_rd()
    rAfirstNew, rBsecondNew = new_rating_glicko2(rAfirst, rBsecond, [aPct])
    rBfirstNew, rAsecondNew = new_rating_glicko2(rBfirst, rAsecond, [bPct])

    return rAfirstNew, rBfirstNew, rAsecondNew, rBsecondNew

def new_rating_glicko2(ratingA: Rating, ratingB: Rating, outcome_list):
    rating_list = [ratingB.rating]
    rd_list = [ratingB.rd]
    ratingA.update_player(rating_list, rd_list, outcome_list)

    rating_list = [ratingA.rating]
    rd_list = [ratingA.rd]
    ratingB.update_player(rating_list, rd_list, [1 - x for x in outcome_list])

    return ratingA, ratingB

def return_to_serve_ratio(surface):
    surface_ratios = {
        "Hard": 3.8045, "Clay": 2.9240, "Grass": 4.6229
    }
    return surface_ratios.get(surface, 3.5727)

def serve_rating(bp_faced, bp_saved, sv_gms):
    service_games_won_pct = (sv_gms - (bp_faced - bp_saved)) / sv_gms * 100 if sv_gms > 0 else 79
    return service_games_won_pct
