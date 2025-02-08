from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import copy
from render.utils.stephenson.stephenson import *

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

def primary_steph(rA: Stephenson, rB: Stephenson, row):
    date = row['tourney_date']
    rA_new, rB_new = new_rating_steph(rA, rB, 1, date)
    return rA_new, rB_new

def points_sets_games_steph(rAset: Stephenson, rBset: Stephenson, rAgame: Stephenson, rBgame: Stephenson, rApoint: Stephenson, rBpoint: Stephenson, row, w_sets, l_sets, w_games, l_games):
    # wDeltaPoint = delta_rating(rApoint, rBpoint)
    # lDeltaPoint = 1 - wDeltaPoint
    # wDeltaSet = delta_rating(rAset, rBset)
    # lDeltaSet = 1 - wDeltaSet
    # wDeltaGame = delta_rating(rAgame, rBgame)
    # lDeltaGame = 1 - wDeltaGame
    date = row['tourney_date']

    w_return_points = row['l_svpt'] - row['l_1stWon'] - row['l_2ndWon']
    l_return_points = row['w_svpt'] - row['w_1stWon'] - row['w_2ndWon']
    w_serve_points = row['w_1stWon'] + row['w_2ndWon']
    l_serve_points = row['l_1stWon'] + row['l_2ndWon']
    w_points = w_serve_points + w_return_points
    l_points = l_serve_points + l_return_points

    deltaPointNew = w_points/(w_points+l_points) if w_points+l_points > 0 else 0
    deltaSetNew = w_sets/(w_sets+l_sets) if w_sets+w_sets > 0 else 0
    deltaGameNew = w_games/(w_games+l_games) if w_sets+l_sets > 0 else 0

    rApointNew, rBpointNew = new_rating_steph(rApoint, rBpoint, deltaPointNew, date)
    rAsetNew, rBsetNew = new_rating_steph(rAset, rBset, deltaSetNew, date)
    rAgameNew, rBgameNew = new_rating_steph(rAgame, rBgame, deltaGameNew, date)

    return rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew

def tb_steph(rAtb: Stephenson, rBtb: Stephenson, tie_breaks_won_winner, tie_breaks_played, date):
    tb_winner = tie_breaks_won_winner / tie_breaks_played if tie_breaks_played > 0 else 0

    rAtbNew, rBtbNew = rAtb, rBtb

    if tie_breaks_played > 0:
        rAtbNew, rBtbNew = new_rating_steph(rAtb, rBtb, tb_winner, date)

    return rAtbNew, rBtbNew

def bp_steph(rAtb: Stephenson, rBtb: Stephenson, bp_won_winner, bp_played, date):
    tb_winner = bp_won_winner / bp_played if bp_played > 0 else 0

    rAtbNew, rBtbNew = rAtb, rBtb

    if bp_played > 0:
        rAtbNew, rBtbNew = new_rating_steph(rAtb, rBtb, tb_winner, date)

    return rAtbNew, rBtbNew

total_clay = 0
total_grass = 0
total_hard = 0
num_clay = 0
num_grass = 0
num_hard = 0
def return_serve_steph(rAservice: Stephenson, rBservice: Stephenson, rAreturn: Stephenson, rBreturn: Stephenson, row):
    global total_clay
    global total_grass
    global total_hard
    global num_clay
    global num_grass
    global num_hard
    date = row['tourney_date']
    surface = row['surface']

    #Newer
    w_return_points = row['l_svpt'] - row['l_1stWon'] - row['l_2ndWon']
    l_return_points = row['w_svpt'] - row['w_1stWon'] - row['w_2ndWon']
    w_serve_points = row['w_1stWon'] + row['w_2ndWon']
    l_serve_points = row['l_1stWon'] + row['l_2ndWon']
    # w_points = w_serve_points + w_return_points
    # l_points = l_serve_points + l_return_points

    surface_ratios = {
        "Hard": .5 / 0.3724, "Clay": .5 / 0.3943, "Grass": .5 / 0.3524
    }

    # ratio =  surface_ratios.get(surface, .5 / .38)
    ratio = 1

    new_delta_aServe = (w_return_points/(w_serve_points + l_return_points)) * ratio
    new_delta_bServe = (l_return_points/(l_serve_points + w_return_points)) * ratio

    if(surface == "Clay"):
        total_clay += new_delta_aServe + new_delta_bServe
        num_clay += 2
    elif(surface == "Hard"):
        total_hard += new_delta_aServe + new_delta_bServe
        num_hard += 2
    elif(surface == "Grass"):
        total_grass += new_delta_aServe + new_delta_bServe
        num_grass += 2

    # print(f"Percent 1st Won Clay: {total_clay/num_clay} Grass: {total_grass/num_grass} Hard: {total_hard/num_hard}")

    rAserviceNew, rBreturnNew = new_rating_steph(rAservice, rBreturn, new_delta_aServe, date)
    rBserviceNew, rAreturnNew = new_rating_steph(rBservice, rAreturn, new_delta_bServe, date)

    #Older
    # playerA_serveStephenson = serve_rating(row['w_bpFaced'], row['w_bpSaved'], row['w_SvGms'])
    # playerB_serveStephenson = serve_rating(row['l_bpFaced'], row['l_bpSaved'], row['l_SvGms'])

    # playerA_returnStephenson = 100 - playerB_serveStephenson
    # playerB_returnStephenson = 100 - playerA_serveStephenson

    # ratio = return_to_serve_ratio(surface)

    # # delta_aServe = delta_rating(rAservice, rBreturn)
    # # delta_bReturn = 1 - delta_aServe
    # new_delta_aServe = (playerA_serveStephenson/(playerA_serveStephenson + playerB_returnStephenson * ratio)) - (playerB_returnStephenson * ratio/(playerA_serveStephenson + playerB_returnStephenson * ratio))

    # # delta_bServe = delta_rating(rBservice, rAreturn)
    # # delta_aReturn = 1 - delta_bServe
    # new_delta_bServe = (playerB_serveStephenson/(playerB_serveStephenson + playerA_returnStephenson * ratio)) - (playerA_returnStephenson * ratio/(playerB_serveStephenson + playerA_returnStephenson * ratio))

    # rAserviceNew, rBreturnNew = new_rating_steph(rAservice, rBreturn, new_delta_aServe, date)
    # rBserviceNew, rAreturnNew = new_rating_steph(rBservice, rAreturn, new_delta_bServe, date)

    return rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew



def ace_steph(rAace: Stephenson, rBace: Stephenson, rAaceReturn: Stephenson, rBaceReturn: Stephenson, row):
    date = row['tourney_date']
    surface = row['surface']

    surface_ratios = {
        "Hard": .5 / 0.088888, "Clay": .5 / 0.05554, "Grass": .5 / 0.10110
    }

    # ratio =  surface_ratios.get(surface, .5 / .08)
    ratio = 1


    wAcePct = (row['w_ace']/row['w_svpt']) * ratio
    lAcePct = (row['l_ace']/row['l_svpt']) * ratio

    return base_competing_steph(rAace, rBace, rAaceReturn, rBaceReturn, wAcePct, lAcePct, date)

def first_won_steph(rAfw: Stephenson, rBfw: Stephenson, rAvFw: Stephenson, rBvFw: Stephenson, row):
    date = row['tourney_date']
    surface = row['surface']

    surface_ratios = {
        "Hard": .5 / 0.72502, "Clay": .5 / 0.69128, "Grass": .5 / 0.74393
    }

    # ratio =  surface_ratios.get(surface, .5 / .72)
    ratio = 1


    w1stIn = row['w_1stWon']/row['w_1stIn'] * ratio
    l1stIn = row['l_1stWon']/row['l_1stIn'] * ratio

    return base_competing_steph(rAfw, rBfw, rAvFw, rBvFw, w1stIn, l1stIn, date)


def second_won_steph(rAfw: Stephenson, rBfw: Stephenson, rAvFw: Stephenson, rBvFw: Stephenson, row):
    date = row['tourney_date']
    surface = row['surface']

    surface_ratios = {
        "Hard": .5 / 0.50975, "Clay": .5 / 0.50792, "Grass": .5 / 0.52159
    }

    # ratio =  surface_ratios.get(surface, .5 / .515)
    ratio = 1


    w1stIn = row['w_2ndWon']/(row['w_svpt']-row['w_1stIn']) * ratio
    l1stIn = row['l_2ndWon']/(row['l_svpt']-row['l_1stIn']) * ratio

    # if(surface == "Clay"):
    #     total_clay += w1stIn + l1stIn
    #     num_clay += 2
    # elif(surface == "Hard"):
    #     total_hard += w1stIn + l1stIn
    #     num_hard += 2
    # elif(surface == "Grass"):
    #     total_grass += w1stIn + l1stIn
    #     num_grass += 2

    # print(f"Percent 1st Won Clay: {total_clay/num_clay} Grass: {total_grass/num_grass} Hard: {total_hard/num_hard}")

    return base_competing_steph(rAfw, rBfw, rAvFw, rBvFw, w1stIn, l1stIn, date)

def base_competing_steph(rAfirst: Stephenson, rBfirst: Stephenson, rAsecond: Stephenson, rBsecond: Stephenson, aPct, bPct, date):
    rAfirstNew, rBsecondNew = new_rating_steph(rAfirst, rBsecond, aPct, date)
    rBfirstNew, rAsecondNew = new_rating_steph(rBfirst, rAsecond, bPct, date)

    return rAfirstNew, rBfirstNew, rAsecondNew, rBsecondNew

def new_rating_steph(ratingA: Stephenson, ratingB: Stephenson, outcome_list, date):
    ratingA.updateVar(date)
    ratingB.updateVar(date)
    
    # So we don't evaluate on outcome
    a_clone = copy.deepcopy(ratingA)

    ratingA.newVarRating(ratingB, outcome_list, 0)
    ratingB.newVarRating(a_clone, (1 - outcome_list), 0) 

    return ratingA, ratingB

def return_to_serve_ratio(surface):
    surface_ratios = {
        "Hard": 3.8045, "Clay": 2.9240, "Grass": 4.6229
    }
    return surface_ratios.get(surface, 3.5727)

def serve_rating(bp_faced, bp_saved, sv_gms):
    service_games_won_pct = (sv_gms - (bp_faced - bp_saved)) / sv_gms * 100 if sv_gms > 0 else 79
    return service_games_won_pct
