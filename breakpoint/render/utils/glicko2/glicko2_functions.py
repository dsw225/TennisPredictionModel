from math import pow, sqrt, log, exp, fabs, pi
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np

START_RATING = 1500
RATING_SCALE = 173.7178  # Scaling factor to convert Glicko-2 rating to traditional rating scale
TAU = 0.5  # System constant

# Constants
DELTA_RATING_CAP = 200.0
MIN_MATCHES = 5

# K_Factors in case adding league significance in future
TB_K_FACTOR = 1
SERVE_RETURN_K_FACTOR = 1
ACE_K_FACTOR = 1
POINT_K_FACTOR = 1
GAME_K_FACTOR = 1
SET_K_FACTOR = 1

class Rating:
    def __init__(self, rating=START_RATING, rd=350, vol=0.06):
        self.rating = (rating - START_RATING) / RATING_SCALE
        self.rd = rd / RATING_SCALE
        self.vol = vol

    def pre_rating_rd(self):
        self.rd = sqrt(self.rd ** 2 + self.vol ** 2)

    def update_player(self, rating_list, rd_list, outcome_list):
        rating_list = [(x - START_RATING) / RATING_SCALE for x in rating_list]
        rd_list = [x / RATING_SCALE for x in rd_list]

        v = self._v(rating_list, rd_list)
        self.vol = self._new_vol(rating_list, rd_list, outcome_list, v)
        self.pre_rating_rd()

        self.rd = 1 / sqrt((1 / self.rd ** 2) + (1 / v))

        temp_sum = 0
        for i in range(len(rating_list)):
            temp_sum += self._g(rd_list[i]) * (outcome_list[i] - self._e(rating_list[i], rd_list[i]))
        self.rating += self.rd ** 2 * temp_sum

        self.rating = self.rating * RATING_SCALE + START_RATING
        self.rd = self.rd * RATING_SCALE

    def _new_vol(self, rating_list, rd_list, outcome_list, v):
        a = log(self.vol ** 2)
        eps = 0.000001
        A = a
        delta = self._delta(rating_list, rd_list, outcome_list, v)
        if delta ** 2 > (self.rd ** 2 + v):
            B = log(delta ** 2 - self.rd ** 2 - v)
        else:
            k = 1
            while self._f(a - k * sqrt(TAU ** 2), delta, v, a) < 0:
                k += 1
            B = a - k * sqrt(TAU ** 2)

        fA = self._f(A, delta, v, a)
        fB = self._f(B, delta, v, a)

        while fabs(B - A) > eps:
            C = A + (A - B) * fA / (fB - fA)
            fC = self._f(C, delta, v, a)
            if fC * fB < 0:
                A = B
                fA = fB
            else:
                fA /= 2
            B = C
            fB = fC

        return exp(A / 2)

    def _f(self, x, delta, v, a):
        ex = exp(x)
        num1 = ex * (delta ** 2 - self.rating ** 2 - v - ex)
        denom1 = 2 * (self.rating ** 2 + v + ex) ** 2
        return num1 / denom1 - (x - a) / TAU ** 2

    def _delta(self, rating_list, rd_list, outcome_list, v):
        temp_sum = 0
        for i in range(len(rating_list)):
            temp_sum += self._g(rd_list[i]) * (outcome_list[i] - self._e(rating_list[i], rd_list[i]))
        return v * temp_sum

    def _v(self, rating_list, rd_list):
        temp_sum = 0
        for i in range(len(rating_list)):
            temp_e = self._e(rating_list[i], rd_list[i])
            temp_sum += self._g(rd_list[i]) ** 2 * temp_e * (1 - temp_e)
        return 1 / temp_sum

    def _e(self, p2rating, p2rd):
        return 1 / (1 + exp(-self._g(p2rd) * (self.rating - p2rating)))

    def _g(self, rd):
        return 1 / sqrt(1 + 3 * rd ** 2 / pi ** 2)

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

def primary_glicko(rA, rB, row, ratingA, ratingB):
    ratingA.pre_rating_rd()
    ratingB.pre_rating_rd()
    delta = delta_rating(rA, rB)

    outcome_list = [1, 0]
    rA_new, rB_new = new_rating_glicko2(ratingA, ratingB, delta, outcome_list, row)
    return rA_new, rB_new

def points_sets_games_glicko(rAset, rBset, rAgame, rBgame, rApoint, rBpoint, row, w_sets, l_sets, w_games, l_games, ratingA, ratingB):
    ratingA.pre_rating_rd()
    ratingB.pre_rating_rd()

    wDeltaPoint = delta_rating(rApoint, rBpoint)
    lDeltaPoint = 1 - wDeltaPoint
    wDeltaSet = delta_rating(rAset, rBset)
    lDeltaSet = 1 - wDeltaSet
    wDeltaGame = delta_rating(rAgame, rBgame)
    lDeltaGame = 1 - wDeltaGame

    w_return_points = row['l_svpt'] - row['l_1stWon'] - row['l_2ndWon']
    l_return_points = row['w_svpt'] - row['w_1stWon'] - row['w_2ndWon']
    w_serve_points = row['w_1stWon'] + row['w_2ndWon']
    l_serve_points = row['l_1stWon'] + row['l_2ndWon']
    w_points = w_serve_points + w_return_points
    l_points = l_serve_points + l_return_points

    deltaPointNew = POINT_K_FACTOR * (wDeltaPoint * (w_points/(w_points+l_points)) - lDeltaPoint * (l_points/(w_points+l_points))) if w_points+l_points > 0 else 0
    deltaSetNew = SET_K_FACTOR * (wDeltaSet * (w_sets/(w_sets+l_sets)) - lDeltaSet * (l_sets/(w_sets+l_sets))) if w_sets+w_sets > 0 else 0
    deltaGameNew = GAME_K_FACTOR * (wDeltaGame * (w_games/(w_games+l_games)) - lDeltaGame * (l_games/(w_games+l_games))) if w_sets+l_sets > 0 else 0

    rApointNew, rBpointNew = new_rating_glicko2(ratingA, ratingB, deltaPointNew, [1, 0], row)
    rAsetNew, rBsetNew = new_rating_glicko2(ratingA, ratingB, deltaSetNew, [1, 0], row)
    rAgameNew, rBgameNew = new_rating_glicko2(ratingA, ratingB, deltaGameNew, [1, 0], row)

    return rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew

def tb_glicko(rAtb, rBtb, row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played, ratingA, ratingB):
    tb_winner = tie_breaks_won_winner / tie_breaks_played if tie_breaks_played > 0 else 0
    tb_loser = tie_breaks_won_loser / tie_breaks_played if tie_breaks_played > 0 else 0

    w_delta = delta_rating(rAtb, rBtb)
    l_delta = 1 - w_delta

    rAtbNew, rBtbNew = ratingA, ratingB

    if tie_breaks_played > 0:
        new_delta = TB_K_FACTOR * (w_delta * tb_winner - l_delta * tb_loser)
        rAtbNew, rBtbNew = new_rating_glicko2(ratingA, ratingB, new_delta, [1, 0], row)

    return rAtbNew, rBtbNew

def return_serve_glicko(rAservice, rBservice, rAreturn, rBreturn, row, ratingA, ratingB):
    surface = row['surface']

    playerA_serveRating = serve_rating(row['w_bpFaced'], row['w_bpSaved'], row['w_SvGms'])
    playerB_serveRating = serve_rating(row['l_bpFaced'], row['l_bpSaved'], row['l_SvGms'])

    playerA_returnRating = 100 - playerB_serveRating
    playerB_returnRating = 100 - playerA_serveRating

    ratio = return_to_serve_ratio(surface)

    delta_aServe = delta_rating(rAservice, rBreturn)
    delta_bReturn = 1 - delta_aServe
    new_delta_aServe = SERVE_RETURN_K_FACTOR * (delta_aServe * (playerA_serveRating/(playerA_serveRating + playerB_returnRating * ratio)) - delta_bReturn * (playerB_returnRating * ratio/(playerA_serveRating + playerB_returnRating * ratio)))

    delta_bServe = delta_rating(rBservice, rAreturn)
    delta_aReturn = 1 - delta_bServe
    new_delta_bServe = SERVE_RETURN_K_FACTOR * (delta_bServe * (playerB_serveRating/(playerB_serveRating + playerA_returnRating * ratio)) - delta_aReturn * (playerA_returnRating * ratio/(playerB_serveRating + playerA_returnRating * ratio)))

    rAserviceNew, rBserviceNew = new_rating_glicko2(ratingA, ratingB, new_delta_aServe, [1, 0], row)
    rAreturnNew, rBreturnNew = new_rating_glicko2(ratingA, ratingB, new_delta_bServe, [1, 0], row)

    return rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew

def ace_glicko(rAace, rBace, rAaceReturn, rBaceReturn, row, ratingA, ratingB):
    surface = row['surface']

    ratio = 1

    wAcePct = (row['w_ace']/row['w_svpt']) * ratio
    lAcePct = (row['l_ace']/row['l_svpt']) * ratio

    return base_competing_glicko(rAace, rBace, rAaceReturn, rBaceReturn, wAcePct, lAcePct, row, ratingA, ratingB)

def first_won_glicko(rAfw, rBfw, rAvFw, rBcFw, row, ratingA, ratingB):
    surface = row['surface']

    ratio = 1

    w1stIn = row['w_1stWon'] * ratio
    l1stIn = row['l_1stWon'] * ratio

    return base_competing_glicko(rAfw, rBfw, rAvFw, rBcFw, w1stIn, l1stIn, row, ratingA, ratingB)

def second_won_glicko(rAfw, rBfw, rAvFw, rBcFw, row, ratingA, ratingB):
    surface = row['surface']

    ratio = 1

    w1stIn = row['w_2ndWon'] * ratio
    l1stIn = row['l_2ndWon'] * ratio

    return base_competing_glicko(rAfw, rBfw, rAvFw, rBcFw, w1stIn, l1stIn, row, ratingA, ratingB)

def base_competing_glicko(rAfirst, rBfirst, rAsecond, rBsecond, aPct, bPct):
    rAfirstNew, rBsecondNew = new_rating_glicko2(rAfirst, rBsecond, [aPct])
    rBfirstNew, rAsecondNew = new_rating_glicko2(rBfirst, rAsecond, [bPct])

    return rAfirstNew, rBfirstNew, rAsecondNew, rBsecondNew

def delta_rating(winner_rating, loser_rating):
    return 1.0 / (1.0 + pow(10.0, (winner_rating - loser_rating) / RATING_SCALE))

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
