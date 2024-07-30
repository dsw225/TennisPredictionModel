from math import pow, copysign, floor, ceil
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import traceback
import warnings

START_RATING = 1500
RATING_SCALE = 480.0
K_FACTOR = 32.0
K_FUNCTION_AMPLIFIER = 10.0
K_FUNCTION_AMPLIFIER_GRADIENT = 63.0
K_FUNCTION_MULTIPLIER = 2.0 * (K_FUNCTION_AMPLIFIER - 1.0)

DELTA_RATING_CAP = 200.0
MIN_MATCHES = 5
RECENT_WEEKS = 56
RECENT_K_GAIN_FACTOR = 2

# K_Factors in case adding league signifigance in future
TB_K_FACTOR = 1
SERVE_RETURN_K_FACTOR = 1
POINT_K_FACTOR = 1
GAME_K_FACTOR = 1
SET_K_FACTOR = 1

async def filter_games(df: pd.DataFrame, end_date : datetime.date):
    warnings.filterwarnings("ignore", category=FutureWarning, message="Comparison of Timestamp with datetime.date is deprecated")
    df['last_date'] = pd.to_datetime(df['last_date'])
    df = df[~(
                (df['matches_played'] < MIN_MATCHES) |
                (df['last_date'].dt.date < (end_date - relativedelta(years=1)))
            )]
    df = df.sort_values(by='elo_rating', ascending=False)
    return df

async def update_elos(players_elo : pd.DataFrame, row):
    try:
        player_a = players_elo[players_elo['player'] == row["winner_name"]]
        player_b = players_elo[players_elo['player'] == row["loser_name"]]

        if player_a.empty or player_b.empty:
            print("Error: One of the players not found in players_elo DataFrame")
            return

        idxA = player_a.index[0]
        idxB = player_b.index[0]
        
        w_games, l_games, w_sets, l_sets, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played = get_score_stats(row)

        #Primary update
        match_date = row['tourney_date']
        match_num = row['match_num']
        rA_new, rB_new = primary_elo(players_elo.at[idxA, 'elo_rating'], players_elo.at[idxB, 'elo_rating'], row)
        update_primary_elo(players_elo, idxA, idxB, rA_new, rB_new, match_date, match_num)
        
        # Point Sets etc.
        rAset = players_elo.at[idxA, 'set_elo_rating']
        rBset = players_elo.at[idxB, 'set_elo_rating']

        rAgame = players_elo.at[idxA, 'game_elo_rating']
        rBgame = players_elo.at[idxB, 'game_elo_rating']

        rApoint = players_elo.at[idxA, 'point_elo_rating']
        rBpoint = players_elo.at[idxB, 'point_elo_rating']

        rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew = points_sets_games_elo(rAset, rBset, rAgame, rBgame, rApoint, rBpoint, row, w_sets, l_sets, w_games, l_games)
        update_points_sets_games_elo(players_elo, idxA, idxB, rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew)

        # TB Update
        rAtbNew, rBtbNew = tb_elo(players_elo.at[idxA, 'tie_break_elo_rating'], players_elo.at[idxB, 'tie_break_elo_rating'], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
        update_tb_elo(players_elo, idxA, idxB, rAtbNew, rBtbNew)

        # Serve/Return Update
        try:
            rAservice = players_elo.at[idxA, 'service_game_elo_rating']
            rBservice = players_elo.at[idxB, 'service_game_elo_rating']

            rAreturn = players_elo.at[idxA, 'return_game_elo_rating']
            rBreturn = players_elo.at[idxB, 'return_game_elo_rating']

            rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew = return_serve_elo(rAservice, rBservice, rAreturn, rBreturn, row)
            update_return_serve_elo(players_elo, idxA, idxB, rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew)
        except Exception as b:
            print(f"Skip worked {b}")
            pass #Missing stats ignore
    except Exception as e:
        print(f"An error occurred: {e}")
        traceback.print_exc()
        pass

async def parse_recent(recent_matches_a: pd.DataFrame, recent_matches_b: pd.DataFrame, game):
    game_date = game['tourney_date']

    player_a = game["winner_name"]
    player_b = game["loser_name"]

    recent_matches_a = recent_matches_a[~(
        recent_matches_a['tourney_date'] < (game_date - timedelta(weeks=RECENT_WEEKS)) 
    )]
    recent_matches_a = recent_matches_a.sort_values(by='tourney_date', ascending=False)

    recent_matches_b = recent_matches_b[~(
        recent_matches_b['tourney_date'] < (game_date - timedelta(weeks=RECENT_WEEKS)) 
    )]
    recent_matches_b = recent_matches_b.sort_values(by='tourney_date', ascending=False)

    player_a_stats = await process_player_matches(player_a, recent_matches_a, game)
    player_b_stats = await process_player_matches(player_b, recent_matches_b, game)

    player_a_entry = game.copy()
    player_a_entry.update(player_a_stats)

    player_b_entry = game.copy()
    player_b_entry.update(player_b_stats)

    # Append the data to the DataFrames
    recent_matches_a = pd.concat([recent_matches_a, pd.DataFrame([player_a_entry])], ignore_index=True)
    recent_matches_b = pd.concat([recent_matches_b, pd.DataFrame([player_b_entry])], ignore_index=True)

    return player_a_stats, player_b_stats, recent_matches_a, recent_matches_b
    
# Function to process matches for a specific player
async def process_player_matches(player, recent_matches: pd.DataFrame, game):
    game_date = game['tourney_date']

    recent_matches = recent_matches.sort_values(by='tourney_date', ascending=False)

    recent_match_number = -1
    recent_matches_played = 0
    recent_elo_rating = START_RATING
    recent_point_elo_rating = START_RATING
    recent_game_elo_rating = START_RATING
    recent_set_elo_rating = START_RATING
    recent_service_game_elo_rating = START_RATING
    recent_return_game_elo_rating = START_RATING
    recent_tie_break_elo_rating = START_RATING
    
    print(game)

    for index, row in recent_matches.iterrows():
        recent_match_number = row['match_num']
        recent_matches_played += 1

        if row["winner_name"] == player:
            w_games, l_games, w_sets, l_sets, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played = get_score_stats(row)

            opponent_elo = row['oppo_elo_rating']
            opponent_set_elo = row['oppo_set_elo_rating']
            opponent_game_elo = row['oppo_game_elo_rating']
            opponent_point_elo = row['oppo_elo_rating']
            opponent_tb_elo = row['oppo_tb_elo_rating']
            opponent_service_elo = row['oppo_service_elo_rating']
            opponent_return_elo = row['oppo_return_elo_rating']

            # Primary update
            recent_elo_rating, _ = primary_elo(recent_elo_rating, opponent_elo, row)
            
            # Point Sets etc.
            recent_point_elo_rating, _, recent_set_elo_rating, _, recent_game_elo_rating, _ = points_sets_games_elo(
                recent_set_elo_rating, opponent_set_elo, recent_game_elo_rating, opponent_game_elo,
                recent_point_elo_rating, opponent_point_elo, row, w_sets, l_sets, w_games, l_games
            )

            # TB Update
            recent_tie_break_elo_rating, _ = tb_elo(recent_tie_break_elo_rating, opponent_tb_elo, row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)

            # Serve/Return Update
            try:
                recent_service_game_elo_rating, _, recent_return_game_elo_rating, _ = return_serve_elo(
                    recent_service_game_elo_rating, opponent_service_elo, recent_return_game_elo_rating, opponent_return_elo, row
                )
            except Exception as e:
                print(f"Skip worked {e}")
                pass  # Missing stats ignore

        else:
            w_games, l_games, w_sets, l_sets, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played = get_score_stats(row)

            opponent_elo = row['oppo_elo_rating']
            opponent_set_elo = row['oppo_set_elo_rating']
            opponent_game_elo = row['oppo_game_elo_rating']
            opponent_point_elo = row['oppo_elo_rating']
            opponent_tb_elo = row['oppo_tb_elo_rating']
            opponent_service_elo = row['oppo_service_elo_rating']
            opponent_return_elo = row['oppo_return_elo_rating']

            # Primary update
            _, recent_elo_rating = primary_elo(opponent_elo, recent_elo_rating, row)
            
            # Point Sets etc.
            _, recent_point_elo_rating, _, recent_set_elo_rating, _, recent_game_elo_rating = points_sets_games_elo(
                opponent_set_elo, recent_set_elo_rating, opponent_game_elo, recent_game_elo_rating,
                opponent_point_elo, recent_point_elo_rating, row, w_sets, l_sets, w_games, l_games
            )

            # TB Update
            _, recent_tie_break_elo_rating = tb_elo(opponent_tb_elo, recent_tie_break_elo_rating, row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)

            # Serve/Return Update
            try:
                _, recent_service_game_elo_rating, _, recent_return_game_elo_rating = return_serve_elo(
                    opponent_service_elo, recent_service_game_elo_rating, opponent_return_elo, recent_return_game_elo_rating, row
                )
            except Exception as e:
                print(f"Skip worked {e}")
                pass  # Missing stats ignore

    return [recent_match_number, recent_matches_played, recent_elo_rating, recent_point_elo_rating, recent_game_elo_rating, recent_set_elo_rating, recent_service_game_elo_rating, recent_return_game_elo_rating, recent_tie_break_elo_rating]

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

def update_dataframe(players_elo : pd.DataFrame, player_idx, col, value):
    players_elo.at[player_idx, col] = value

def primary_elo(rA, rB, row):
    delta = delta_rating(rA, rB, "N/A")

    rA_new = new_rating(rA, delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rB_new = new_rating(rB, -delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

    return rA_new, rB_new

def update_primary_elo(players_elo : pd.DataFrame, idxA, idxB, rA_new, rB_new, match_date, match_num):
    update_dataframe(players_elo, idxA, 'elo_rating', rA_new)
    update_dataframe(players_elo, idxA, 'last_date', match_date)
    update_dataframe(players_elo, idxA, 'match_number', match_num)
    update_dataframe(players_elo, idxA, 'matches_played', players_elo.at[idxA, 'matches_played'] + 1)
    update_dataframe(players_elo, idxB, 'elo_rating', rB_new)
    update_dataframe(players_elo, idxB, 'last_date', match_date)
    update_dataframe(players_elo, idxB, 'match_number', match_num)
    update_dataframe(players_elo, idxB, 'matches_played', players_elo.at[idxB, 'matches_played'] + 1)

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

def update_points_sets_games_elo(players_elo : pd.DataFrame, idxA, idxB, rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew):
    update_dataframe(players_elo, idxA, 'point_elo_rating', rApointNew)
    update_dataframe(players_elo, idxB, 'point_elo_rating', rBpointNew)

    update_dataframe(players_elo, idxA, 'set_elo_rating', rAsetNew)
    update_dataframe(players_elo, idxB, 'set_elo_rating', rBsetNew)

    update_dataframe(players_elo, idxA, 'game_elo_rating', rAgameNew)
    update_dataframe(players_elo, idxB, 'game_elo_rating', rBgameNew)

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

def update_tb_elo(players_elo : pd.DataFrame, idxA, idxB, rAtbNew, rBtbNew):
    update_dataframe(players_elo, idxA, 'tie_break_elo_rating', rAtbNew)
    update_dataframe(players_elo, idxB, 'tie_break_elo_rating', rBtbNew)

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

def update_return_serve_elo(players_elo : pd.DataFrame, idxA, idxB, rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew):
    update_dataframe(players_elo, idxA, 'service_game_elo_rating', rAserviceNew)
    update_dataframe(players_elo, idxB, 'service_game_elo_rating', rBserviceNew)

    update_dataframe(players_elo, idxA, 'return_game_elo_rating', rAreturnNew)
    update_dataframe(players_elo, idxB, 'return_game_elo_rating', rBreturnNew)

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
        "Hard": 3.8045, "Clay": 2.9240, "Grass": 4.6229
    }
    return surface_ratios.get(surface, 3.5727)

# # Ultimate Tennis Serve Rating = Ace % - Double Faults % + 1st Serve % + 1st Serve Points Won % + 2nd Serve Points Won % + Break Points Saved % + Service Games Won %
# # ATP Serve Rating Official = Aces - Double Faults + 1st Serve % + 1st Serve Points Won % + 2nd Serve Points Won % + Service Games Won %
def serve_rating(bp_faced, bp_saved, sv_gms):
    service_games_won_pct = (sv_gms - (bp_faced - bp_saved))/ sv_gms * 100 if sv_gms > 0 else 79
    return service_games_won_pct 
