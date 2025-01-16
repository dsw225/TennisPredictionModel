import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from render.utils.stephenson.stephenson_functions import *
import warnings
import traceback

async def filter_games(df: pd.DataFrame, end_date : datetime.date):
    warnings.filterwarnings("ignore", category=FutureWarning, message="Comparison of Timestamp with datetime.date is deprecated")
    df['last_date'] = pd.to_datetime(df['last_date'])
    df = df[~(
                (df['matches_played'] < MIN_MATCHES) |
                (df['last_date'].dt.date < (end_date - relativedelta(years=1)))
            )]
    df = df.sort_values(by='steph_rating', ascending=False)
    return df

async def update_stephs(players_steph : pd.DataFrame, row):
    try:
        player_a = players_steph[players_steph['player'] == row["winner_name"]]
        player_b = players_steph[players_steph['player'] == row["loser_name"]]

        if player_a.empty or player_b.empty:
            print("Error: One of the players not found in players_steph DataFrame")
            return

        idxA = player_a.index[0]
        idxB = player_b.index[0]
        
        w_games, l_games, w_sets, l_sets, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played = get_score_stats(row)

        #Primary update
        match_date = row['tourney_date']
        match_num = row['match_num']
        rA_new, rB_new = primary_steph(players_steph.at[idxA, 'steph_rating'], players_steph.at[idxB, 'steph_rating'], row)
        update_primary_steph(players_steph, idxA, idxB, rA_new, rB_new, match_date, match_num)
        
        # Point Sets etc.
        rAset = players_steph.at[idxA, 'set_steph_rating']
        rBset = players_steph.at[idxB, 'set_steph_rating']

        rAgame = players_steph.at[idxA, 'game_steph_rating']
        rBgame = players_steph.at[idxB, 'game_steph_rating']

        rApoint = players_steph.at[idxA, 'point_steph_rating']
        rBpoint = players_steph.at[idxB, 'point_steph_rating']

        rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew = points_sets_games_steph(rAset, rBset, rAgame, rBgame, rApoint, rBpoint, row, w_sets, l_sets, w_games, l_games)
        update_points_sets_games_steph(players_steph, idxA, idxB, rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew)

        # TB Update
        rAtbNew, rBtbNew = tb_steph(players_steph.at[idxA, 'tie_break_steph_rating'], players_steph.at[idxB, 'tie_break_steph_rating'], tie_breaks_won_winner, tie_breaks_played, row['tourney_date'])
        update_tb_steph(players_steph, idxA, idxB, rAtbNew, rBtbNew)


        # Serve/Return Update
        try:
            w_bp = row['w_bpSaved'] + (row['l_bpFaced'] - row['l_bpSaved'])
            bps = row['w_bpFaced'] + row['l_bpFaced']

            rAbpNew, rBbpNew = tb_steph(players_steph.at[idxA, 'bp_steph_rating'], players_steph.at[idxB, 'bp_steph_rating'], w_bp, bps, row['tourney_date'])
            update_bp_steph(players_steph, idxA, idxB, rAbpNew, rBbpNew)

            rAservice = players_steph.at[idxA, 'service_game_steph_rating']
            rBservice = players_steph.at[idxB, 'service_game_steph_rating']

            rAreturn = players_steph.at[idxA, 'return_game_steph_rating']
            rBreturn = players_steph.at[idxB, 'return_game_steph_rating']

            rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew = return_serve_steph(rAservice, rBservice, rAreturn, rBreturn, row)
            update_return_serve_steph(players_steph, idxA, idxB, rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew)

            rAace = players_steph.at[idxA, 'ace_steph_rating']
            rBace = players_steph.at[idxB, 'ace_steph_rating']

            rAaceReturn = players_steph.at[idxA, 'return_ace_steph_rating']
            rBaceReturn = players_steph.at[idxB, 'return_ace_steph_rating']

            rAaceNew, rBaceNew, rAaceReturnNew, rBaceReturnNew = ace_steph(rAace, rBace, rAaceReturn, rBaceReturn, row)
            update_ace_steph(players_steph, idxA, idxB, rAaceNew, rBaceNew, rAaceReturnNew, rBaceReturnNew)

            rAfw = players_steph.at[idxA, 'first_won_steph_rating']
            rBfw = players_steph.at[idxB, 'first_won_steph_rating']

            rAvFw = players_steph.at[idxA, 'return_first_won_steph_rating']
            rBvFw = players_steph.at[idxB, 'return_first_won_steph_rating']

            rAfwNew, rBfwNew, rAvFwNew, rBvFwNew = first_won_steph(rAfw, rBfw, rAvFw, rBvFw, row)
            update_first_won_steph(players_steph, idxA, idxB, rAfwNew, rBfwNew, rAvFwNew, rBvFwNew)

            rAsw = players_steph.at[idxA, 'second_won_steph_rating']
            rBsw = players_steph.at[idxB, 'second_won_steph_rating']

            rAvSw = players_steph.at[idxA, 'second_won_steph_rating']
            rBvSw = players_steph.at[idxB, 'second_won_steph_rating']

            rAswNew, rBswNew, rAvSwNew, rBvSwNew = second_won_steph(rAsw, rBsw, rAvSw, rBvSw, row)
            update_second_won_steph(players_steph, idxA, idxB, rAswNew, rBswNew, rAvSwNew, rBvSwNew)
        except Exception as b:
            # print(f"Skip worked {b}")
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

    # steph Stats Calced
    player_a_stats = await process_player_matches(player_a, recent_matches_a, game)
    player_b_stats = await process_player_matches(player_b, recent_matches_b, game)

    changed_headers = {
        'recent_match_number': 'oppo_recent_match_number',
        'recent_matches_played': 'oppo_recent_matches_played',
        'recent_steph_rating': 'oppo_recent_steph_rating',
        'recent_point_steph_rating': 'oppo_recent_point_steph_rating',
        'recent_game_steph_rating': 'oppo_recent_game_steph_rating',
        'recent_set_steph_rating': 'oppo_recent_set_steph_rating',
        'recent_service_game_steph_rating': 'oppo_recent_service_game_steph_rating',
        'recent_return_game_steph_rating': 'oppo_recent_return_game_steph_rating',
        'recent_tie_break_steph_rating': 'oppo_recent_tie_break_steph_rating',
        'recent_bp_steph_rating': 'oppo_recent_bp_steph_rating',
        "recent_ace_steph_rating": "oppo_recent_ace_steph_rating",
        "recent_return_ace_steph_rating": "oppo_recent_return_ace_steph_rating",
        "recent_first_won_steph_rating": "oppo_recent_first_won_steph_rating",
        "recent_return_first_won_steph_rating": "oppo_recent_return_first_won_steph_rating",
        "recent_second_won_steph_rating": "oppo_recent_second_won_steph_rating",
        "recent_return_second_won_steph_rating": "oppo_recent_return_second_won_steph_rating",

        'recent_surface_match_number': 'oppo_recent_surface_match_number',
        'recent_surface_matches_played': 'oppo_recent_surface_matches_played',
        'recent_surface_steph_rating': 'oppo_recent_surface_steph_rating',
        'recent_surface_point_steph_rating': 'oppo_recent_surface_point_steph_rating',
        'recent_surface_game_steph_rating': 'oppo_recent_surface_game_steph_rating',
        'recent_surface_set_steph_rating': 'oppo_recent_surface_set_steph_rating',
        'recent_surface_service_game_steph_rating': 'oppo_recent_surface_service_game_steph_rating',
        'recent_surface_return_game_steph_rating': 'oppo_recent_surface_return_game_steph_rating',
        'recent_surface_tie_break_steph_rating': 'oppo_recent_surface_tie_break_steph_rating',
        'recent_surface_bp_steph_rating': 'oppo_recent_surface_bp_steph_rating',
        "recent_surface_ace_steph_rating": "oppo_recent_surface_ace_steph_rating",
        "recent_surface_return_ace_steph_rating": "oppo_recent_surface_return_ace_steph_rating",
        "recent_surface_first_won_steph_rating": "oppo_recent_surface_first_won_steph_rating",
        "recent_surface_return_first_won_steph_rating": "oppo_recent_surface_return_first_won_steph_rating",
        "recent_surface_second_won_steph_rating": "oppo_recent_surface_second_won_steph_rating",
        "recent_surface_return_second_won_steph_rating": "oppo_recent_surface_return_second_won_steph_rating",
    }

    oppo_player_a_stats = player_a_stats.rename(index=changed_headers)
    oppo_player_b_stats = player_b_stats.rename(index=changed_headers)

    player_a_entry = pd.concat([game, oppo_player_b_stats])
    player_b_entry = pd.concat([game, oppo_player_a_stats])

    # Append the data to the DataFrames
    recent_matches_a = pd.concat([recent_matches_a, pd.DataFrame([player_a_entry])], ignore_index=True)
    recent_matches_b = pd.concat([recent_matches_b, pd.DataFrame([player_b_entry])], ignore_index=True)

    # a_headers = {key: f'a_{key}' for key in player_a_stats.index}
    # player_a_stats = player_a_stats.rename(index=a_headers)

    # b_headers = {key: f'b_{key}' for key in player_b_stats.index}
    # player_b_stats = player_b_stats.rename(index=b_headers)

    # combined_stats = {**player_a_stats, **player_b_stats}

    return player_a_stats, player_b_stats, recent_matches_a, recent_matches_b

# Function to process matches for a specific player
async def process_player_matches(player, recent_matches: pd.DataFrame, game):
    def initialize_result():
        return {
            "recent_match_number": -1,
            "recent_matches_played": 0,
            "recent_steph_rating": Stephenson(),
            "recent_point_steph_rating": Stephenson(),
            "recent_game_steph_rating": Stephenson(),
            "recent_set_steph_rating": Stephenson(),
            "recent_service_game_steph_rating": Stephenson(),
            "recent_return_game_steph_rating": Stephenson(),
            "recent_tie_break_steph_rating": Stephenson(),
            "recent_bp_steph_rating": Stephenson(),
            "recent_ace_steph_rating": Stephenson(),
            "recent_return_ace_steph_rating": Stephenson(),
            "recent_first_won_steph_rating": Stephenson(),
            "recent_return_first_won_steph_rating": Stephenson(),
            "recent_second_won_steph_rating": Stephenson(),
            "recent_return_second_won_steph_rating": Stephenson(),

            "recent_surface_match_number": -1,
            "recent_surface_matches_played": 0,
            "recent_surface_steph_rating": Stephenson(),
            "recent_surface_point_steph_rating": Stephenson(),
            "recent_surface_game_steph_rating": Stephenson(),
            "recent_surface_set_steph_rating": Stephenson(),
            "recent_surface_service_game_steph_rating": Stephenson(),
            "recent_surface_return_game_steph_rating": Stephenson(),
            "recent_surface_tie_break_steph_rating": Stephenson(),
            "recent_surface_bp_steph_rating": Stephenson(),
            "recent_surface_ace_steph_rating": Stephenson(),
            "recent_surface_return_ace_steph_rating": Stephenson(),
            "recent_surface_first_won_steph_rating": Stephenson(),
            "recent_surface_return_first_won_steph_rating": Stephenson(),
            "recent_surface_second_won_steph_rating": Stephenson(),
            "recent_surface_return_second_won_steph_rating": Stephenson(),
        }

    def update_result_for_match(result, row, player, game_date, surface):
        if row["winner_name"] == player:
            update_steph_ratings(result, row, surface, update_latest=False)
        else:
            update_steph_ratings(result, row, surface, update_latest=False, is_winner=False)

    def update_steph_ratings(result, row, surface, update_latest, is_winner=True):
        w_games, l_games, w_sets, l_sets, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played = get_score_stats(row)
        if not isinstance(row, pd.Series):
            raise TypeError(f"Expected pandas Series but got {type(row)}")
        prefix = "recent_"

        result[f"{prefix}match_number"] = row['match_num']
        result[f"{prefix}matches_played"] += 1
        
        steph_keys = {
            "steph_rating": f"{prefix}steph_rating",
            "point_steph_rating": f"{prefix}point_steph_rating",
            "game_steph_rating": f"{prefix}game_steph_rating",
            "set_steph_rating": f"{prefix}set_steph_rating",
            "service_game_steph_rating": f"{prefix}service_game_steph_rating",
            "return_game_steph_rating": f"{prefix}return_game_steph_rating",
            "tie_break_steph_rating": f"{prefix}tie_break_steph_rating",
            "bp_steph_rating": f"{prefix}bp_steph_rating",
            "ace_steph_rating": f"{prefix}ace_steph_rating",
            "return_ace_steph_rating": f"{prefix}return_ace_steph_rating",
            "first_won_steph_rating": f"{prefix}first_won_steph_rating",
            "return_first_won_steph_rating": f"{prefix}return_first_won_steph_rating",
            "second_won_steph_rating": f"{prefix}second_won_steph_rating",
            "return_second_won_steph_rating": f"{prefix}return_second_won_steph_rating",

            "surface_steph_rating": f"{prefix}surface_steph_rating",
            "surface_point_steph_rating": f"{prefix}surface_point_steph_rating",
            "surface_game_steph_rating": f"{prefix}surface_game_steph_rating",
            "surface_set_steph_rating": f"{prefix}surface_set_steph_rating",
            "surface_service_game_steph_rating": f"{prefix}surface_service_game_steph_rating",
            "surface_return_game_steph_rating": f"{prefix}surface_return_game_steph_rating",
            "surface_tie_break_steph_rating": f"{prefix}surface_tie_break_steph_rating",
            "surface_bp_steph_rating": f"{prefix}surface_bp_steph_rating",
            "surface_ace_steph_rating": f"{prefix}surface_ace_steph_rating",
            "surface_return_ace_steph_rating": f"{prefix}surface_return_ace_steph_rating",
            "surface_first_won_steph_rating": f"{prefix}surface_first_won_steph_rating",
            "surface_return_first_won_steph_rating": f"{prefix}surface_return_first_won_steph_rating",
            "surface_second_won_steph_rating": f"{prefix}surface_second_won_steph_rating",
            "surface_return_second_won_steph_rating": f"{prefix}surface_return_second_won_steph_rating",
        }

        opponent_steph = {
            "steph_rating": row[f'oppo_{prefix}steph_rating'] if ~np.isnan(row[f'oppo_{prefix}steph_rating']) else 1500,
            "point_steph_rating": row[f'oppo_{prefix}point_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}point_steph_rating']) else 1500,
            "game_steph_rating": row[f'oppo_{prefix}game_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}game_steph_rating']) else 1500,
            "set_steph_rating": row[f'oppo_{prefix}set_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}set_steph_rating']) else 1500,
            "service_game_steph_rating": row[f'oppo_{prefix}service_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}service_steph_rating']) else 1500,
            "return_game_steph_rating": row[f'oppo_{prefix}return_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}return_steph_rating']) else 1500,
            "tie_break_steph_rating": row[f'oppo_{prefix}tb_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}tb_steph_rating']) else 1500,
            "bp_steph_rating": row[f'oppo_{prefix}tb_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}tb_steph_rating']) else 1500,
            "ace_steph_rating": row[f'oppo_{prefix}ace_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}ace_steph_rating']) else 1500,
            "return_ace_steph_rating": row[f'oppo_{prefix}return_ace_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}return_ace_steph_rating']) else 1500,
            "first_won_steph_rating": row[f'oppo_{prefix}first_won_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}first_won_steph_rating']) else 1500,
            "return_first_won_steph_rating": row[f'oppo_{prefix}return_first_won_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}return_first_won_steph_rating']) else 1500,
            "second_won_steph_rating": row[f'oppo_{prefix}second_won_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}second_won_steph_rating']) else 1500,
            "return_second_won_steph_rating": row[f'oppo_{prefix}return_second_won_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}return_second_won_steph_rating']) else 1500,

            "surface_steph_rating": row[f'oppo_{prefix}surface_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_steph_rating']) else 1500,
            "surface_point_steph_rating": row[f'oppo_{prefix}surface_point_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_point_steph_rating']) else 1500,
            "surface_game_steph_rating": row[f'oppo_{prefix}surface_game_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_game_steph_rating']) else 1500,
            "surface_set_steph_rating": row[f'oppo_{prefix}surface_set_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_set_steph_rating']) else 1500,
            "surface_service_game_steph_rating": row[f'oppo_{prefix}surface_service_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_service_steph_rating']) else 1500,
            "surface_return_game_steph_rating": row[f'oppo_{prefix}surface_return_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_steph_rating']) else 1500,
            "surface_tie_break_steph_rating": row[f'oppo_{prefix}surface_tb_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_tb_steph_rating']) else 1500,
            "surface_bp_steph_rating": row[f'oppo_{prefix}surface_tb_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_tb_steph_rating']) else 1500,
            "surface_ace_steph_rating": row[f'oppo_{prefix}surface_ace_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_ace_steph_rating']) else 1500,
            "surface_return_ace_steph_rating": row[f'oppo_{prefix}surface_return_ace_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_ace_steph_rating']) else 1500,
            "surface_first_won_steph_rating": row[f'oppo_{prefix}surface_first_won_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_first_won_steph_rating']) else 1500,
            "surface_return_first_won_steph_rating": row[f'oppo_{prefix}surface_return_first_won_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_first_won_steph_rating']) else 1500,
            "surface_second_won_steph_rating": row[f'oppo_{prefix}surface_second_won_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_second_won_steph_rating']) else 1500,
            "surface_return_second_won_steph_rating": row[f'oppo_{prefix}surface_return_second_won_steph_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_second_won_steph_rating']) else 1500,
        }
        
        if is_winner:
            if surface == row["surface"]:
                result[f"{prefix}surface_match_number"] = row['match_num']
                result[f"{prefix}surface_matches_played"] += 1

                result[steph_keys["surface_steph_rating"]], _ = primary_steph(result[steph_keys["steph_rating"]], opponent_steph["surface_steph_rating"], row)
                result[steph_keys["surface_point_steph_rating"]], _, result[steph_keys["surface_set_steph_rating"]], _, result[steph_keys["surface_game_steph_rating"]], _ = points_sets_games_steph(
                    result[steph_keys["surface_set_steph_rating"]], opponent_steph["surface_set_steph_rating"], result[steph_keys["surface_game_steph_rating"]], opponent_steph["surface_game_steph_rating"],
                    result[steph_keys["surface_point_steph_rating"]], opponent_steph["surface_point_steph_rating"], row, w_sets, l_sets, w_games, l_games
                )
                try:
                    result[steph_keys["surface_tie_break_steph_rating"]], _ = tb_steph(result[steph_keys["surface_tie_break_steph_rating"]], opponent_steph["surface_tie_break_steph_rating"], tie_breaks_won_winner, tie_breaks_played)
                    result[steph_keys["surface_bp_steph_rating"]], _ = bp_steph(result[steph_keys["surface_bp_steph_rating"]], opponent_steph["surface_bp_steph_rating"], row)
                    
                    result[steph_keys["surface_service_game_steph_rating"]], _, result[steph_keys["surface_return_game_steph_rating"]], _ = return_serve_steph(
                        result[steph_keys["surface_service_game_steph_rating"]], opponent_steph["surface_service_game_steph_rating"], result[steph_keys["surface_return_game_steph_rating"]], opponent_steph["surface_return_game_steph_rating"], row
                    )

                    result[steph_keys["surface_ace_steph_rating"]], _, result[steph_keys["surface_return_ace_steph_rating"]], _ = ace_steph(
                        result[steph_keys["surface_ace_steph_rating"]], opponent_steph["surface_ace_steph_rating"], result[steph_keys["surface_return_ace_steph_rating"]], opponent_steph["surface_return_ace_steph_rating"], row
                    )

                    result[steph_keys["surface_first_won_steph_rating"]], _, result[steph_keys["surface_return_first_won_steph_rating"]], _ = first_won_steph(
                        result[steph_keys["surface_first_won_steph_rating"]], opponent_steph["surface_first_won_steph_rating"], result[steph_keys["surface_return_first_won_steph_rating"]], opponent_steph["surface_return_first_won_steph_rating"], row
                    )

                    result[steph_keys["surface_second_won_steph_rating"]], _, result[steph_keys["surface_return_second_won_steph_rating"]], _ = second_won_steph(
                        result[steph_keys["surface_second_won_steph_rating"]], opponent_steph["surface_second_won_steph_rating"], result[steph_keys["surface_return_second_won_steph_rating"]], opponent_steph["surface_return_second_won_steph_rating"], row
                    )
                except Exception as e:
                    # print(f"Skip worked {e}")
                    pass  # Missing stats ignore

            result[steph_keys["steph_rating"]], _ = primary_steph(result[steph_keys["steph_rating"]], opponent_steph["steph_rating"], row)
            result[steph_keys["point_steph_rating"]], _, result[steph_keys["set_steph_rating"]], _, result[steph_keys["game_steph_rating"]], _ = points_sets_games_steph(
                result[steph_keys["set_steph_rating"]], opponent_steph["set_steph_rating"], result[steph_keys["game_steph_rating"]], opponent_steph["game_steph_rating"],
                result[steph_keys["point_steph_rating"]], opponent_steph["point_steph_rating"], row, w_sets, l_sets, w_games, l_games
            )
            result[steph_keys["tie_break_steph_rating"]], _ = tb_steph(result[steph_keys["tie_break_steph_rating"]], opponent_steph["tie_break_steph_rating"], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
            try:
                result[steph_keys["service_game_steph_rating"]], _, result[steph_keys["return_game_steph_rating"]], _ = return_serve_steph(
                    result[steph_keys["service_game_steph_rating"]], opponent_steph["service_game_steph_rating"], result[steph_keys["return_game_steph_rating"]], opponent_steph["return_game_steph_rating"], row
                )

                result[steph_keys["ace_steph_rating"]], _, result[steph_keys["return_ace_steph_rating"]], _ = ace_steph(
                    result[steph_keys["ace_steph_rating"]], opponent_steph["ace_steph_rating"], result[steph_keys["return_ace_steph_rating"]], opponent_steph["return_ace_steph_rating"], row
                )

                result[steph_keys["first_won_steph_rating"]], _, result[steph_keys["return_first_won_steph_rating"]], _ = first_won_steph(
                    result[steph_keys["first_won_steph_rating"]], opponent_steph["first_won_steph_rating"], result[steph_keys["return_first_won_steph_rating"]], opponent_steph["return_first_won_steph_rating"], row
                )

                result[steph_keys["second_won_steph_rating"]], _, result[steph_keys["return_second_won_steph_rating"]], _ = second_won_steph(
                    result[steph_keys["second_won_steph_rating"]], opponent_steph["second_won_steph_rating"], result[steph_keys["return_second_won_steph_rating"]], opponent_steph["return_second_won_steph_rating"], row
                )
            except Exception as e:
                # print(f"Skip worked {e}")
                pass  # Missing stats ignore
        else:
            if surface == row["surface"]:
                result[f"{prefix}surface_match_number"] = row['match_num']
                result[f"{prefix}surface_matches_played"] += 1

                _, result[steph_keys["surface_steph_rating"]] = primary_steph(opponent_steph["surface_steph_rating"], result[steph_keys["surface_steph_rating"]], row)
                _, result[steph_keys["surface_point_steph_rating"]], _, result[steph_keys["surface_set_steph_rating"]], _, result[steph_keys["surface_game_steph_rating"]] = points_sets_games_steph(
                    opponent_steph["surface_set_steph_rating"], result[steph_keys["surface_set_steph_rating"]], opponent_steph["surface_game_steph_rating"], result[steph_keys["surface_game_steph_rating"]],
                    opponent_steph["surface_point_steph_rating"], result[steph_keys["surface_point_steph_rating"]], row, w_sets, l_sets, w_games, l_games
                )
                _, result[steph_keys["surface_tie_break_steph_rating"]] = tb_steph(opponent_steph["surface_tie_break_steph_rating"], result[steph_keys["surface_tie_break_steph_rating"]], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
                try:
                    _, result[steph_keys["surface_service_game_steph_rating"]], _, result[steph_keys["surface_return_game_steph_rating"]] = return_serve_steph(
                        opponent_steph["surface_service_game_steph_rating"], result[steph_keys["surface_service_game_steph_rating"]], opponent_steph["surface_return_game_steph_rating"], result[steph_keys["surface_return_game_steph_rating"]], row
                    )

                    _, result[steph_keys["surface_ace_steph_rating"]], _, result[steph_keys["surface_return_ace_steph_rating"]] = ace_steph(
                        opponent_steph["surface_ace_steph_rating"], result[steph_keys["surface_ace_steph_rating"]], opponent_steph["surface_return_ace_steph_rating"], result[steph_keys["surface_return_ace_steph_rating"]], row
                    )

                    _, result[steph_keys["surface_first_won_steph_rating"]], _, result[steph_keys["surface_return_first_won_steph_rating"]] = first_won_steph(
                        opponent_steph["surface_first_won_steph_rating"], result[steph_keys["surface_first_won_steph_rating"]], opponent_steph["surface_return_first_won_steph_rating"], result[steph_keys["surface_return_first_won_steph_rating"]], row
                    )

                    _, result[steph_keys["surface_second_won_steph_rating"]], _, result[steph_keys["surface_return_second_won_steph_rating"]] = second_won_steph(
                        opponent_steph["surface_second_won_steph_rating"], result[steph_keys["surface_second_won_steph_rating"]], opponent_steph["surface_return_second_won_steph_rating"], result[steph_keys["surface_return_second_won_steph_rating"]], row
                    )
                except Exception as e:
                    # print(f"Skip worked {e}")
                    pass  # Missing stats ignore

            _, result[steph_keys["steph_rating"]] = primary_steph(opponent_steph["steph_rating"], result[steph_keys["steph_rating"]], row)
            _, result[steph_keys["point_steph_rating"]], _, result[steph_keys["set_steph_rating"]], _, result[steph_keys["game_steph_rating"]] = points_sets_games_steph(
                opponent_steph["set_steph_rating"], result[steph_keys["set_steph_rating"]], opponent_steph["game_steph_rating"], result[steph_keys["game_steph_rating"]],
                opponent_steph["point_steph_rating"], result[steph_keys["point_steph_rating"]], row, w_sets, l_sets, w_games, l_games
            )
            _, result[steph_keys["tie_break_steph_rating"]] = tb_steph(opponent_steph["tie_break_steph_rating"], result[steph_keys["tie_break_steph_rating"]], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
            try:
                _, result[steph_keys["service_game_steph_rating"]], _, result[steph_keys["return_game_steph_rating"]] = return_serve_steph(
                    opponent_steph["service_game_steph_rating"], result[steph_keys["service_game_steph_rating"]], opponent_steph["return_game_steph_rating"], result[steph_keys["return_game_steph_rating"]], row
                )

                _, result[steph_keys["ace_steph_rating"]], _, result[steph_keys["return_ace_steph_rating"]] = ace_steph(
                    opponent_steph["ace_steph_rating"], result[steph_keys["ace_steph_rating"]], opponent_steph["return_ace_steph_rating"], result[steph_keys["return_ace_steph_rating"]], row
                )

                _, result[steph_keys["first_won_steph_rating"]], _, result[steph_keys["return_first_won_steph_rating"]] = first_won_steph(
                    opponent_steph["first_won_steph_rating"], result[steph_keys["first_won_steph_rating"]], opponent_steph["return_first_won_steph_rating"], result[steph_keys["return_first_won_steph_rating"]], row
                )

                _, result[steph_keys["second_won_steph_rating"]], _, result[steph_keys["return_second_won_steph_rating"]] = second_won_steph(
                    opponent_steph["second_won_steph_rating"], result[steph_keys["second_won_steph_rating"]], opponent_steph["return_second_won_steph_rating"], result[steph_keys["return_second_won_steph_rating"]], row
                )
            except Exception as e:
                # print(f"Skip worked {e}")
                pass  # Missing stats ignore

    # Main processing
    game_date = game['tourney_date']
    surface = game['surface']
    recent_matches = recent_matches.sort_values(by='tourney_date', ascending=True)

    result = initialize_result()

    for index, row in recent_matches.iterrows():
        update_result_for_match(result, row, player, game_date, surface)

    return pd.Series(result)

def update_dataframe(players_steph : pd.DataFrame, player_idx, col, value):
    players_steph.at[player_idx, col] = value

def update_primary_steph(players_steph : pd.DataFrame, idxA, idxB, rA_new, rB_new, match_date, match_num):
    update_dataframe(players_steph, idxA, 'steph_rating', rA_new)
    update_dataframe(players_steph, idxA, 'last_date', match_date)
    update_dataframe(players_steph, idxA, 'match_number', match_num)
    update_dataframe(players_steph, idxA, 'matches_played', players_steph.at[idxA, 'matches_played'] + 1)
    update_dataframe(players_steph, idxB, 'steph_rating', rB_new)
    update_dataframe(players_steph, idxB, 'last_date', match_date)
    update_dataframe(players_steph, idxB, 'match_number', match_num)
    update_dataframe(players_steph, idxB, 'matches_played', players_steph.at[idxB, 'matches_played'] + 1)

def update_points_sets_games_steph(players_steph : pd.DataFrame, idxA, idxB, rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew):
    update_dataframe(players_steph, idxA, 'point_steph_rating', rApointNew)
    update_dataframe(players_steph, idxB, 'point_steph_rating', rBpointNew)

    update_dataframe(players_steph, idxA, 'set_steph_rating', rAsetNew)
    update_dataframe(players_steph, idxB, 'set_steph_rating', rBsetNew)

    update_dataframe(players_steph, idxA, 'game_steph_rating', rAgameNew)
    update_dataframe(players_steph, idxB, 'game_steph_rating', rBgameNew)

def update_tb_steph(players_steph : pd.DataFrame, idxA, idxB, rAtbNew, rBtbNew):
    update_dataframe(players_steph, idxA, 'tie_break_steph_rating', rAtbNew)
    update_dataframe(players_steph, idxB, 'tie_break_steph_rating', rBtbNew)
    
def update_bp_steph(players_steph : pd.DataFrame, idxA, idxB, rAtbNew, rBtbNew):
    update_dataframe(players_steph, idxA, 'bp_steph_rating', rAtbNew)
    update_dataframe(players_steph, idxB, 'bp_steph_rating', rBtbNew)

def update_return_serve_steph(players_steph : pd.DataFrame, idxA, idxB, rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew):
    update_dataframe(players_steph, idxA, 'service_game_steph_rating', rAserviceNew)
    update_dataframe(players_steph, idxB, 'service_game_steph_rating', rBserviceNew)

    update_dataframe(players_steph, idxA, 'return_game_steph_rating', rAreturnNew)
    update_dataframe(players_steph, idxB, 'return_game_steph_rating', rBreturnNew)

def update_ace_steph(players_steph : pd.DataFrame, idxA, idxB, rAaceNew, rBaceNew, rAaceReturnNew, rBaceReturnNew):
    update_dataframe(players_steph, idxA, 'ace_steph_rating', rAaceNew)
    update_dataframe(players_steph, idxB, 'ace_steph_rating', rBaceNew)

    update_dataframe(players_steph, idxA, 'return_ace_steph_rating', rAaceReturnNew)
    update_dataframe(players_steph, idxB, 'return_ace_steph_rating', rBaceReturnNew)

def update_first_won_steph(players_steph : pd.DataFrame, idxA, idxB, rAfwNew, rBfwNew, rAvFwNew, rBvFwNew):
    update_dataframe(players_steph, idxA, 'first_won_steph_rating', rAfwNew)
    update_dataframe(players_steph, idxB, 'first_won_steph_rating', rBfwNew)

    update_dataframe(players_steph, idxA, 'return_first_won_steph_rating', rAvFwNew)
    update_dataframe(players_steph, idxB, 'return_first_won_steph_rating', rBvFwNew)

def update_second_won_steph(players_steph : pd.DataFrame, idxA, idxB, rAfwNew, rBfwNew, rAvFwNew, rBvFwNew):
    update_dataframe(players_steph, idxA, 'second_won_steph_rating', rAfwNew)
    update_dataframe(players_steph, idxB, 'second_won_steph_rating', rBfwNew)

    update_dataframe(players_steph, idxA, 'return_second_won_steph_rating', rAvFwNew)
    update_dataframe(players_steph, idxB, 'return_second_won_steph_rating', rBvFwNew)