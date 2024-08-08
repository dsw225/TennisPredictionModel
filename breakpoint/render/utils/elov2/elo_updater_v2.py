import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from render.utils.elov2.elo_functions_v2 import *
import warnings
import traceback

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

    # Elo Stats Calced
    player_a_stats = await process_player_matches(player_a, recent_matches_a, game)
    player_b_stats = await process_player_matches(player_b, recent_matches_b, game)

    changed_headers = {
        'recent_match_number': 'oppo_recent_match_number',
        'recent_matches_played': 'oppo_recent_matches_played',
        'recent_elo_rating': 'oppo_recent_elo_rating',
        'recent_point_elo_rating': 'oppo_recent_point_elo_rating',
        'recent_game_elo_rating': 'oppo_recent_game_elo_rating',
        'recent_set_elo_rating': 'oppo_recent_set_elo_rating',
        'recent_service_game_elo_rating': 'oppo_recent_service_game_elo_rating',
        'recent_return_game_elo_rating': 'oppo_recent_return_game_elo_rating',
        'recent_tie_break_elo_rating': 'oppo_recent_tie_break_elo_rating',
        "recent_ace_elo_rating": "oppo_recent_ace_elo_rating",
        "recent_return_ace_elo_rating": "oppo_recent_return_ace_elo_rating",
        "recent_first_won_elo_rating": "oppo_recent_first_won_elo_rating",
        "recent_return_first_won_elo_rating": "oppo_recent_return_first_won_elo_rating",
        "recent_second_won_elo_rating": "oppo_recent_second_won_elo_rating",
        "recent_return_second_won_elo_rating": "oppo_recent_return_second_won_elo_rating",

        'recent_surface_match_number': 'oppo_recent_surface_match_number',
        'recent_surface_matches_played': 'oppo_recent_surface_matches_played',
        'recent_surface_elo_rating': 'oppo_recent_surface_elo_rating',
        'recent_surface_point_elo_rating': 'oppo_recent_surface_point_elo_rating',
        'recent_surface_game_elo_rating': 'oppo_recent_surface_game_elo_rating',
        'recent_surface_set_elo_rating': 'oppo_recent_surface_set_elo_rating',
        'recent_surface_service_game_elo_rating': 'oppo_recent_surface_service_game_elo_rating',
        'recent_surface_return_game_elo_rating': 'oppo_recent_surface_return_game_elo_rating',
        'recent_surface_tie_break_elo_rating': 'oppo_recent_surface_tie_break_elo_rating',
        "recent_surface_ace_elo_rating": "oppo_recent_surface_ace_elo_rating",
        "recent_surface_return_ace_elo_rating": "oppo_recent_surface_return_ace_elo_rating",
        "recent_surface_first_won_elo_rating": "oppo_recent_surface_first_won_elo_rating",
        "recent_surface_return_first_won_elo_rating": "oppo_recent_surface_return_first_won_elo_rating",
        "recent_surface_second_won_elo_rating": "oppo_recent_surface_second_won_elo_rating",
        "recent_surface_return_second_won_elo_rating": "oppo_recent_surface_return_second_won_elo_rating",

        'latest_match_number': 'oppo_latest_match_number',
        'latest_matches_played': 'oppo_latest_matches_played',
        'latest_elo_rating': 'oppo_latest_elo_rating',
        'latest_point_elo_rating': 'oppo_latest_point_elo_rating',
        'latest_game_elo_rating': 'oppo_latest_game_elo_rating',
        'latest_set_elo_rating': 'oppo_latest_set_elo_rating',
        'latest_service_game_elo_rating': 'oppo_latest_service_game_elo_rating',
        'latest_return_game_elo_rating': 'oppo_latest_return_game_elo_rating',
        'latest_tie_break_elo_rating': 'oppo_latest_tie_break_elo_rating',
        "latest_ace_elo_rating": "oppo_latest_ace_elo_rating",
        "latest_return_ace_elo_rating": "oppo_latest_return_ace_elo_rating",
        "latest_first_won_elo_rating": "oppo_latest_first_won_elo_rating",
        "latest_return_first_won_elo_rating": "oppo_latest_return_first_won_elo_rating",
        "latest_second_won_elo_rating": "oppo_latest_second_won_elo_rating",
        "latest_return_second_won_elo_rating": "oppo_latest_return_second_won_elo_rating",

        'latest_surface_match_number': 'oppo_latest_surface_match_number',
        'latest_surface_matches_played': 'oppo_latest_surface_matches_played',
        'latest_surface_elo_rating': 'oppo_latest_surface_elo_rating',
        'latest_surface_point_elo_rating': 'oppo_latest_surface_point_elo_rating',
        'latest_surface_game_elo_rating': 'oppo_latest_surface_game_elo_rating',
        'latest_surface_set_elo_rating': 'oppo_latest_surface_set_elo_rating',
        'latest_surface_service_game_elo_rating': 'oppo_latest_surface_service_game_elo_rating',
        'latest_surface_return_game_elo_rating': 'oppo_latest_surface_return_game_elo_rating',
        'latest_surface_tie_break_elo_rating': 'oppo_latest_surface_tie_break_elo_rating',
        "latest_surface_ace_elo_rating": "oppo_latest_surface_ace_elo_rating",
        "latest_surface_return_ace_elo_rating": "oppo_latest_surface_return_ace_elo_rating",
        "latest_surface_first_won_elo_rating": "oppo_latest_surface_first_won_elo_rating",
        "latest_surface_return_first_won_elo_rating": "oppo_latest_surface_return_first_won_elo_rating",
        "latest_surface_second_won_elo_rating": "oppo_latest_surface_second_won_elo_rating",
        "latest_surface_return_second_won_elo_rating": "oppo_latest_surface_return_second_won_elo_rating",
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
            "recent_elo_rating": START_RATING,
            "recent_point_elo_rating": START_RATING,
            "recent_game_elo_rating": START_RATING,
            "recent_set_elo_rating": START_RATING,
            "recent_service_game_elo_rating": START_RATING,
            "recent_return_game_elo_rating": START_RATING,
            "recent_tie_break_elo_rating": START_RATING,
            "recent_ace_elo_rating": START_RATING,
            "recent_return_ace_elo_rating": START_RATING,
            "recent_first_won_elo_rating": START_RATING,
            "recent_return_first_won_elo_rating": START_RATING,
            "recent_second_won_elo_rating": START_RATING,
            "recent_return_second_won_elo_rating": START_RATING,

            "recent_surface_match_number": -1,
            "recent_surface_matches_played": 0,
            "recent_surface_elo_rating": START_RATING,
            "recent_surface_point_elo_rating": START_RATING,
            "recent_surface_game_elo_rating": START_RATING,
            "recent_surface_set_elo_rating": START_RATING,
            "recent_surface_service_game_elo_rating": START_RATING,
            "recent_surface_return_game_elo_rating": START_RATING,
            "recent_surface_tie_break_elo_rating": START_RATING,
            "recent_surface_ace_elo_rating": START_RATING,
            "recent_surface_return_ace_elo_rating": START_RATING,
            "recent_surface_first_won_elo_rating": START_RATING,
            "recent_surface_return_first_won_elo_rating": START_RATING,
            "recent_surface_second_won_elo_rating": START_RATING,
            "recent_surface_return_second_won_elo_rating": START_RATING,

            "latest_match_number": -1,
            "latest_matches_played": 0,
            "latest_elo_rating": START_RATING,
            "latest_point_elo_rating": START_RATING,
            "latest_game_elo_rating": START_RATING,
            "latest_set_elo_rating": START_RATING,
            "latest_service_game_elo_rating": START_RATING,
            "latest_return_game_elo_rating": START_RATING,
            "latest_tie_break_elo_rating": START_RATING,
            "latest_ace_elo_rating": START_RATING,
            "latest_return_ace_elo_rating": START_RATING,
            "latest_first_won_elo_rating": START_RATING,
            "latest_return_first_won_elo_rating": START_RATING,
            "latest_second_won_elo_rating": START_RATING,
            "latest_return_second_won_elo_rating": START_RATING,

            "latest_surface_match_number": -1,
            "latest_surface_matches_played": 0,
            "latest_surface_elo_rating": START_RATING,
            "latest_surface_point_elo_rating": START_RATING,
            "latest_surface_game_elo_rating": START_RATING,
            "latest_surface_set_elo_rating": START_RATING,
            "latest_surface_service_game_elo_rating": START_RATING,
            "latest_surface_return_game_elo_rating": START_RATING,
            "latest_surface_tie_break_elo_rating": START_RATING,
            "latest_surface_ace_elo_rating": START_RATING,
            "latest_surface_return_ace_elo_rating": START_RATING,
            "latest_surface_first_won_elo_rating": START_RATING,
            "latest_surface_return_first_won_elo_rating": START_RATING,
            "latest_surface_second_won_elo_rating": START_RATING,
            "latest_surface_return_second_won_elo_rating": START_RATING
        }

    def update_result_for_match(result, row, player, game_date, surface):
        if row["winner_name"] == player:
            # print(f"{row['tourney_date']} vs {game_date - timedelta(weeks=LATEST_WEEKS)}")
            if row['tourney_date'] > game_date - timedelta(weeks=LATEST_WEEKS):
                update_elo_ratings(result, row, surface, update_latest=True)
                # print("ran")
            update_elo_ratings(result, row, surface, update_latest=False)
        else:
            if row['tourney_date'] > game_date - timedelta(weeks=LATEST_WEEKS):
                update_elo_ratings(result, row, surface, update_latest=True, is_winner=False)
            update_elo_ratings(result, row, surface, update_latest=False, is_winner=False)

    def update_elo_ratings(result, row, surface, update_latest, is_winner=True):
        w_games, l_games, w_sets, l_sets, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played = get_score_stats(row)
        if not isinstance(row, pd.Series):
            raise TypeError(f"Expected pandas Series but got {type(row)}")
        prefix = "latest_" if update_latest else "recent_"

        result[f"{prefix}match_number"] = row['match_num']
        result[f"{prefix}matches_played"] += 1
        
        elo_keys = {
            "elo_rating": f"{prefix}elo_rating",
            "point_elo_rating": f"{prefix}point_elo_rating",
            "game_elo_rating": f"{prefix}game_elo_rating",
            "set_elo_rating": f"{prefix}set_elo_rating",
            "service_game_elo_rating": f"{prefix}service_game_elo_rating",
            "return_game_elo_rating": f"{prefix}return_game_elo_rating",
            "tie_break_elo_rating": f"{prefix}tie_break_elo_rating",
            "ace_elo_rating": f"{prefix}ace_elo_rating",
            "return_ace_elo_rating": f"{prefix}return_ace_elo_rating",
            "first_won_elo_rating": f"{prefix}first_won_elo_rating",
            "return_first_won_elo_rating": f"{prefix}return_first_won_elo_rating",
            "second_won_elo_rating": f"{prefix}second_won_elo_rating",
            "return_second_won_elo_rating": f"{prefix}return_second_won_elo_rating",

            "surface_elo_rating": f"{prefix}surface_elo_rating",
            "surface_point_elo_rating": f"{prefix}surface_point_elo_rating",
            "surface_game_elo_rating": f"{prefix}surface_game_elo_rating",
            "surface_set_elo_rating": f"{prefix}surface_set_elo_rating",
            "surface_service_game_elo_rating": f"{prefix}surface_service_game_elo_rating",
            "surface_return_game_elo_rating": f"{prefix}surface_return_game_elo_rating",
            "surface_tie_break_elo_rating": f"{prefix}surface_tie_break_elo_rating",
            "surface_ace_elo_rating": f"{prefix}surface_ace_elo_rating",
            "surface_return_ace_elo_rating": f"{prefix}surface_return_ace_elo_rating",
            "surface_first_won_elo_rating": f"{prefix}surface_first_won_elo_rating",
            "surface_return_first_won_elo_rating": f"{prefix}surface_return_first_won_elo_rating",
            "surface_second_won_elo_rating": f"{prefix}surface_second_won_elo_rating",
            "surface_return_second_won_elo_rating": f"{prefix}surface_return_second_won_elo_rating",
        }

        opponent_elo = {
            "elo_rating": row[f'oppo_{prefix}elo_rating'] if ~np.isnan(row[f'oppo_{prefix}elo_rating']) else 1500,
            "point_elo_rating": row[f'oppo_{prefix}point_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}point_elo_rating']) else 1500,
            "game_elo_rating": row[f'oppo_{prefix}game_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}game_elo_rating']) else 1500,
            "set_elo_rating": row[f'oppo_{prefix}set_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}set_elo_rating']) else 1500,
            "service_game_elo_rating": row[f'oppo_{prefix}service_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}service_elo_rating']) else 1500,
            "return_game_elo_rating": row[f'oppo_{prefix}return_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}return_elo_rating']) else 1500,
            "tie_break_elo_rating": row[f'oppo_{prefix}tb_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}tb_elo_rating']) else 1500,
            "ace_elo_rating": row[f'oppo_{prefix}ace_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}ace_elo_rating']) else 1500,
            "return_ace_elo_rating": row[f'oppo_{prefix}return_ace_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}return_ace_elo_rating']) else 1500,
            "first_won_elo_rating": row[f'oppo_{prefix}first_won_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}first_won_elo_rating']) else 1500,
            "return_first_won_elo_rating": row[f'oppo_{prefix}return_first_won_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}return_first_won_elo_rating']) else 1500,
            "second_won_elo_rating": row[f'oppo_{prefix}second_won_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}second_won_elo_rating']) else 1500,
            "return_second_won_elo_rating": row[f'oppo_{prefix}return_second_won_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}return_second_won_elo_rating']) else 1500,

            "surface_elo_rating": row[f'oppo_{prefix}surface_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_elo_rating']) else 1500,
            "surface_point_elo_rating": row[f'oppo_{prefix}surface_point_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_point_elo_rating']) else 1500,
            "surface_game_elo_rating": row[f'oppo_{prefix}surface_game_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_game_elo_rating']) else 1500,
            "surface_set_elo_rating": row[f'oppo_{prefix}surface_set_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_set_elo_rating']) else 1500,
            "surface_service_game_elo_rating": row[f'oppo_{prefix}surface_service_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_service_elo_rating']) else 1500,
            "surface_return_game_elo_rating": row[f'oppo_{prefix}surface_return_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_elo_rating']) else 1500,
            "surface_tie_break_elo_rating": row[f'oppo_{prefix}surface_tb_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_tb_elo_rating']) else 1500,
            "surface_ace_elo_rating": row[f'oppo_{prefix}surface_ace_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_ace_elo_rating']) else 1500,
            "surface_return_ace_elo_rating": row[f'oppo_{prefix}surface_return_ace_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_ace_elo_rating']) else 1500,
            "surface_first_won_elo_rating": row[f'oppo_{prefix}surface_first_won_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_first_won_elo_rating']) else 1500,
            "surface_return_first_won_elo_rating": row[f'oppo_{prefix}surface_return_first_won_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_first_won_elo_rating']) else 1500,
            "surface_second_won_elo_rating": row[f'oppo_{prefix}surface_second_won_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_second_won_elo_rating']) else 1500,
            "surface_return_second_won_elo_rating": row[f'oppo_{prefix}surface_return_second_won_elo_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_second_won_elo_rating']) else 1500,
        }
        
        if is_winner:
            if surface == row["surface"]:
                result[f"{prefix}surface_match_number"] = row['match_num']
                result[f"{prefix}surface_matches_played"] += 1

                result[elo_keys["surface_elo_rating"]], _ = primary_elo(result[elo_keys["elo_rating"]], opponent_elo["surface_elo_rating"], row)
                result[elo_keys["surface_point_elo_rating"]], _, result[elo_keys["surface_set_elo_rating"]], _, result[elo_keys["surface_game_elo_rating"]], _ = points_sets_games_elo(
                    result[elo_keys["surface_set_elo_rating"]], opponent_elo["surface_set_elo_rating"], result[elo_keys["surface_game_elo_rating"]], opponent_elo["surface_game_elo_rating"],
                    result[elo_keys["surface_point_elo_rating"]], opponent_elo["surface_point_elo_rating"], row, w_sets, l_sets, w_games, l_games
                )
                result[elo_keys["surface_tie_break_elo_rating"]], _ = tb_elo(result[elo_keys["surface_tie_break_elo_rating"]], opponent_elo["surface_tie_break_elo_rating"], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
                try:
                    result[elo_keys["surface_service_game_elo_rating"]], _, result[elo_keys["surface_return_game_elo_rating"]], _ = return_serve_elo(
                        result[elo_keys["surface_service_game_elo_rating"]], opponent_elo["surface_service_game_elo_rating"], result[elo_keys["surface_return_game_elo_rating"]], opponent_elo["surface_return_game_elo_rating"], row
                    )

                    result[elo_keys["surface_ace_elo_rating"]], _, result[elo_keys["surface_return_ace_elo_rating"]], _ = ace_elo(
                        result[elo_keys["surface_ace_elo_rating"]], opponent_elo["surface_ace_elo_rating"], result[elo_keys["surface_return_ace_elo_rating"]], opponent_elo["surface_return_ace_elo_rating"], row
                    )

                    result[elo_keys["surface_first_won_elo_rating"]], _, result[elo_keys["surface_return_first_won_elo_rating"]], _ = first_won_elo(
                        result[elo_keys["surface_first_won_elo_rating"]], opponent_elo["surface_first_won_elo_rating"], result[elo_keys["surface_return_first_won_elo_rating"]], opponent_elo["surface_return_first_won_elo_rating"], row
                    )

                    result[elo_keys["surface_second_won_elo_rating"]], _, result[elo_keys["surface_return_second_won_elo_rating"]], _ = second_won_elo(
                        result[elo_keys["surface_second_won_elo_rating"]], opponent_elo["surface_second_won_elo_rating"], result[elo_keys["surface_return_second_won_elo_rating"]], opponent_elo["surface_return_second_won_elo_rating"], row
                    )
                except Exception as e:
                    # print(f"Skip worked {e}")
                    pass  # Missing stats ignore

            result[elo_keys["elo_rating"]], _ = primary_elo(result[elo_keys["elo_rating"]], opponent_elo["elo_rating"], row)
            result[elo_keys["point_elo_rating"]], _, result[elo_keys["set_elo_rating"]], _, result[elo_keys["game_elo_rating"]], _ = points_sets_games_elo(
                result[elo_keys["set_elo_rating"]], opponent_elo["set_elo_rating"], result[elo_keys["game_elo_rating"]], opponent_elo["game_elo_rating"],
                result[elo_keys["point_elo_rating"]], opponent_elo["point_elo_rating"], row, w_sets, l_sets, w_games, l_games
            )
            result[elo_keys["tie_break_elo_rating"]], _ = tb_elo(result[elo_keys["tie_break_elo_rating"]], opponent_elo["tie_break_elo_rating"], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
            try:
                result[elo_keys["service_game_elo_rating"]], _, result[elo_keys["return_game_elo_rating"]], _ = return_serve_elo(
                    result[elo_keys["service_game_elo_rating"]], opponent_elo["service_game_elo_rating"], result[elo_keys["return_game_elo_rating"]], opponent_elo["return_game_elo_rating"], row
                )

                result[elo_keys["ace_elo_rating"]], _, result[elo_keys["return_ace_elo_rating"]], _ = ace_elo(
                    result[elo_keys["ace_elo_rating"]], opponent_elo["ace_elo_rating"], result[elo_keys["return_ace_elo_rating"]], opponent_elo["return_ace_elo_rating"], row
                )

                result[elo_keys["first_won_elo_rating"]], _, result[elo_keys["return_first_won_elo_rating"]], _ = first_won_elo(
                    result[elo_keys["first_won_elo_rating"]], opponent_elo["first_won_elo_rating"], result[elo_keys["return_first_won_elo_rating"]], opponent_elo["return_first_won_elo_rating"], row
                )

                result[elo_keys["second_won_elo_rating"]], _, result[elo_keys["return_second_won_elo_rating"]], _ = second_won_elo(
                    result[elo_keys["second_won_elo_rating"]], opponent_elo["second_won_elo_rating"], result[elo_keys["return_second_won_elo_rating"]], opponent_elo["return_second_won_elo_rating"], row
                )
            except Exception as e:
                # print(f"Skip worked {e}")
                pass  # Missing stats ignore
        else:
            if surface == row["surface"]:
                result[f"{prefix}surface_match_number"] = row['match_num']
                result[f"{prefix}surface_matches_played"] += 1

                _, result[elo_keys["surface_elo_rating"]] = primary_elo(opponent_elo["surface_elo_rating"], result[elo_keys["surface_elo_rating"]], row)
                _, result[elo_keys["surface_point_elo_rating"]], _, result[elo_keys["surface_set_elo_rating"]], _, result[elo_keys["surface_game_elo_rating"]] = points_sets_games_elo(
                    opponent_elo["surface_set_elo_rating"], result[elo_keys["surface_set_elo_rating"]], opponent_elo["surface_game_elo_rating"], result[elo_keys["surface_game_elo_rating"]],
                    opponent_elo["surface_point_elo_rating"], result[elo_keys["surface_point_elo_rating"]], row, w_sets, l_sets, w_games, l_games
                )
                _, result[elo_keys["surface_tie_break_elo_rating"]] = tb_elo(opponent_elo["surface_tie_break_elo_rating"], result[elo_keys["surface_tie_break_elo_rating"]], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
                try:
                    _, result[elo_keys["surface_service_game_elo_rating"]], _, result[elo_keys["surface_return_game_elo_rating"]] = return_serve_elo(
                        opponent_elo["surface_service_game_elo_rating"], result[elo_keys["surface_service_game_elo_rating"]], opponent_elo["surface_return_game_elo_rating"], result[elo_keys["surface_return_game_elo_rating"]], row
                    )

                    _, result[elo_keys["surface_ace_elo_rating"]], _, result[elo_keys["surface_return_ace_elo_rating"]] = ace_elo(
                        opponent_elo["surface_ace_elo_rating"], result[elo_keys["surface_ace_elo_rating"]], opponent_elo["surface_return_ace_elo_rating"], result[elo_keys["surface_return_ace_elo_rating"]], row
                    )

                    _, result[elo_keys["surface_first_won_elo_rating"]], _, result[elo_keys["surface_return_first_won_elo_rating"]] = first_won_elo(
                        opponent_elo["surface_first_won_elo_rating"], result[elo_keys["surface_first_won_elo_rating"]], opponent_elo["surface_return_first_won_elo_rating"], result[elo_keys["surface_return_first_won_elo_rating"]], row
                    )

                    _, result[elo_keys["surface_second_won_elo_rating"]], _, result[elo_keys["surface_return_second_won_elo_rating"]] = second_won_elo(
                        opponent_elo["surface_second_won_elo_rating"], result[elo_keys["surface_second_won_elo_rating"]], opponent_elo["surface_return_second_won_elo_rating"], result[elo_keys["surface_return_second_won_elo_rating"]], row
                    )
                except Exception as e:
                    # print(f"Skip worked {e}")
                    pass  # Missing stats ignore

            _, result[elo_keys["elo_rating"]] = primary_elo(opponent_elo["elo_rating"], result[elo_keys["elo_rating"]], row)
            _, result[elo_keys["point_elo_rating"]], _, result[elo_keys["set_elo_rating"]], _, result[elo_keys["game_elo_rating"]] = points_sets_games_elo(
                opponent_elo["set_elo_rating"], result[elo_keys["set_elo_rating"]], opponent_elo["game_elo_rating"], result[elo_keys["game_elo_rating"]],
                opponent_elo["point_elo_rating"], result[elo_keys["point_elo_rating"]], row, w_sets, l_sets, w_games, l_games
            )
            _, result[elo_keys["tie_break_elo_rating"]] = tb_elo(opponent_elo["tie_break_elo_rating"], result[elo_keys["tie_break_elo_rating"]], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
            try:
                _, result[elo_keys["service_game_elo_rating"]], _, result[elo_keys["return_game_elo_rating"]] = return_serve_elo(
                    opponent_elo["service_game_elo_rating"], result[elo_keys["service_game_elo_rating"]], opponent_elo["return_game_elo_rating"], result[elo_keys["return_game_elo_rating"]], row
                )

                _, result[elo_keys["ace_elo_rating"]], _, result[elo_keys["return_ace_elo_rating"]] = ace_elo(
                    opponent_elo["ace_elo_rating"], result[elo_keys["ace_elo_rating"]], opponent_elo["return_ace_elo_rating"], result[elo_keys["return_ace_elo_rating"]], row
                )

                _, result[elo_keys["first_won_elo_rating"]], _, result[elo_keys["return_first_won_elo_rating"]] = first_won_elo(
                    opponent_elo["first_won_elo_rating"], result[elo_keys["first_won_elo_rating"]], opponent_elo["return_first_won_elo_rating"], result[elo_keys["return_first_won_elo_rating"]], row
                )

                _, result[elo_keys["second_won_elo_rating"]], _, result[elo_keys["return_second_won_elo_rating"]] = second_won_elo(
                    opponent_elo["second_won_elo_rating"], result[elo_keys["second_won_elo_rating"]], opponent_elo["return_second_won_elo_rating"], result[elo_keys["return_second_won_elo_rating"]], row
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

def update_dataframe(players_elo : pd.DataFrame, player_idx, col, value):
    players_elo.at[player_idx, col] = value

def update_primary_elo(players_elo : pd.DataFrame, idxA, idxB, rA_new, rB_new, match_date, match_num):
    update_dataframe(players_elo, idxA, 'elo_rating', rA_new)
    update_dataframe(players_elo, idxA, 'last_date', match_date)
    update_dataframe(players_elo, idxA, 'match_number', match_num)
    update_dataframe(players_elo, idxA, 'matches_played', players_elo.at[idxA, 'matches_played'] + 1)
    update_dataframe(players_elo, idxB, 'elo_rating', rB_new)
    update_dataframe(players_elo, idxB, 'last_date', match_date)
    update_dataframe(players_elo, idxB, 'match_number', match_num)
    update_dataframe(players_elo, idxB, 'matches_played', players_elo.at[idxB, 'matches_played'] + 1)

def update_points_sets_games_elo(players_elo : pd.DataFrame, idxA, idxB, rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew):
    update_dataframe(players_elo, idxA, 'point_elo_rating', rApointNew)
    update_dataframe(players_elo, idxB, 'point_elo_rating', rBpointNew)

    update_dataframe(players_elo, idxA, 'set_elo_rating', rAsetNew)
    update_dataframe(players_elo, idxB, 'set_elo_rating', rBsetNew)

    update_dataframe(players_elo, idxA, 'game_elo_rating', rAgameNew)
    update_dataframe(players_elo, idxB, 'game_elo_rating', rBgameNew)

def update_tb_elo(players_elo : pd.DataFrame, idxA, idxB, rAtbNew, rBtbNew):
    update_dataframe(players_elo, idxA, 'tie_break_elo_rating', rAtbNew)
    update_dataframe(players_elo, idxB, 'tie_break_elo_rating', rBtbNew)

def update_return_serve_elo(players_elo : pd.DataFrame, idxA, idxB, rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew):
    update_dataframe(players_elo, idxA, 'service_game_elo_rating', rAserviceNew)
    update_dataframe(players_elo, idxB, 'service_game_elo_rating', rBserviceNew)

    update_dataframe(players_elo, idxA, 'return_game_elo_rating', rAreturnNew)
    update_dataframe(players_elo, idxB, 'return_game_elo_rating', rBreturnNew)

def update_ace_elo(players_elo : pd.DataFrame, idxA, idxB, rAaceNew, rBaceNew, rAaceReturnNew, rBaceReturnNew):
    update_dataframe(players_elo, idxA, 'ace_elo_rating', rAaceNew)
    update_dataframe(players_elo, idxB, 'ace_elo_rating', rBaceNew)

    update_dataframe(players_elo, idxA, 'return_ace_elo_rating', rAaceReturnNew)
    update_dataframe(players_elo, idxB, 'return_ace_elo_rating', rBaceReturnNew)

def update_first_won_elo(players_elo : pd.DataFrame, idxA, idxB, rAfwNew, rBfwNew, rAvFwNew, rBcFwNew):
    update_dataframe(players_elo, idxA, 'first_won_elo_rating', rAfwNew)
    update_dataframe(players_elo, idxB, 'first_won_elo_rating', rBfwNew)

    update_dataframe(players_elo, idxA, 'return_first_won_elo_rating', rAvFwNew)
    update_dataframe(players_elo, idxB, 'return_first_won_elo_rating', rBcFwNew)

def update_second_won_elo(players_elo : pd.DataFrame, idxA, idxB, rAfwNew, rBfwNew, rAvFwNew, rBcFwNew):
    update_dataframe(players_elo, idxA, 'second_won_elo_rating', rAfwNew)
    update_dataframe(players_elo, idxB, 'second_won_elo_rating', rBfwNew)

    update_dataframe(players_elo, idxA, 'return_second_won_elo_rating', rAvFwNew)
    update_dataframe(players_elo, idxB, 'return_second_won_elo_rating', rBcFwNew)