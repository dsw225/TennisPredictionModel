import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from render.utils.glicko2.glicko2_functions import *
from render.utils.glicko2.glicko2 import *
import warnings
import traceback

async def filter_games(df: pd.DataFrame, end_date : datetime.date):
    warnings.filterwarnings("ignore", category=FutureWarning, message="Comparison of Timestamp with datetime.date is deprecated")
    df['last_date'] = pd.to_datetime(df['last_date'])
    df = df[~(
                (df['matches_played'] < MIN_MATCHES) |
                (df['last_date'].dt.date < (end_date - relativedelta(years=1)))
            )]
    df = df.sort_values(by='glicko_rating', ascending=False)
    return df

async def update_glickos(players_glicko : pd.DataFrame, row):
    try:
        player_a = players_glicko[players_glicko['player'] == row["winner_name"]]
        player_b = players_glicko[players_glicko['player'] == row["loser_name"]]

        if player_a.empty or player_b.empty:
            print("Error: One of the players not found in players_glicko DataFrame")
            return

        idxA = player_a.index[0]
        idxB = player_b.index[0]
        
        w_games, l_games, w_sets, l_sets, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played = get_score_stats(row)

        #Primary update
        match_date = row['tourney_date']
        match_num = row['match_num']
        rA_new, rB_new = primary_glicko(players_glicko.at[idxA, 'glicko_rating'], players_glicko.at[idxB, 'glicko_rating'], row)
        update_primary_glicko(players_glicko, idxA, idxB, rA_new, rB_new, match_date, match_num)
        
        # Point Sets etc.
        rAset = players_glicko.at[idxA, 'set_glicko_rating']
        rBset = players_glicko.at[idxB, 'set_glicko_rating']

        rAgame = players_glicko.at[idxA, 'game_glicko_rating']
        rBgame = players_glicko.at[idxB, 'game_glicko_rating']

        rApoint = players_glicko.at[idxA, 'point_glicko_rating']
        rBpoint = players_glicko.at[idxB, 'point_glicko_rating']

        rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew = points_sets_games_glicko(rAset, rBset, rAgame, rBgame, rApoint, rBpoint, row, w_sets, l_sets, w_games, l_games)
        update_points_sets_games_glicko(players_glicko, idxA, idxB, rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew)

        # TB Update
        rAtbNew, rBtbNew = tb_glicko(players_glicko.at[idxA, 'tie_break_glicko_rating'], players_glicko.at[idxB, 'tie_break_glicko_rating'], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
        update_tb_glicko(players_glicko, idxA, idxB, rAtbNew, rBtbNew)

        # Serve/Return Update
        try:
            rAservice = players_glicko.at[idxA, 'service_game_glicko_rating']
            rBservice = players_glicko.at[idxB, 'service_game_glicko_rating']

            rAreturn = players_glicko.at[idxA, 'return_game_glicko_rating']
            rBreturn = players_glicko.at[idxB, 'return_game_glicko_rating']

            rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew = return_serve_glicko(rAservice, rBservice, rAreturn, rBreturn, row)
            update_return_serve_glicko(players_glicko, idxA, idxB, rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew)
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

    # glicko Stats Calced
    player_a_stats = await process_player_matches(player_a, recent_matches_a, game)
    player_b_stats = await process_player_matches(player_b, recent_matches_b, game)

    changed_headers = {
        'recent_match_number': 'oppo_recent_match_number',
        'recent_matches_played': 'oppo_recent_matches_played',
        'recent_glicko_rating': 'oppo_recent_glicko_rating',
        'recent_point_glicko_rating': 'oppo_recent_point_glicko_rating',
        'recent_game_glicko_rating': 'oppo_recent_game_glicko_rating',
        'recent_set_glicko_rating': 'oppo_recent_set_glicko_rating',
        'recent_service_game_glicko_rating': 'oppo_recent_service_game_glicko_rating',
        'recent_return_game_glicko_rating': 'oppo_recent_return_game_glicko_rating',
        'recent_tie_break_glicko_rating': 'oppo_recent_tie_break_glicko_rating',
        "recent_ace_glicko_rating": "oppo_recent_ace_glicko_rating",
        "recent_return_ace_glicko_rating": "oppo_recent_return_ace_glicko_rating",
        "recent_first_won_glicko_rating": "oppo_recent_first_won_glicko_rating",
        "recent_return_first_won_glicko_rating": "oppo_recent_return_first_won_glicko_rating",
        "recent_second_won_glicko_rating": "oppo_recent_second_won_glicko_rating",
        "recent_return_second_won_glicko_rating": "oppo_recent_return_second_won_glicko_rating",

        'recent_surface_match_number': 'oppo_recent_surface_match_number',
        'recent_surface_matches_played': 'oppo_recent_surface_matches_played',
        'recent_surface_glicko_rating': 'oppo_recent_surface_glicko_rating',
        'recent_surface_point_glicko_rating': 'oppo_recent_surface_point_glicko_rating',
        'recent_surface_game_glicko_rating': 'oppo_recent_surface_game_glicko_rating',
        'recent_surface_set_glicko_rating': 'oppo_recent_surface_set_glicko_rating',
        'recent_surface_service_game_glicko_rating': 'oppo_recent_surface_service_game_glicko_rating',
        'recent_surface_return_game_glicko_rating': 'oppo_recent_surface_return_game_glicko_rating',
        'recent_surface_tie_break_glicko_rating': 'oppo_recent_surface_tie_break_glicko_rating',
        "recent_surface_ace_glicko_rating": "oppo_recent_surface_ace_glicko_rating",
        "recent_surface_return_ace_glicko_rating": "oppo_recent_surface_return_ace_glicko_rating",
        "recent_surface_first_won_glicko_rating": "oppo_recent_surface_first_won_glicko_rating",
        "recent_surface_return_first_won_glicko_rating": "oppo_recent_surface_return_first_won_glicko_rating",
        "recent_surface_second_won_glicko_rating": "oppo_recent_surface_second_won_glicko_rating",
        "recent_surface_return_second_won_glicko_rating": "oppo_recent_surface_return_second_won_glicko_rating",
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
            "recent_glicko_rating": Rating(),
            "recent_point_glicko_rating": Rating(),
            "recent_game_glicko_rating": Rating(),
            "recent_set_glicko_rating": Rating(),
            "recent_service_game_glicko_rating": Rating(),
            "recent_return_game_glicko_rating": Rating(),
            "recent_tie_break_glicko_rating": Rating(),
            "recent_ace_glicko_rating": Rating(),
            "recent_return_ace_glicko_rating": Rating(),
            "recent_first_won_glicko_rating": Rating(),
            "recent_return_first_won_glicko_rating": Rating(),
            "recent_second_won_glicko_rating": Rating(),
            "recent_return_second_won_glicko_rating": Rating(),

            "recent_surface_match_number": -1,
            "recent_surface_matches_played": 0,
            "recent_surface_glicko_rating": Rating(),
            "recent_surface_point_glicko_rating": Rating(),
            "recent_surface_game_glicko_rating": Rating(),
            "recent_surface_set_glicko_rating": Rating(),
            "recent_surface_service_game_glicko_rating": Rating(),
            "recent_surface_return_game_glicko_rating": Rating(),
            "recent_surface_tie_break_glicko_rating": Rating(),
            "recent_surface_ace_glicko_rating": Rating(),
            "recent_surface_return_ace_glicko_rating": Rating(),
            "recent_surface_first_won_glicko_rating": Rating(),
            "recent_surface_return_first_won_glicko_rating": Rating(),
            "recent_surface_second_won_glicko_rating": Rating(),
            "recent_surface_return_second_won_glicko_rating": Rating(),
        }

    def update_result_for_match(result, row, player, game_date, surface):
        if row["winner_name"] == player:
            update_glicko_ratings(result, row, surface, update_latest=False)
        else:
            update_glicko_ratings(result, row, surface, update_latest=False, is_winner=False)

    def update_glicko_ratings(result, row, surface, update_latest, is_winner=True):
        w_games, l_games, w_sets, l_sets, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played = get_score_stats(row)
        if not isinstance(row, pd.Series):
            raise TypeError(f"Expected pandas Series but got {type(row)}")
        prefix = "recent_"

        result[f"{prefix}match_number"] = row['match_num']
        result[f"{prefix}matches_played"] += 1
        
        glicko_keys = {
            "glicko_rating": f"{prefix}glicko_rating",
            "point_glicko_rating": f"{prefix}point_glicko_rating",
            "game_glicko_rating": f"{prefix}game_glicko_rating",
            "set_glicko_rating": f"{prefix}set_glicko_rating",
            "service_game_glicko_rating": f"{prefix}service_game_glicko_rating",
            "return_game_glicko_rating": f"{prefix}return_game_glicko_rating",
            "tie_break_glicko_rating": f"{prefix}tie_break_glicko_rating",
            "ace_glicko_rating": f"{prefix}ace_glicko_rating",
            "return_ace_glicko_rating": f"{prefix}return_ace_glicko_rating",
            "first_won_glicko_rating": f"{prefix}first_won_glicko_rating",
            "return_first_won_glicko_rating": f"{prefix}return_first_won_glicko_rating",
            "second_won_glicko_rating": f"{prefix}second_won_glicko_rating",
            "return_second_won_glicko_rating": f"{prefix}return_second_won_glicko_rating",

            "surface_glicko_rating": f"{prefix}surface_glicko_rating",
            "surface_point_glicko_rating": f"{prefix}surface_point_glicko_rating",
            "surface_game_glicko_rating": f"{prefix}surface_game_glicko_rating",
            "surface_set_glicko_rating": f"{prefix}surface_set_glicko_rating",
            "surface_service_game_glicko_rating": f"{prefix}surface_service_game_glicko_rating",
            "surface_return_game_glicko_rating": f"{prefix}surface_return_game_glicko_rating",
            "surface_tie_break_glicko_rating": f"{prefix}surface_tie_break_glicko_rating",
            "surface_ace_glicko_rating": f"{prefix}surface_ace_glicko_rating",
            "surface_return_ace_glicko_rating": f"{prefix}surface_return_ace_glicko_rating",
            "surface_first_won_glicko_rating": f"{prefix}surface_first_won_glicko_rating",
            "surface_return_first_won_glicko_rating": f"{prefix}surface_return_first_won_glicko_rating",
            "surface_second_won_glicko_rating": f"{prefix}surface_second_won_glicko_rating",
            "surface_return_second_won_glicko_rating": f"{prefix}surface_return_second_won_glicko_rating",
        }

        opponent_glicko = {
            "glicko_rating": row[f'oppo_{prefix}glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}glicko_rating']) else 1500,
            "point_glicko_rating": row[f'oppo_{prefix}point_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}point_glicko_rating']) else 1500,
            "game_glicko_rating": row[f'oppo_{prefix}game_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}game_glicko_rating']) else 1500,
            "set_glicko_rating": row[f'oppo_{prefix}set_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}set_glicko_rating']) else 1500,
            "service_game_glicko_rating": row[f'oppo_{prefix}service_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}service_glicko_rating']) else 1500,
            "return_game_glicko_rating": row[f'oppo_{prefix}return_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}return_glicko_rating']) else 1500,
            "tie_break_glicko_rating": row[f'oppo_{prefix}tb_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}tb_glicko_rating']) else 1500,
            "ace_glicko_rating": row[f'oppo_{prefix}ace_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}ace_glicko_rating']) else 1500,
            "return_ace_glicko_rating": row[f'oppo_{prefix}return_ace_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}return_ace_glicko_rating']) else 1500,
            "first_won_glicko_rating": row[f'oppo_{prefix}first_won_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}first_won_glicko_rating']) else 1500,
            "return_first_won_glicko_rating": row[f'oppo_{prefix}return_first_won_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}return_first_won_glicko_rating']) else 1500,
            "second_won_glicko_rating": row[f'oppo_{prefix}second_won_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}second_won_glicko_rating']) else 1500,
            "return_second_won_glicko_rating": row[f'oppo_{prefix}return_second_won_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}return_second_won_glicko_rating']) else 1500,

            "surface_glicko_rating": row[f'oppo_{prefix}surface_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_glicko_rating']) else 1500,
            "surface_point_glicko_rating": row[f'oppo_{prefix}surface_point_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_point_glicko_rating']) else 1500,
            "surface_game_glicko_rating": row[f'oppo_{prefix}surface_game_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_game_glicko_rating']) else 1500,
            "surface_set_glicko_rating": row[f'oppo_{prefix}surface_set_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_set_glicko_rating']) else 1500,
            "surface_service_game_glicko_rating": row[f'oppo_{prefix}surface_service_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_service_glicko_rating']) else 1500,
            "surface_return_game_glicko_rating": row[f'oppo_{prefix}surface_return_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_glicko_rating']) else 1500,
            "surface_tie_break_glicko_rating": row[f'oppo_{prefix}surface_tb_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_tb_glicko_rating']) else 1500,
            "surface_ace_glicko_rating": row[f'oppo_{prefix}surface_ace_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_ace_glicko_rating']) else 1500,
            "surface_return_ace_glicko_rating": row[f'oppo_{prefix}surface_return_ace_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_ace_glicko_rating']) else 1500,
            "surface_first_won_glicko_rating": row[f'oppo_{prefix}surface_first_won_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_first_won_glicko_rating']) else 1500,
            "surface_return_first_won_glicko_rating": row[f'oppo_{prefix}surface_return_first_won_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_first_won_glicko_rating']) else 1500,
            "surface_second_won_glicko_rating": row[f'oppo_{prefix}surface_second_won_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_second_won_glicko_rating']) else 1500,
            "surface_return_second_won_glicko_rating": row[f'oppo_{prefix}surface_return_second_won_glicko_rating'] if ~np.isnan(row[f'oppo_{prefix}surface_return_second_won_glicko_rating']) else 1500,
        }
        
        if is_winner:
            if surface == row["surface"]:
                result[f"{prefix}surface_match_number"] = row['match_num']
                result[f"{prefix}surface_matches_played"] += 1

                result[glicko_keys["surface_glicko_rating"]], _ = primary_glicko(result[glicko_keys["glicko_rating"]], opponent_glicko["surface_glicko_rating"], row)
                result[glicko_keys["surface_point_glicko_rating"]], _, result[glicko_keys["surface_set_glicko_rating"]], _, result[glicko_keys["surface_game_glicko_rating"]], _ = points_sets_games_glicko(
                    result[glicko_keys["surface_set_glicko_rating"]], opponent_glicko["surface_set_glicko_rating"], result[glicko_keys["surface_game_glicko_rating"]], opponent_glicko["surface_game_glicko_rating"],
                    result[glicko_keys["surface_point_glicko_rating"]], opponent_glicko["surface_point_glicko_rating"], row, w_sets, l_sets, w_games, l_games
                )
                result[glicko_keys["surface_tie_break_glicko_rating"]], _ = tb_glicko(result[glicko_keys["surface_tie_break_glicko_rating"]], opponent_glicko["surface_tie_break_glicko_rating"], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
                try:
                    result[glicko_keys["surface_service_game_glicko_rating"]], _, result[glicko_keys["surface_return_game_glicko_rating"]], _ = return_serve_glicko(
                        result[glicko_keys["surface_service_game_glicko_rating"]], opponent_glicko["surface_service_game_glicko_rating"], result[glicko_keys["surface_return_game_glicko_rating"]], opponent_glicko["surface_return_game_glicko_rating"], row
                    )

                    result[glicko_keys["surface_ace_glicko_rating"]], _, result[glicko_keys["surface_return_ace_glicko_rating"]], _ = ace_glicko(
                        result[glicko_keys["surface_ace_glicko_rating"]], opponent_glicko["surface_ace_glicko_rating"], result[glicko_keys["surface_return_ace_glicko_rating"]], opponent_glicko["surface_return_ace_glicko_rating"], row
                    )

                    result[glicko_keys["surface_first_won_glicko_rating"]], _, result[glicko_keys["surface_return_first_won_glicko_rating"]], _ = first_won_glicko(
                        result[glicko_keys["surface_first_won_glicko_rating"]], opponent_glicko["surface_first_won_glicko_rating"], result[glicko_keys["surface_return_first_won_glicko_rating"]], opponent_glicko["surface_return_first_won_glicko_rating"], row
                    )

                    result[glicko_keys["surface_second_won_glicko_rating"]], _, result[glicko_keys["surface_return_second_won_glicko_rating"]], _ = second_won_glicko(
                        result[glicko_keys["surface_second_won_glicko_rating"]], opponent_glicko["surface_second_won_glicko_rating"], result[glicko_keys["surface_return_second_won_glicko_rating"]], opponent_glicko["surface_return_second_won_glicko_rating"], row
                    )
                except Exception as e:
                    print(f"Skip worked {e}")
                    pass  # Missing stats ignore

            result[glicko_keys["glicko_rating"]], _ = primary_glicko(result[glicko_keys["glicko_rating"]], opponent_glicko["glicko_rating"], row)
            result[glicko_keys["point_glicko_rating"]], _, result[glicko_keys["set_glicko_rating"]], _, result[glicko_keys["game_glicko_rating"]], _ = points_sets_games_glicko(
                result[glicko_keys["set_glicko_rating"]], opponent_glicko["set_glicko_rating"], result[glicko_keys["game_glicko_rating"]], opponent_glicko["game_glicko_rating"],
                result[glicko_keys["point_glicko_rating"]], opponent_glicko["point_glicko_rating"], row, w_sets, l_sets, w_games, l_games
            )
            result[glicko_keys["tie_break_glicko_rating"]], _ = tb_glicko(result[glicko_keys["tie_break_glicko_rating"]], opponent_glicko["tie_break_glicko_rating"], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
            try:
                result[glicko_keys["service_game_glicko_rating"]], _, result[glicko_keys["return_game_glicko_rating"]], _ = return_serve_glicko(
                    result[glicko_keys["service_game_glicko_rating"]], opponent_glicko["service_game_glicko_rating"], result[glicko_keys["return_game_glicko_rating"]], opponent_glicko["return_game_glicko_rating"], row
                )

                result[glicko_keys["ace_glicko_rating"]], _, result[glicko_keys["return_ace_glicko_rating"]], _ = ace_glicko(
                    result[glicko_keys["ace_glicko_rating"]], opponent_glicko["ace_glicko_rating"], result[glicko_keys["return_ace_glicko_rating"]], opponent_glicko["return_ace_glicko_rating"], row
                )

                result[glicko_keys["first_won_glicko_rating"]], _, result[glicko_keys["return_first_won_glicko_rating"]], _ = first_won_glicko(
                    result[glicko_keys["first_won_glicko_rating"]], opponent_glicko["first_won_glicko_rating"], result[glicko_keys["return_first_won_glicko_rating"]], opponent_glicko["return_first_won_glicko_rating"], row
                )

                result[glicko_keys["second_won_glicko_rating"]], _, result[glicko_keys["return_second_won_glicko_rating"]], _ = second_won_glicko(
                    result[glicko_keys["second_won_glicko_rating"]], opponent_glicko["second_won_glicko_rating"], result[glicko_keys["return_second_won_glicko_rating"]], opponent_glicko["return_second_won_glicko_rating"], row
                )
            except Exception as e:
                print(f"Skip worked {e}")
                pass  # Missing stats ignore
        else:
            if surface == row["surface"]:
                result[f"{prefix}surface_match_number"] = row['match_num']
                result[f"{prefix}surface_matches_played"] += 1

                _, result[glicko_keys["surface_glicko_rating"]] = primary_glicko(opponent_glicko["surface_glicko_rating"], result[glicko_keys["surface_glicko_rating"]], row)
                _, result[glicko_keys["surface_point_glicko_rating"]], _, result[glicko_keys["surface_set_glicko_rating"]], _, result[glicko_keys["surface_game_glicko_rating"]] = points_sets_games_glicko(
                    opponent_glicko["surface_set_glicko_rating"], result[glicko_keys["surface_set_glicko_rating"]], opponent_glicko["surface_game_glicko_rating"], result[glicko_keys["surface_game_glicko_rating"]],
                    opponent_glicko["surface_point_glicko_rating"], result[glicko_keys["surface_point_glicko_rating"]], row, w_sets, l_sets, w_games, l_games
                )
                _, result[glicko_keys["surface_tie_break_glicko_rating"]] = tb_glicko(opponent_glicko["surface_tie_break_glicko_rating"], result[glicko_keys["surface_tie_break_glicko_rating"]], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
                try:
                    _, result[glicko_keys["surface_service_game_glicko_rating"]], _, result[glicko_keys["surface_return_game_glicko_rating"]] = return_serve_glicko(
                        opponent_glicko["surface_service_game_glicko_rating"], result[glicko_keys["surface_service_game_glicko_rating"]], opponent_glicko["surface_return_game_glicko_rating"], result[glicko_keys["surface_return_game_glicko_rating"]], row
                    )

                    _, result[glicko_keys["surface_ace_glicko_rating"]], _, result[glicko_keys["surface_return_ace_glicko_rating"]] = ace_glicko(
                        opponent_glicko["surface_ace_glicko_rating"], result[glicko_keys["surface_ace_glicko_rating"]], opponent_glicko["surface_return_ace_glicko_rating"], result[glicko_keys["surface_return_ace_glicko_rating"]], row
                    )

                    _, result[glicko_keys["surface_first_won_glicko_rating"]], _, result[glicko_keys["surface_return_first_won_glicko_rating"]] = first_won_glicko(
                        opponent_glicko["surface_first_won_glicko_rating"], result[glicko_keys["surface_first_won_glicko_rating"]], opponent_glicko["surface_return_first_won_glicko_rating"], result[glicko_keys["surface_return_first_won_glicko_rating"]], row
                    )

                    _, result[glicko_keys["surface_second_won_glicko_rating"]], _, result[glicko_keys["surface_return_second_won_glicko_rating"]] = second_won_glicko(
                        opponent_glicko["surface_second_won_glicko_rating"], result[glicko_keys["surface_second_won_glicko_rating"]], opponent_glicko["surface_return_second_won_glicko_rating"], result[glicko_keys["surface_return_second_won_glicko_rating"]], row
                    )
                except Exception as e:
                    print(f"Skip worked {e}")
                    pass  # Missing stats ignore

            _, result[glicko_keys["glicko_rating"]] = primary_glicko(opponent_glicko["glicko_rating"], result[glicko_keys["glicko_rating"]], row)
            _, result[glicko_keys["point_glicko_rating"]], _, result[glicko_keys["set_glicko_rating"]], _, result[glicko_keys["game_glicko_rating"]] = points_sets_games_glicko(
                opponent_glicko["set_glicko_rating"], result[glicko_keys["set_glicko_rating"]], opponent_glicko["game_glicko_rating"], result[glicko_keys["game_glicko_rating"]],
                opponent_glicko["point_glicko_rating"], result[glicko_keys["point_glicko_rating"]], row, w_sets, l_sets, w_games, l_games
            )
            _, result[glicko_keys["tie_break_glicko_rating"]] = tb_glicko(opponent_glicko["tie_break_glicko_rating"], result[glicko_keys["tie_break_glicko_rating"]], row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played)
            try:
                _, result[glicko_keys["service_game_glicko_rating"]], _, result[glicko_keys["return_game_glicko_rating"]] = return_serve_glicko(
                    opponent_glicko["service_game_glicko_rating"], result[glicko_keys["service_game_glicko_rating"]], opponent_glicko["return_game_glicko_rating"], result[glicko_keys["return_game_glicko_rating"]], row
                )

                _, result[glicko_keys["ace_glicko_rating"]], _, result[glicko_keys["return_ace_glicko_rating"]] = ace_glicko(
                    opponent_glicko["ace_glicko_rating"], result[glicko_keys["ace_glicko_rating"]], opponent_glicko["return_ace_glicko_rating"], result[glicko_keys["return_ace_glicko_rating"]], row
                )

                _, result[glicko_keys["first_won_glicko_rating"]], _, result[glicko_keys["return_first_won_glicko_rating"]] = first_won_glicko(
                    opponent_glicko["first_won_glicko_rating"], result[glicko_keys["first_won_glicko_rating"]], opponent_glicko["return_first_won_glicko_rating"], result[glicko_keys["return_first_won_glicko_rating"]], row
                )

                _, result[glicko_keys["second_won_glicko_rating"]], _, result[glicko_keys["return_second_won_glicko_rating"]] = second_won_glicko(
                    opponent_glicko["second_won_glicko_rating"], result[glicko_keys["second_won_glicko_rating"]], opponent_glicko["return_second_won_glicko_rating"], result[glicko_keys["return_second_won_glicko_rating"]], row
                )
            except Exception as e:
                print(f"Skip worked {e}")
                pass  # Missing stats ignore

    # Main processing
    game_date = game['tourney_date']
    surface = game['surface']
    recent_matches = recent_matches.sort_values(by='tourney_date', ascending=True)

    result = initialize_result()

    for index, row in recent_matches.iterrows():
        update_result_for_match(result, row, player, game_date, surface)

    return pd.Series(result)

def update_dataframe(players_glicko : pd.DataFrame, player_idx, col, value):
    players_glicko.at[player_idx, col] = value

def update_primary_glicko(players_glicko : pd.DataFrame, idxA, idxB, rA_new, rB_new, match_date, match_num):
    update_dataframe(players_glicko, idxA, 'glicko_rating', rA_new)
    update_dataframe(players_glicko, idxA, 'last_date', match_date)
    update_dataframe(players_glicko, idxA, 'match_number', match_num)
    update_dataframe(players_glicko, idxA, 'matches_played', players_glicko.at[idxA, 'matches_played'] + 1)
    update_dataframe(players_glicko, idxB, 'glicko_rating', rB_new)
    update_dataframe(players_glicko, idxB, 'last_date', match_date)
    update_dataframe(players_glicko, idxB, 'match_number', match_num)
    update_dataframe(players_glicko, idxB, 'matches_played', players_glicko.at[idxB, 'matches_played'] + 1)

def update_points_sets_games_glicko(players_glicko : pd.DataFrame, idxA, idxB, rApointNew, rBpointNew, rAsetNew, rBsetNew, rAgameNew, rBgameNew):
    update_dataframe(players_glicko, idxA, 'point_glicko_rating', rApointNew)
    update_dataframe(players_glicko, idxB, 'point_glicko_rating', rBpointNew)

    update_dataframe(players_glicko, idxA, 'set_glicko_rating', rAsetNew)
    update_dataframe(players_glicko, idxB, 'set_glicko_rating', rBsetNew)

    update_dataframe(players_glicko, idxA, 'game_glicko_rating', rAgameNew)
    update_dataframe(players_glicko, idxB, 'game_glicko_rating', rBgameNew)

def update_tb_glicko(players_glicko : pd.DataFrame, idxA, idxB, rAtbNew, rBtbNew):
    update_dataframe(players_glicko, idxA, 'tie_break_glicko_rating', rAtbNew)
    update_dataframe(players_glicko, idxB, 'tie_break_glicko_rating', rBtbNew)

def update_return_serve_glicko(players_glicko : pd.DataFrame, idxA, idxB, rAserviceNew, rBserviceNew, rAreturnNew, rBreturnNew):
    update_dataframe(players_glicko, idxA, 'service_game_glicko_rating', rAserviceNew)
    update_dataframe(players_glicko, idxB, 'service_game_glicko_rating', rBserviceNew)

    update_dataframe(players_glicko, idxA, 'return_game_glicko_rating', rAreturnNew)
    update_dataframe(players_glicko, idxB, 'return_game_glicko_rating', rBreturnNew)

def update_ace_glicko(players_glicko : pd.DataFrame, idxA, idxB, rAaceNew, rBaceNew, rAaceReturnNew, rBaceReturnNew):
    update_dataframe(players_glicko, idxA, 'ace_glicko_rating', rAaceNew)
    update_dataframe(players_glicko, idxB, 'ace_glicko_rating', rBaceNew)

    update_dataframe(players_glicko, idxA, 'return_ace_glicko_rating', rAaceReturnNew)
    update_dataframe(players_glicko, idxB, 'return_ace_glicko_rating', rBaceReturnNew)

def update_first_won_glicko(players_glicko : pd.DataFrame, idxA, idxB, rAfwNew, rBfwNew, rAvFwNew, rBcFwNew):
    update_dataframe(players_glicko, idxA, 'first_won_glicko_rating', rAfwNew)
    update_dataframe(players_glicko, idxB, 'first_won_glicko_rating', rBfwNew)

    update_dataframe(players_glicko, idxA, 'return_first_won_glicko_rating', rAvFwNew)
    update_dataframe(players_glicko, idxB, 'return_first_won_glicko_rating', rBcFwNew)

def update_second_won_glicko(players_glicko : pd.DataFrame, idxA, idxB, rAfwNew, rBfwNew, rAvFwNew, rBcFwNew):
    update_dataframe(players_glicko, idxA, 'second_won_glicko_rating', rAfwNew)
    update_dataframe(players_glicko, idxB, 'second_won_glicko_rating', rBfwNew)

    update_dataframe(players_glicko, idxA, 'return_second_won_glicko_rating', rAvFwNew)
    update_dataframe(players_glicko, idxB, 'return_second_won_glicko_rating', rBcFwNew)