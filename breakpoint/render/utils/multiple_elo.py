import pandas as pd
import aiofiles
import asyncio
from datetime import datetime, timedelta
from math import pow, copysign, floor, ceil
import traceback
import numpy as np
from render.utils.elo_functions import *

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

    players_elo = pd.DataFrame(data, columns=new_header)
    clay_players_elo = pd.DataFrame(data, columns=new_header)
    grass_players_elo = pd.DataFrame(data, columns=new_header)
    hard_players_elo = pd.DataFrame(data, columns=new_header)

    # new_game_df = create_new_game_df(matches_df)

    tasks = []
    for index, row in matches_df.iterrows():
        if index % 1000 == 0:
            print(f"Processing Elos @ Match indexes: {index} - {index+1000}")
        tasks.append(update_elos(players_elo, row))
        if row['surface'] == "Grass":
            tasks.append(update_elos(grass_players_elo, row))
        elif row['surface'] == "Clay":
            tasks.append(update_elos(clay_players_elo, row))
        elif row['surface'] == "Hard":
            tasks.append(update_elos(hard_players_elo, row))

    await asyncio.gather(*tasks)

    players_elo = await filter_games(players_elo)
    grass_players_elo = await filter_games(grass_players_elo)
    clay_players_elo = await filter_games(clay_players_elo)
    hard_players_elo = await filter_games(hard_players_elo)

    return players_elo, grass_players_elo, clay_players_elo, hard_players_elo

# def create_new_game_df(df: pd.DataFrame):
#     new_game_header = ['tourney_id', 'tourney_name', 'tourney_date', 'surface', 'best_of', 'match_num', 
#                        'a_player_id', 'a_player_name', 'a_player_slug', 'b_player_id', 'b_player_name', 
#                        'b_player_slug', 'a_elo_rating', 'a_point_elo_rating', 'a_game_elo_rating', 
#                        'a_set_elo_rating', 'a_service_game_elo_rating', 'a_return_game_elo_rating', 
#                        'a_tie_break_elo_rating', 'a_surface_elo_rating', 'a_surface_point_elo_rating', 
#                        'a_surface_game_elo_rating', 'a_surface_set_elo_rating', 'a_surface_service_game_elo_rating', 
#                        'a_surface_return_game_elo_rating', 'a_surface_tie_break_elo_rating', 'a_win_percent', 
#                        'a_serve_rating', 'a_return_rating', 'a_pressure_rating', 'a_avg_vs_elo', 'a_matches_played', 
#                        'b_elo_rating', 'b_point_elo_rating', 'b_game_elo_rating', 'b_set_elo_rating', 
#                        'b_service_game_elo_rating', 'b_return_game_elo_rating', 'b_tie_break_elo_rating', 
#                        'b_surface_elo_rating', 'b_surface_point_elo_rating', 'b_surface_game_elo_rating', 
#                        'b_surface_set_elo_rating', 'b_surface_service_game_elo_rating', 'b_surface_return_game_elo_rating', 
#                        'b_surface_tie_break_elo_rating', 'b_win_percent', 'b_serve_rating', 'b_return_rating', 
#                        'b_pressure_rating', 'b_avg_vs_elo', 'b_matches_played', 'a_b_win']
    
#     new_df = pd.DataFrame(columns=new_game_header)
    
#     new_df['tourney_id'] = df['tourney_id']
#     new_df['tourney_name'] = df['tourney_name']
#     new_df['tourney_date'] = df['tourney_date']
#     new_df['surface'] = df['surface']
#     new_df['best_of'] = df['best_of']
#     new_df['match_num'] = df['match_num']
    
#     new_df['a_player_id'] = df['winner_id']
#     new_df['a_player_name'] = df['winner_name']
    
#     new_df['b_player_id'] = df['loser_id']
#     new_df['b_player_name'] = df['loser_name']
    
#     # Fill the rest of the columns with empty strings
#     for col in new_game_header:
#         if col not in new_df.columns:
#             new_df[col] = ''
    
#     return new_df

# async def compute_game(df: pd.DataFrame, game):
    return