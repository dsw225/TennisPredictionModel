import pandas as pd
import aiofiles
import asyncio
from datetime import datetime, timedelta
from math import pow, copysign, floor, ceil
import traceback
import numpy as np
# from elo_functions import *
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
    
    new_game_header = ['tourney_id', 'tourney_name', 'tourney_date', 'surface', 'best_of', 'match_num', 'a_player_id', 'a_player_name', 
                       'a_player_slug', 'b_player_id', 'b_player_name', 'b_player_slug', 'a_elo_rating', 'a_point_elo_rating', 'a_game_elo_rating', 
                       'a_set_elo_rating', 'a_service_game_elo_rating', 'a_return_game_elo_rating', 'a_tie_break_elo_rating', 'a_surface_elo_rating', 
                       'a_surface_point_elo_rating', 'a_surface_game_elo_rating', 'a_surface_set_elo_rating', 'a_surface_service_game_elo_rating', 
                       'a_surface_return_game_elo_rating', 'a_surface_tie_break_elo_rating', 'a_win_percent', 'a_serve_rating', 'a_return_rating', 
                       'a_pressure_rating', 'a_avg_vs_elo', 'a_matches_played', 'b_elo_rating', 'b_point_elo_rating', 'b_game_elo_rating', 'b_set_elo_rating', 
                       'b_service_game_elo_rating', 'b_return_game_elo_rating', 'b_tie_break_elo_rating', 'b_surface_elo_rating', 'b_surface_point_elo_rating', 
                       'b_surface_game_elo_rating', 'b_surface_set_elo_rating', 'b_surface_service_game_elo_rating', 'b_surface_return_game_elo_rating', 
                       'b_surface_tie_break_elo_rating', 'b_win_percent', 'b_serve_rating', 'b_return_rating', 'b_pressure_rating', 'b_avg_vs_elo', 'b_matches_played', 'a_b_win']

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

    tasks = []
    for index, row in matches_df.iterrows():
        if index % 1000 == 0:
            print(f"Processing Elos @ Match indexes: {index} - {index+1000}")
        tasks.append(update_elos(players_elo, row))
    
    await asyncio.gather(*tasks)

    players_elo['last_date'] = pd.to_datetime(players_elo['last_date'])
    players_elo = players_elo[~(
                (players_elo['matches_played'] < MIN_MATCHES)
            )]
    players_elo = players_elo.sort_values(by='elo_rating', ascending=False)
    return players_elo

