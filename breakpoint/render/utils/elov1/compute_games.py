import pandas as pd
from tqdm.asyncio import tqdm
import asyncio
from datetime import datetime, timedelta
from math import pow, copysign, floor, ceil
import random
import traceback
import numpy as np
from render.utils.elov1.elo_functions import *
import arff

async def prior_games(df: pd.DataFrame, enddate: datetime.date):
    # Changing dataframe fix for future
    w1_iloc = df.columns.get_loc('w1')
    w_ace_iloc = df.columns.get_loc('w_ace')
    w_bp_f_iloc = df.columns.get_loc('w_bpFaced')
    l_ace_iloc = df.columns.get_loc('l_ace')
    l_bp_f_iloc = df.columns.get_loc('l_bpFaced')

    # df = df.sort_values(by='tourney_date').reset_index(drop=True)

    # Basic filter conditions
    conditions = (
        ~pd.isnull(df.iloc[:, w1_iloc].values) &
        ~pd.isnull(df.iloc[:, w_ace_iloc:w_bp_f_iloc+1].values).any(axis=1) &
        ~pd.isnull(df.iloc[:, l_ace_iloc:l_bp_f_iloc+1].values).any(axis=1)
    )

    # Filter the DataFrame
    matches_df = df[conditions].sort_values(by='tourney_date').reset_index(drop=True)

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


    game_header = [
        'tourney_id',
        'tourney_name',
        'tourney_date',
        'surface',
        'best_of',
        'match_num',
        'a_player_id',
        'a_player_name',
        'a_player_slug',
        'a_player_rank',
        'b_player_id',
        'b_player_name',
        'b_player_slug',
        'b_player_rank',
        'a_elo_rating',
        'a_point_elo_rating',
        'a_game_elo_rating',
        'a_set_elo_rating',
        'a_service_game_elo_rating',
        'a_return_game_elo_rating',
        'a_tie_break_elo_rating',
        'a_surface_elo_rating',
        'a_surface_point_elo_rating',
        'a_surface_game_elo_rating',
        'a_surface_set_elo_rating',
        'a_surface_service_game_elo_rating',
        'a_surface_return_game_elo_rating',
        'a_surface_tie_break_elo_rating',
        'a_win_percent',
        'a_serve_rating',
        'a_return_rating',
        'a_pressure_rating',
        'a_avg_vs_elo',
        'a_matches_played',
        'b_elo_rating',
        'b_point_elo_rating',
        'b_game_elo_rating',
        'b_set_elo_rating',
        'b_service_game_elo_rating',
        'b_return_game_elo_rating',
        'b_tie_break_elo_rating',
        'b_surface_elo_rating',
        'b_surface_point_elo_rating',
        'b_surface_game_elo_rating',
        'b_surface_set_elo_rating',
        'b_surface_service_game_elo_rating',
        'b_surface_return_game_elo_rating',
        'b_surface_tie_break_elo_rating',
        'b_win_percent',
        'b_serve_rating',
        'b_return_rating',
        'b_pressure_rating',
        'b_avg_vs_elo',
        'b_matches_played',
        'a_b_win',
        'a_odds',
        'b_odds'
    ]

    new_format = pd.DataFrame(np.nan, index=range(0, len(matches_df)), columns=game_header)

    tasks = []
    # For surfaces + total elo
    pbar = tqdm(total=len(matches_df)*2, desc="Processing Elos")

    async def update_elos_with_progress(players_elo, row):
        await update_elos(players_elo, row)
        pbar.update(1)

    async def update_games(new_format: pd.DataFrame, index, row, players_elo, surface_players_elo):
        new_format.iloc[index] = pd.Series(await create_new_game_df(row, players_elo, surface_players_elo))

    for index, row in matches_df.iterrows():
        if row['surface'] == "Grass":
            tasks.append(update_games(new_format, index, row, players_elo, grass_players_elo))
            tasks.append(update_elos_with_progress(grass_players_elo, row))
        elif row['surface'] == "Clay":
            tasks.append(update_games(new_format, index, row, players_elo, clay_players_elo))
            tasks.append(update_elos_with_progress(clay_players_elo, row))
        elif row['surface'] == "Hard":
            tasks.append(update_games(new_format, index, row, players_elo, hard_players_elo))
            tasks.append(update_elos_with_progress(hard_players_elo, row))
        else:
            pbar.update(1)

        tasks.append(update_elos_with_progress(players_elo, row))

        # To avoid accumulating too many tasks, you can process them in smaller batches
        if len(tasks) >= 1000:
            await asyncio.gather(*tasks)
            tasks = []  # Reset the tasks list

    # Process any remaining tasks
    if tasks:
        await asyncio.gather(*tasks)

    pbar.close()

    new_format.to_csv('testout.csv', index=False)

    return new_format

async def create_new_game_df(game, players_elo, player_surface_elos):
    w_player = game['winner_name'] #Change to ID later
    l_player = game['loser_name'] #Change to ID later
    w_rank = game['winner_rank']
    l_rank = game['loser_rank']
    w_odds = game['winner_odds']
    l_odds = game['loser_odds']

    if random.choice([True, False]):
        player_a, player_b = w_player, l_player
        player_a_rank, player_b_rank = w_rank, l_rank
        player_a_odds, player_b_odds = w_odds, l_odds
        a_b_win = 1
    else:
        player_a, player_b = l_player, w_player
        player_a_rank, player_b_rank = l_rank, w_rank
        player_a_odds, player_b_odds = l_odds, w_odds
        a_b_win = 0

    a_player_elos = players_elo[players_elo['player'] == player_a]
    b_player_elos = players_elo[players_elo['player'] == player_b]

    a_surface_elos = player_surface_elos[player_surface_elos['player'] == player_a]
    b_surface_elos = player_surface_elos[player_surface_elos['player'] == player_b]

    # Construct the game entry
    game_entry = [
        game['tourney_id'],
        game['tourney_name'],
        game['tourney_date'],
        game['surface'],
        game['best_of'],
        game['match_num'],
        0,
        player_a,
        '',
        player_a_rank,
        0,
        player_b,
        '',
        player_b_rank,
        a_player_elos['elo_rating'],
        a_player_elos['point_elo_rating'],
        a_player_elos['game_elo_rating'],
        a_player_elos['set_elo_rating'],
        a_player_elos['service_game_elo_rating'],
        a_player_elos['return_game_elo_rating'],
        a_player_elos['tie_break_elo_rating'],
        a_surface_elos['elo_rating'],
        a_surface_elos['point_elo_rating'],
        a_surface_elos['game_elo_rating'],
        a_surface_elos['set_elo_rating'],
        a_surface_elos['service_game_elo_rating'],
        a_surface_elos['return_game_elo_rating'],
        a_surface_elos['tie_break_elo_rating'],
        0,  # Placeholder, calculate as needed
        0,  # Placeholder, calculate as needed
        0,  # Placeholder, calculate as needed
        0,  # Placeholder, calculate as needed
        0,  # Placeholder, calculate as needed
        0,  # Placeholder, calculate as needed
        b_player_elos['elo_rating'],
        b_player_elos['point_elo_rating'],
        b_player_elos['game_elo_rating'],
        b_player_elos['set_elo_rating'],
        b_player_elos['service_game_elo_rating'],
        b_player_elos['return_game_elo_rating'],
        b_player_elos['tie_break_elo_rating'],
        b_surface_elos['elo_rating'],
        b_surface_elos['point_elo_rating'],
        b_surface_elos['game_elo_rating'],
        b_surface_elos['set_elo_rating'],
        b_surface_elos['service_game_elo_rating'],
        b_surface_elos['return_game_elo_rating'],
        b_surface_elos['tie_break_elo_rating'],
        0,  # Placeholder, calculate as needed
        0,  # Placeholder, calculate as needed
        0,  # Placeholder, calculate as needed
        0,  # Placeholder, calculate as needed
        0,  # Placeholder, calculate as needed
        0,  # Placeholder, calculate as needed
        a_b_win,
        player_a_odds,
        player_b_odds
    ]

    return game_entry
