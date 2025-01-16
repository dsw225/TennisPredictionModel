import pandas as pd
from tqdm.asyncio import tqdm
import asyncio
from datetime import datetime, timedelta
from math import pow, copysign, floor, ceil
import random
import traceback
import numpy as np
from render.utils.stephenson.stephenson_updater import *
from render.utils.stephenson.stephenson import *
import arff
import copy

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
    players_to_steph = combined_names.drop_duplicates().tolist()

    new_header = ['player', 'matches_played', 'steph_rating', 'point_steph_rating', 'game_steph_rating', 
                  'set_steph_rating', 'service_game_steph_rating', 'return_game_steph_rating', 'tie_break_steph_rating', 'bp_steph_rating', 'ace_steph_rating', 
                  'return_ace_steph_rating', 'first_won_steph_rating', 'return_first_won_steph_rating', 'second_won_steph_rating', 'return_second_won_steph_rating',]

    data = {
        'player': players_to_steph,   
        'matches_played': [0] * len(players_to_steph),
        'steph_rating': [Stephenson() for _ in players_to_steph],
        'point_steph_rating': [Stephenson() for _ in players_to_steph],
        'game_steph_rating': [Stephenson() for _ in players_to_steph],
        'set_steph_rating': [Stephenson() for _ in players_to_steph],
        'service_game_steph_rating': [Stephenson() for _ in players_to_steph],
        'return_game_steph_rating': [Stephenson() for _ in players_to_steph],
        'tie_break_steph_rating': [Stephenson() for _ in players_to_steph],
        'bp_steph_rating': [Stephenson() for _ in players_to_steph],
        'ace_steph_rating': [Stephenson() for _ in players_to_steph],
        'return_ace_steph_rating': [Stephenson() for _ in players_to_steph],
        'first_won_steph_rating': [Stephenson() for _ in players_to_steph],
        'return_first_won_steph_rating': [Stephenson() for _ in players_to_steph],
        'second_won_steph_rating': [Stephenson() for _ in players_to_steph],
        'return_second_won_steph_rating': [Stephenson() for _ in players_to_steph],
    }

    # Create deep copies of the data for each DataFrame
    data_copy_1 = copy.deepcopy(data)
    data_copy_2 = copy.deepcopy(data)
    data_copy_3 = copy.deepcopy(data)
    data_copy_4 = copy.deepcopy(data)

    # Initialize DataFrames with separate data copies
    players_steph = pd.DataFrame(data_copy_1, columns=new_header)
    clay_players_steph = pd.DataFrame(data_copy_2, columns=new_header)
    grass_players_steph = pd.DataFrame(data_copy_3, columns=new_header)
    hard_players_steph = pd.DataFrame(data_copy_4, columns=new_header)

    game_header = [
        'tourney_id',
        'tourney_name',
        'tourney_date',
        'surface',
        'best_of',
        'match_num',
        'tourney_level', #Performs much worse?
        'tourney_round',

        'a_player_id',
        'a_player_name',
        'a_player_age',
        'a_player_hand',
        'a_player_ht',
        'a_player_slug',
        'a_player_rank',
        'a_player_rank_points',
        'b_player_id',
        'b_player_name',
        'b_player_age',
        'b_player_hand',
        'b_player_ht',
        'b_player_slug',
        'b_player_rank',
        'b_player_rank_points',

        'rank_diff',
        'rank_points_diff',
        'age_diff',
        'ht_diff',

        'steph_rating_diff',
        'a_steph_rating',
        'b_steph_rating',
        'a_steph_rd',
        'b_steph_rd',

        'point_steph_rating_diff',
        'a_point_steph_rating',
        'b_point_steph_rating',
        'a_point_steph_rd',
        'b_point_steph_rd',

        'game_steph_rating_diff',
        'a_game_steph_rating',
        'b_game_steph_rating',
        'a_game_steph_rd',
        'b_game_steph_rd',

        'set_steph_rating_diff',
        'a_set_steph_rating',
        'b_set_steph_rating',
        'a_set_steph_rd',
        'b_set_steph_rd',

        'service_steph_rating_diff',
        'a_service_steph_rating',
        'b_return_steph_rating',
        'a_service_steph_rd',
        'b_return_steph_rd',

        'return_steph_rating_diff',
        'a_return_steph_rating',
        'b_service_steph_rating',
        'a_return_steph_rd',
        'b_service_steph_rd',

        'tiebreak_steph_rating_diff',
        'a_tiebreak_steph_rating',
        'b_tiebreak_steph_rating',
        'a_tiebreak_steph_rd',
        'b_tiebreak_steph_rd',

        'bp_steph_rating_diff',
        'a_bp_steph_rating',
        'b_bp_steph_rating',
        'a_bp_steph_rd',
        'b_bp_steph_rd',

        'ace_steph_rating_diff',
        'a_ace_steph_rating',
        'b_return_ace_steph_rating',
        'a_ace_steph_rd',
        'b_return_ace_steph_rd',

        'return_ace_steph_rating_diff',
        'a_return_ace_steph_rating',
        'b_ace_steph_rating',
        'a_return_ace_steph_rd',
        'b_ace_steph_rd',

        'first_won_steph_rating_diff',
        'a_first_won_steph_rating',
        'b_return_first_won_steph_rating',
        'a_first_won_steph_rd',
        'b_return_first_won_steph_rd',

        'return_first_won_steph_rating_diff',
        'a_return_first_won_steph_rating',
        'b_first_won_steph_rating',
        'a_return_first_won_steph_rd',
        'b_first_won_steph_rd',

        'second_won_steph_rating_diff',
        'a_second_won_steph_rating',
        'b_return_second_won_steph_rating',
        'a_second_won_steph_rd',
        'b_return_second_won_steph_rd',

        'return_second_won_steph_rating_diff',
        'a_return_second_won_steph_rating',
        'b_second_won_steph_rating',
        'a_return_second_won_steph_rd',
        'b_second_won_steph_rd',

        
        
        'surface_steph_rating_diff',
        'a_surface_steph_rating',
        'b_surface_steph_rating',
        'a_surface_steph_rd',
        'b_surface_steph_rd',

        'surface_point_steph_rating_diff',
        'a_surface_point_steph_rating',
        'b_surface_point_steph_rating',
        'a_surface_point_steph_rd',
        'b_surface_point_steph_rd',

        'surface_game_steph_rating_diff',
        'a_surface_game_steph_rating',
        'b_surface_game_steph_rating',
        'a_surface_game_steph_rd',
        'b_surface_game_steph_rd',

        'surface_set_steph_rating_diff',
        'a_surface_set_steph_rating',
        'b_surface_set_steph_rating',
        'a_surface_set_steph_rd',
        'b_surface_set_steph_rd',

        'surface_service_steph_rating_diff',
        'a_surface_service_steph_rating',
        'b_surface_return_steph_rating',
        'a_surface_service_steph_rd',
        'b_surface_return_steph_rd',

        'surface_return_steph_rating_diff',
        'a_surface_return_steph_rating',
        'b_surface_service_steph_rating',
        'a_surface_return_steph_rd',
        'b_surface_service_steph_rd',

        'surface_tiebreak_steph_rating_diff',
        'a_surface_tiebreak_steph_rating',
        'b_surface_tiebreak_steph_rating',
        'a_surface_tiebreak_steph_rd',
        'b_surface_tiebreak_steph_rd',

        'surface_bp_steph_rating_diff',
        'a_surface_bp_steph_rating',
        'b_surface_bp_steph_rating',
        'a_surface_bp_steph_rd',
        'b_surface_bp_steph_rd',

        'surface_ace_steph_rating_diff',
        'a_surface_ace_steph_rating',
        'b_surface_return_ace_steph_rating',
        'a_surface_ace_steph_rd',
        'b_surface_return_ace_steph_rd',

        'surface_return_ace_steph_rating_diff',
        'a_surface_return_ace_steph_rating',
        'b_surface_ace_steph_rating',
        'a_surface_return_ace_steph_rd',
        'b_surface_ace_steph_rd',

        'surface_first_won_steph_rating_diff',
        'a_surface_first_won_steph_rating',
        'b_surface_return_first_won_steph_rating',
        'a_surface_first_won_steph_rd',
        'b_surface_return_first_won_steph_rd',

        'surface_return_first_won_steph_rating_diff',
        'a_surface_return_first_won_steph_rating',
        'b_surface_first_won_steph_rating',
        'a_surface_return_first_won_steph_rd',
        'b_surface_first_won_steph_rd',

        'surface_second_won_steph_rating_diff',
        'a_surface_second_won_steph_rating',
        'b_surface_return_second_won_steph_rating',
        'a_surface_second_won_steph_rd',
        'b_surface_return_second_won_steph_rd',

        'surface_return_second_won_steph_rating_diff',
        'a_surface_return_second_won_steph_rating',
        'b_surface_second_won_steph_rating',
        'a_surface_return_second_won_steph_rd',
        'b_surface_second_won_steph_rd',

        'sets',
        'games',
        'tiebreaks',

        'a_odds',
        'b_odds',
        'a_b_win',
    ]
    # print(len(game_header))

    new_format = pd.DataFrame(np.nan, index=range(0, len(matches_df)), columns=game_header)

    tasks = []
    # For surfaces + total steph
    pbar = tqdm(total=len(matches_df)*2, desc="Processing stephs")

    async def update_stephs_with_progress(players_steph, row):
        await update_stephs(players_steph, row)
        pbar.update(1)

    async def update_games(new_format: pd.DataFrame, index, row, players_steph, surface_players_steph):
        new_format.iloc[index] = pd.Series(await create_new_game_df(row, players_steph, surface_players_steph))

    for index, row in matches_df.iterrows():
        if row['surface'] == "Grass":
            tasks.append(update_games(new_format, index, row, players_steph, grass_players_steph))
            tasks.append(update_stephs_with_progress(grass_players_steph, row))
        elif row['surface'] == "Clay":
            tasks.append(update_games(new_format, index, row, players_steph, clay_players_steph))
            tasks.append(update_stephs_with_progress(clay_players_steph, row))
        elif row['surface'] == "Hard":
            tasks.append(update_games(new_format, index, row, players_steph, hard_players_steph))
            tasks.append(update_stephs_with_progress(hard_players_steph, row))
        else:
            pbar.update(1)

        tasks.append(update_stephs_with_progress(players_steph, row))

        # To avoid accumulating too many tasks, you can process them in smaller batches
        if len(tasks) >= 1000:
            await asyncio.gather(*tasks)
            tasks = []  # Reset the tasks list

    # Process any remaining tasks
    if tasks:
        await asyncio.gather(*tasks)

    pbar.close()

    new_format.to_csv(f'testcsvs/StephNRP{RATING_PERIOD}.csv', index=False)

    return new_format

async def create_new_game_df(game, players_steph, player_surface_stephs):
    w_player = game['winner_name']  # Change to ID later
    l_player = game['loser_name']  # Change to ID later
    w_age = game['winner_age']
    l_age = game['loser_age']
    w_hand = game['winner_hand']
    l_hand = game['loser_hand']
    w_ht = game['winner_ht']
    l_ht = game['loser_ht']
    w_rank = game['winner_rank']
    w_rank_points = game['winner_rank_points']
    l_rank_points = game['loser_rank_points']
    l_rank = game['loser_rank']
    w_odds = game['winner_odds']
    l_odds = game['loser_odds']
    date = game['tourney_date']
    # Add game importance factor
    tourney_rating_level = {
        "A": 2, "O": 3, "G": 6, "M": 4, "F" : 5
    }
    tourney_level = tourney_rating_level.get(game['tourney_level'], 1)
    round_factors = {
        "F": 1.0, "BR": 0.95, "SF": 0.90, "QF": 0.85, "R16": 0.80, "R32": 0.80,
        "R64": 0.75, "R128": 0.75, "RR": 0.85
    }
    tourney_round = round_factors.get(game['round'], .75)


    #For testing over/under games/sets/tiebreaks
    w_games, l_games, w_sets, l_sets, _, _, total_tiebreaks = get_score_stats(game)
    total_sets = w_sets+l_sets
    total_games = w_games + l_games
    # print(total_games)
    surface_ratios = {
        "Hard": 22.75, "Clay": 21.29, "Grass": 22.87
    }

    total_games = 1 if total_games > surface_ratios.get(game['surface'], 22.5) else 0
    total_sets = 1 if total_sets > 2.5 and game['best_of'] < 5 else 0 if game['best_of'] < 5 else -20

    # Randomly decide which player is 'a' and which is 'b'
    if random.choice([True, False]):
        player_a, player_b = w_player, l_player
        player_a_hand, player_b_hand = w_hand, l_hand
        player_a_ht, player_b_ht = w_ht, l_ht
        player_a_rank, player_b_rank = w_rank, l_rank
        player_a_rank_points, player_b_rank_points = w_rank_points, l_rank_points
        player_a_odds, player_b_odds = w_odds, l_odds
        player_a_age, player_b_age = w_age, l_age
        a_b_win = 1
    else:
        player_a, player_b = l_player, w_player
        player_a_hand, player_b_hand = l_hand, w_hand
        player_a_ht, player_b_ht = l_ht, w_ht
        player_a_rank, player_b_rank = l_rank, w_rank
        player_a_rank_points, player_b_rank_points = l_rank_points, w_rank_points
        player_a_odds, player_b_odds = l_odds, w_odds
        player_a_age, player_b_age = l_age, w_age
        a_b_win = 0

    player_a_hand = 1 if player_a_hand == 'R' else 0
    player_b_hand = 1 if player_b_hand == 'R' else 0

    # Ensure we're dealing with scalar values
    a_player_stephs = players_steph[players_steph['player'] == player_a].iloc[0]
    b_player_stephs = players_steph[players_steph['player'] == player_b].iloc[0]

    a_surface_stephs = player_surface_stephs[player_surface_stephs['player'] == player_a].iloc[0]
    b_surface_stephs = player_surface_stephs[player_surface_stephs['player'] == player_b].iloc[0]

    # Construct the game entry ensuring all values are scalar
    game_entry = [
        game['tourney_id'],
        game['tourney_name'],
        date,
        game['surface'],
        game['best_of'],
        game['match_num'],
        tourney_level,
        tourney_round,
        0,
        player_a,
        player_a_age,
        player_a_hand,
        player_a_ht,
        '',
        player_a_rank,
        player_a_rank_points,
        0,
        player_b,
        player_b_age,
        player_b_hand,
        player_b_ht,
        '',
        player_b_rank,
        player_b_rank_points,

        player_a_rank - player_b_rank,
        player_a_rank_points - player_b_rank_points,
        player_a_age - player_b_age,
        player_a_ht - player_b_ht,

        a_player_stephs['steph_rating'].getRating() - b_player_stephs['steph_rating'].getRating(),
        a_player_stephs['steph_rating'].getRating(),
        b_player_stephs['steph_rating'].getRating(),
        a_player_stephs['steph_rating'].updateGetVar(),
        b_player_stephs['steph_rating'].updateGetVar(),

        a_player_stephs['point_steph_rating'].getRating() - b_player_stephs['point_steph_rating'].getRating(),
        a_player_stephs['point_steph_rating'].getRating(),
        b_player_stephs['point_steph_rating'].getRating(),
        a_player_stephs['point_steph_rating'].updateGetVar(),
        b_player_stephs['point_steph_rating'].updateGetVar(),

        a_player_stephs['game_steph_rating'].getRating() - b_player_stephs['game_steph_rating'].getRating(),
        a_player_stephs['game_steph_rating'].getRating(),
        b_player_stephs['game_steph_rating'].getRating(),
        a_player_stephs['game_steph_rating'].updateGetVar(),
        b_player_stephs['game_steph_rating'].updateGetVar(),

        a_player_stephs['set_steph_rating'].getRating() - b_player_stephs['set_steph_rating'].getRating(),
        a_player_stephs['set_steph_rating'].getRating(),
        b_player_stephs['set_steph_rating'].getRating(),
        a_player_stephs['set_steph_rating'].updateGetVar(),
        b_player_stephs['set_steph_rating'].updateGetVar(),
        
        a_player_stephs['service_game_steph_rating'].getRating() - b_player_stephs['return_game_steph_rating'].getRating(),
        a_player_stephs['service_game_steph_rating'].getRating(),
        b_player_stephs['return_game_steph_rating'].getRating(),
        a_player_stephs['service_game_steph_rating'].updateGetVar(),
        b_player_stephs['return_game_steph_rating'].updateGetVar(),
        
        a_player_stephs['return_game_steph_rating'].getRating() - b_player_stephs['service_game_steph_rating'].getRating(),
        a_player_stephs['return_game_steph_rating'].getRating(),
        b_player_stephs['service_game_steph_rating'].getRating(),
        a_player_stephs['return_game_steph_rating'].updateGetVar(),
        b_player_stephs['service_game_steph_rating'].updateGetVar(),

        a_player_stephs['tie_break_steph_rating'].getRating() - b_player_stephs['tie_break_steph_rating'].getRating(),
        a_player_stephs['tie_break_steph_rating'].getRating(),
        b_player_stephs['tie_break_steph_rating'].getRating(),
        a_player_stephs['tie_break_steph_rating'].updateGetVar(),
        b_player_stephs['tie_break_steph_rating'].updateGetVar(),

        a_player_stephs['bp_steph_rating'].getRating() - b_player_stephs['bp_steph_rating'].getRating(),
        a_player_stephs['bp_steph_rating'].getRating(),
        b_player_stephs['bp_steph_rating'].getRating(),
        a_player_stephs['bp_steph_rating'].updateGetVar(),
        b_player_stephs['bp_steph_rating'].updateGetVar(),
        
        a_player_stephs['ace_steph_rating'].getRating() - b_player_stephs['return_ace_steph_rating'].getRating(),
        a_player_stephs['ace_steph_rating'].getRating(),
        b_player_stephs['return_ace_steph_rating'].getRating(),
        a_player_stephs['ace_steph_rating'].updateGetVar(),
        b_player_stephs['return_ace_steph_rating'].updateGetVar(),
        
        a_player_stephs['return_ace_steph_rating'].getRating() - b_player_stephs['ace_steph_rating'].getRating(),
        a_player_stephs['return_ace_steph_rating'].getRating(),
        b_player_stephs['ace_steph_rating'].getRating(),
        a_player_stephs['return_ace_steph_rating'].updateGetVar(),
        b_player_stephs['ace_steph_rating'].updateGetVar(),

        a_player_stephs['first_won_steph_rating'].getRating() - b_player_stephs['return_first_won_steph_rating'].getRating(),
        a_player_stephs['first_won_steph_rating'].getRating(),
        b_player_stephs['return_first_won_steph_rating'].getRating(),
        a_player_stephs['first_won_steph_rating'].updateGetVar(),
        b_player_stephs['return_first_won_steph_rating'].updateGetVar(),
        
        a_player_stephs['return_first_won_steph_rating'].getRating() - b_player_stephs['first_won_steph_rating'].getRating(),
        a_player_stephs['return_first_won_steph_rating'].getRating(),
        b_player_stephs['first_won_steph_rating'].getRating(),
        a_player_stephs['return_first_won_steph_rating'].updateGetVar(),
        b_player_stephs['first_won_steph_rating'].updateGetVar(),

        a_player_stephs['second_won_steph_rating'].getRating() - b_player_stephs['return_second_won_steph_rating'].getRating(),
        a_player_stephs['second_won_steph_rating'].getRating(),
        b_player_stephs['return_second_won_steph_rating'].getRating(),
        a_player_stephs['second_won_steph_rating'].updateGetVar(),
        b_player_stephs['return_second_won_steph_rating'].updateGetVar(),
        
        a_player_stephs['return_second_won_steph_rating'].getRating() - b_player_stephs['second_won_steph_rating'].getRating(),
        a_player_stephs['return_second_won_steph_rating'].getRating(),
        b_player_stephs['second_won_steph_rating'].getRating(),
        a_player_stephs['return_second_won_steph_rating'].updateGetVar(),
        b_player_stephs['second_won_steph_rating'].updateGetVar(),

        # Surface-specific Glicko ratings        
        a_surface_stephs['steph_rating'].getRating() - b_surface_stephs['steph_rating'].getRating(),
        a_surface_stephs['steph_rating'].getRating(),
        b_surface_stephs['steph_rating'].getRating(),
        a_surface_stephs['steph_rating'].updateGetVar(),
        b_surface_stephs['steph_rating'].updateGetVar(),

        a_surface_stephs['point_steph_rating'].getRating() - b_surface_stephs['point_steph_rating'].getRating(),
        a_surface_stephs['point_steph_rating'].getRating(),
        b_surface_stephs['point_steph_rating'].getRating(),
        a_surface_stephs['point_steph_rating'].updateGetVar(),
        b_surface_stephs['point_steph_rating'].updateGetVar(),

        a_surface_stephs['game_steph_rating'].getRating() - b_surface_stephs['game_steph_rating'].getRating(),
        a_surface_stephs['game_steph_rating'].getRating(),
        b_surface_stephs['game_steph_rating'].getRating(),
        a_surface_stephs['game_steph_rating'].updateGetVar(),
        b_surface_stephs['game_steph_rating'].updateGetVar(),

        a_surface_stephs['set_steph_rating'].getRating() - b_surface_stephs['set_steph_rating'].getRating(),
        a_surface_stephs['set_steph_rating'].getRating(),
        b_surface_stephs['set_steph_rating'].getRating(),
        a_surface_stephs['set_steph_rating'].updateGetVar(),
        b_surface_stephs['set_steph_rating'].updateGetVar(),
        
        a_surface_stephs['service_game_steph_rating'].getRating() - b_surface_stephs['return_game_steph_rating'].getRating(),
        a_surface_stephs['service_game_steph_rating'].getRating(),
        b_surface_stephs['return_game_steph_rating'].getRating(),
        a_surface_stephs['service_game_steph_rating'].updateGetVar(),
        b_surface_stephs['return_game_steph_rating'].updateGetVar(),
        
        a_surface_stephs['return_game_steph_rating'].getRating() - b_surface_stephs['service_game_steph_rating'].getRating(),
        a_surface_stephs['return_game_steph_rating'].getRating(),
        b_surface_stephs['service_game_steph_rating'].getRating(),
        a_surface_stephs['return_game_steph_rating'].updateGetVar(),
        b_surface_stephs['service_game_steph_rating'].updateGetVar(),

        a_surface_stephs['tie_break_steph_rating'].getRating() - b_surface_stephs['tie_break_steph_rating'].getRating(),
        a_surface_stephs['tie_break_steph_rating'].getRating(),
        b_surface_stephs['tie_break_steph_rating'].getRating(),
        a_surface_stephs['tie_break_steph_rating'].updateGetVar(),
        b_surface_stephs['tie_break_steph_rating'].updateGetVar(),

        a_surface_stephs['bp_steph_rating'].getRating() - b_surface_stephs['bp_steph_rating'].getRating(),
        a_surface_stephs['bp_steph_rating'].getRating(),
        b_surface_stephs['bp_steph_rating'].getRating(),
        a_surface_stephs['bp_steph_rating'].updateGetVar(),
        b_surface_stephs['bp_steph_rating'].updateGetVar(),
        
        a_surface_stephs['ace_steph_rating'].getRating() - b_surface_stephs['return_ace_steph_rating'].getRating(),
        a_surface_stephs['ace_steph_rating'].getRating(),
        b_surface_stephs['return_ace_steph_rating'].getRating(),
        a_surface_stephs['ace_steph_rating'].updateGetVar(),
        b_surface_stephs['return_ace_steph_rating'].updateGetVar(),
        
        a_surface_stephs['return_ace_steph_rating'].getRating() - b_surface_stephs['ace_steph_rating'].getRating(),
        a_surface_stephs['return_ace_steph_rating'].getRating(),
        b_surface_stephs['ace_steph_rating'].getRating(),
        a_surface_stephs['return_ace_steph_rating'].updateGetVar(),
        b_surface_stephs['ace_steph_rating'].updateGetVar(),

        a_surface_stephs['first_won_steph_rating'].getRating() - b_surface_stephs['return_first_won_steph_rating'].getRating(),
        a_surface_stephs['first_won_steph_rating'].getRating(),
        b_surface_stephs['return_first_won_steph_rating'].getRating(),
        a_surface_stephs['first_won_steph_rating'].updateGetVar(),
        b_surface_stephs['return_first_won_steph_rating'].updateGetVar(),
        
        a_surface_stephs['return_first_won_steph_rating'].getRating() - b_surface_stephs['first_won_steph_rating'].getRating(),
        a_surface_stephs['return_first_won_steph_rating'].getRating(),
        b_surface_stephs['first_won_steph_rating'].getRating(),
        a_surface_stephs['return_first_won_steph_rating'].updateGetVar(),
        b_surface_stephs['first_won_steph_rating'].updateGetVar(),

        a_surface_stephs['second_won_steph_rating'].getRating() - b_surface_stephs['return_second_won_steph_rating'].getRating(),
        a_surface_stephs['second_won_steph_rating'].getRating(),
        b_surface_stephs['return_second_won_steph_rating'].getRating(),
        a_surface_stephs['second_won_steph_rating'].updateGetVar(),
        b_surface_stephs['return_second_won_steph_rating'].updateGetVar(),
        
        a_surface_stephs['return_second_won_steph_rating'].getRating() - b_surface_stephs['second_won_steph_rating'].getRating(),
        a_surface_stephs['return_second_won_steph_rating'].getRating(),
        b_surface_stephs['second_won_steph_rating'].getRating(),
        a_surface_stephs['return_second_won_steph_rating'].updateGetVar(),
        b_surface_stephs['second_won_steph_rating'].updateGetVar(),

        total_sets,
        total_games,
        total_tiebreaks,

        player_a_odds,
        player_b_odds,
        a_b_win
    ]

    return game_entry

