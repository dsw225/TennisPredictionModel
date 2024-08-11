import pandas as pd
from tqdm.asyncio import tqdm
import asyncio
from datetime import datetime, timedelta
from math import pow, copysign, floor, ceil
import random
import traceback
import numpy as np
from render.utils.glicko2.glicko2_updater import *
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
    players_to_glicko = combined_names.drop_duplicates().tolist()

    new_header = ['player', 'matches_played', 'glicko_rating', 'point_glicko_rating', 'game_glicko_rating', 
                  'set_glicko_rating', 'service_game_glicko_rating', 'return_game_glicko_rating', 'tie_break_glicko_rating', 'ace_glicko_rating', 
                  'return_ace_glicko_rating', 'first_won_glicko_rating', 'return_first_won_glicko_rating', 'second_won_glicko_rating', 'return_second_won_glicko_rating',]

    data = {
        'player': players_to_glicko,   
        'matches_played': [0] * len(players_to_glicko),
        'glicko_rating': [Rating() for _ in players_to_glicko],
        'point_glicko_rating': [Rating() for _ in players_to_glicko],
        'game_glicko_rating': [Rating() for _ in players_to_glicko],
        'set_glicko_rating': [Rating() for _ in players_to_glicko],
        'service_game_glicko_rating': [Rating() for _ in players_to_glicko],
        'return_game_glicko_rating': [Rating() for _ in players_to_glicko],
        'tie_break_glicko_rating': [Rating() for _ in players_to_glicko],
        'ace_glicko_rating': [Rating() for _ in players_to_glicko],
        'return_ace_glicko_rating': [Rating() for _ in players_to_glicko],
        'first_won_glicko_rating': [Rating() for _ in players_to_glicko],
        'return_first_won_glicko_rating': [Rating() for _ in players_to_glicko],
        'second_won_glicko_rating': [Rating() for _ in players_to_glicko],
        'return_second_won_glicko_rating': [Rating() for _ in players_to_glicko],
    }


    players_glicko = pd.DataFrame(data, columns=new_header)
    clay_players_glicko = pd.DataFrame(data, columns=new_header)
    grass_players_glicko = pd.DataFrame(data, columns=new_header)
    hard_players_glicko = pd.DataFrame(data, columns=new_header)

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

        'glicko_rating_diff_high',
        'glicko_rating_diff_low',
        'a_glicko_rating',
        'b_glicko_rating',
        'a_glicko_rd',
        'b_glicko_rd',

        'point_glicko_rating_diff_high',
        'point_glicko_rating_diff_low',
        'a_point_glicko_rating',
        'b_point_glicko_rating',

        'game_glicko_rating_diff_high',
        'game_glicko_rating_diff_low',
        'a_game_glicko_rating',
        'b_game_glicko_rating',

        'set_glicko_rating_diff_high',
        'set_glicko_rating_diff_low',
        'a_set_glicko_rating',
        'b_set_glicko_rating',

        'service_glicko_rating_diff_high',
        'service_glicko_rating_diff_low',
        'a_service_glicko_rating',
        'b_return_glicko_rating',

        'return_glicko_rating_diff_high',
        'return_glicko_rating_diff_low',
        'a_return_glicko_rating',
        'b_service_glicko_rating',

        'tiebreak_glicko_rating_diff_high',
        'tiebreak_glicko_rating_diff_low',
        'a_tiebreak_glicko_rating',
        'b_tiebreak_glicko_rating',

        'ace_glicko_rating_diff_high',
        'ace_glicko_rating_diff_low',
        'a_ace_glicko_rating',
        'b_return_ace_glicko_rating',

        'return_ace_glicko_rating_diff_high',
        'return_ace_glicko_rating_diff_low',
        'a_return_ace_glicko_rating',
        'b_ace_glicko_rating',

        'first_won_glicko_rating_diff_high',
        'first_won_glicko_rating_diff_low',
        'a_first_won_glicko_rating',
        'b_return_first_won_glicko_rating',

        'return_first_won_glicko_rating_diff_high',
        'return_first_won_glicko_rating_diff_low',
        'a_return_first_won_glicko_rating',
        'b_first_won_glicko_rating',

        'second_won_glicko_rating_diff_high',
        'second_won_glicko_rating_diff_low',
        'a_second_won_glicko_rating',
        'b_return_second_won_glicko_rating',

        'return_second_won_glicko_rating_diff_high',
        'return_second_won_glicko_rating_diff_low',
        'a_return_second_won_glicko_rating',
        'b_second_won_glicko_rating',



        'surface_glicko_rating_diff_high',
        'surface_glicko_rating_diff_low',
        'a_surface_glicko_rating',
        'b_surface_glicko_rating',
        'a_surface_glicko_rd',
        'b_surface_glicko_rd',

        'surface_point_glicko_rating_diff_high',
        'surface_point_glicko_rating_diff_low',
        'a_surface_point_glicko_rating',
        'b_surface_point_glicko_rating',

        'surface_game_glicko_rating_diff_high',
        'surface_game_glicko_rating_diff_low',
        'a_surface_game_glicko_rating',
        'b_surface_game_glicko_rating',

        'surface_set_glicko_rating_diff_high',
        'surface_set_glicko_rating_diff_low',
        'a_surface_set_glicko_rating',
        'b_surface_set_glicko_rating',

        'surface_service_glicko_rating_diff_high',
        'surface_service_glicko_rating_diff_low',
        'a_surface_service_glicko_rating',
        'b_surface_return_glicko_rating',

        'surface_return_glicko_rating_diff_high',
        'surface_return_glicko_rating_diff_low',
        'a_surface_return_glicko_rating',
        'b_surface_service_glicko_rating',

        'surface_tiebreak_glicko_rating_diff_high',
        'surface_tiebreak_glicko_rating_diff_low',
        'a_surface_tiebreak_glicko_rating',
        'b_surface_tiebreak_glicko_rating',

        'surface_ace_glicko_rating_diff_high',
        'surface_ace_glicko_rating_diff_low',
        'a_surface_ace_glicko_rating',
        'b_surface_return_ace_glicko_rating',

        'surface_return_ace_glicko_rating_diff_high',
        'surface_return_ace_glicko_rating_diff_low',
        'a_surface_return_ace_glicko_rating',
        'b_surface_ace_glicko_rating',

        'surface_first_won_glicko_rating_diff_high',
        'surface_first_won_glicko_rating_diff_low',
        'a_surface_first_won_glicko_rating',
        'b_surface_return_first_won_glicko_rating',

        'surface_return_first_won_glicko_rating_diff_high',
        'surface_return_first_won_glicko_rating_diff_low',
        'a_surface_return_first_won_glicko_rating',
        'b_surface_first_won_glicko_rating',

        'surface_second_won_glicko_rating_diff_high',
        'surface_second_won_glicko_rating_diff_low',
        'a_surface_second_won_glicko_rating',
        'b_surface_return_second_won_glicko_rating',

        'surface_return_second_won_glicko_rating_diff_high',
        'surface_return_second_won_glicko_rating_diff_low',
        'a_surface_return_second_won_glicko_rating',
        'b_surface_second_won_glicko_rating',


        'a_b_win',
        'a_odds',
        'b_odds'
    ]
    # print(len(game_header))

    new_format = pd.DataFrame(np.nan, index=range(0, len(matches_df)), columns=game_header)

    tasks = []
    # For surfaces + total glicko
    pbar = tqdm(total=len(matches_df)*2, desc="Processing glickos")

    async def update_glickos_with_progress(players_glicko, row):
        await update_glickos(players_glicko, row)
        pbar.update(1)

    async def update_games(new_format: pd.DataFrame, index, row, players_glicko, surface_players_glicko):
        new_format.iloc[index] = pd.Series(await create_new_game_df(row, players_glicko, surface_players_glicko))

    for index, row in matches_df.iterrows():
        if row['surface'] == "Grass":
            tasks.append(update_games(new_format, index, row, players_glicko, grass_players_glicko))
            tasks.append(update_glickos_with_progress(grass_players_glicko, row))
        elif row['surface'] == "Clay":
            tasks.append(update_games(new_format, index, row, players_glicko, clay_players_glicko))
            tasks.append(update_glickos_with_progress(clay_players_glicko, row))
        elif row['surface'] == "Hard":
            tasks.append(update_games(new_format, index, row, players_glicko, hard_players_glicko))
            tasks.append(update_glickos_with_progress(hard_players_glicko, row))
        else:
            pbar.update(1)

        tasks.append(update_glickos_with_progress(players_glicko, row))

        # To avoid accumulating too many tasks, you can process them in smaller batches
        if len(tasks) >= 1000:
            await asyncio.gather(*tasks)
            tasks = []  # Reset the tasks list

    # Process any remaining tasks
    if tasks:
        await asyncio.gather(*tasks)

    pbar.close()

    new_format.to_csv('testcsvs/estout3.csv', index=False)

    return new_format
async def create_new_game_df(game, players_glicko, player_surface_glickos):
    w_player = game['winner_name']  # Change to ID later
    l_player = game['loser_name']  # Change to ID later
    w_rank = game['winner_rank']
    l_rank = game['loser_rank']
    w_odds = game['winner_odds']
    l_odds = game['loser_odds']
    date = game['tourney_date']

    # Randomly decide which player is 'a' and which is 'b'
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

    # Ensure we're dealing with scalar values
    a_player_glickos = players_glicko[players_glicko['player'] == player_a].iloc[0]
    b_player_glickos = players_glicko[players_glicko['player'] == player_b].iloc[0]

    a_surface_glickos = player_surface_glickos[player_surface_glickos['player'] == player_a].iloc[0]
    b_surface_glickos = player_surface_glickos[player_surface_glickos['player'] == player_b].iloc[0]

    # Construct the game entry ensuring all values are scalar
    game_entry = [
        game['tourney_id'],
        game['tourney_name'],
        date,
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

        (a_player_glickos['glicko_rating'].getRating() - a_player_glickos['glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['glicko_rating'].getRating() + b_player_glickos['glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['glicko_rating'].getRating() + a_player_glickos['glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['glicko_rating'].getRating() - b_player_glickos['glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['glicko_rating'].getRating(),
        b_player_glickos['glicko_rating'].getRating(),
        a_player_glickos['glicko_rating'].get_pre_rating_rd(date),
        b_player_glickos['glicko_rating'].get_pre_rating_rd(date),

        (a_player_glickos['point_glicko_rating'].getRating() - a_player_glickos['point_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['point_glicko_rating'].getRating() + b_player_glickos['point_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['point_glicko_rating'].getRating() + a_player_glickos['point_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['point_glicko_rating'].getRating() - b_player_glickos['point_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['point_glicko_rating'].getRating(),
        b_player_glickos['point_glicko_rating'].getRating(),

        (a_player_glickos['game_glicko_rating'].getRating() - a_player_glickos['game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['game_glicko_rating'].getRating() + b_player_glickos['game_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['game_glicko_rating'].getRating() + a_player_glickos['game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['game_glicko_rating'].getRating() - b_player_glickos['game_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['game_glicko_rating'].getRating(),
        b_player_glickos['game_glicko_rating'].getRating(),

        (a_player_glickos['set_glicko_rating'].getRating() - a_player_glickos['set_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['set_glicko_rating'].getRating() + b_player_glickos['set_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['set_glicko_rating'].getRating() + a_player_glickos['set_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['set_glicko_rating'].getRating() - b_player_glickos['set_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['set_glicko_rating'].getRating(),
        b_player_glickos['set_glicko_rating'].getRating(),

        (a_player_glickos['service_game_glicko_rating'].getRating() - a_player_glickos['service_game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['return_game_glicko_rating'].getRating() + b_player_glickos['return_game_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['service_game_glicko_rating'].getRating() + a_player_glickos['service_game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['return_game_glicko_rating'].getRating() - b_player_glickos['return_game_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['service_game_glicko_rating'].getRating(),
        b_player_glickos['return_game_glicko_rating'].getRating(),

        (a_player_glickos['return_game_glicko_rating'].getRating() - a_player_glickos['return_game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['service_game_glicko_rating'].getRating() + b_player_glickos['service_game_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['return_game_glicko_rating'].getRating() + a_player_glickos['return_game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['service_game_glicko_rating'].getRating() - b_player_glickos['service_game_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['return_game_glicko_rating'].getRating(),
        b_player_glickos['service_game_glicko_rating'].getRating(),

        (a_player_glickos['tie_break_glicko_rating'].getRating() - a_player_glickos['tie_break_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['tie_break_glicko_rating'].getRating() + b_player_glickos['tie_break_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['tie_break_glicko_rating'].getRating() + a_player_glickos['tie_break_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['tie_break_glicko_rating'].getRating() - b_player_glickos['tie_break_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['tie_break_glicko_rating'].getRating(),
        b_player_glickos['tie_break_glicko_rating'].getRating(),

        (a_player_glickos['ace_glicko_rating'].getRating() - a_player_glickos['ace_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['return_ace_glicko_rating'].getRating() + b_player_glickos['return_ace_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['ace_glicko_rating'].getRating() + a_player_glickos['ace_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['return_ace_glicko_rating'].getRating() - b_player_glickos['return_ace_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['ace_glicko_rating'].getRating(),
        b_player_glickos['return_ace_glicko_rating'].getRating(),

        (a_player_glickos['return_ace_glicko_rating'].getRating() - a_player_glickos['return_ace_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['ace_glicko_rating'].getRating() + b_player_glickos['ace_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['return_ace_glicko_rating'].getRating() + a_player_glickos['return_ace_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['ace_glicko_rating'].getRating() - b_player_glickos['ace_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['return_ace_glicko_rating'].getRating(),
        b_player_glickos['ace_glicko_rating'].getRating(),

        (a_player_glickos['first_won_glicko_rating'].getRating() - a_player_glickos['first_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['return_first_won_glicko_rating'].getRating() + b_player_glickos['return_first_won_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['first_won_glicko_rating'].getRating() + a_player_glickos['first_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['return_first_won_glicko_rating'].getRating() - b_player_glickos['return_first_won_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['first_won_glicko_rating'].getRating(),
        b_player_glickos['return_first_won_glicko_rating'].getRating(),

        (a_player_glickos['return_first_won_glicko_rating'].getRating() - a_player_glickos['return_first_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['first_won_glicko_rating'].getRating() + b_player_glickos['first_won_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['return_first_won_glicko_rating'].getRating() + a_player_glickos['return_first_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['first_won_glicko_rating'].getRating() - b_player_glickos['first_won_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['return_first_won_glicko_rating'].getRating(),
        b_player_glickos['first_won_glicko_rating'].getRating(),

        (a_player_glickos['second_won_glicko_rating'].getRating() - a_player_glickos['second_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['return_second_won_glicko_rating'].getRating() + b_player_glickos['return_second_won_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['second_won_glicko_rating'].getRating() + a_player_glickos['second_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['return_second_won_glicko_rating'].getRating() - b_player_glickos['return_second_won_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['second_won_glicko_rating'].getRating(),
        b_player_glickos['return_second_won_glicko_rating'].getRating(),

        (a_player_glickos['return_second_won_glicko_rating'].getRating() - a_player_glickos['return_second_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['second_won_glicko_rating'].getRating() + b_player_glickos['second_won_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_player_glickos['return_second_won_glicko_rating'].getRating() + a_player_glickos['return_second_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_player_glickos['second_won_glicko_rating'].getRating() - b_player_glickos['second_won_glicko_rating'].get_pre_rating_rd(date)),
        
        a_player_glickos['return_second_won_glicko_rating'].getRating(),
        b_player_glickos['second_won_glicko_rating'].getRating(),

        # Surface-specific Glicko ratings
        (a_surface_glickos['glicko_rating'].getRating() - a_surface_glickos['glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['glicko_rating'].getRating() + b_surface_glickos['glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['glicko_rating'].getRating() + a_surface_glickos['glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['glicko_rating'].getRating() - b_surface_glickos['glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['glicko_rating'].getRating(),
        b_surface_glickos['glicko_rating'].getRating(),
        a_surface_glickos['glicko_rating'].get_pre_rating_rd(date),
        b_surface_glickos['glicko_rating'].get_pre_rating_rd(date),

        (a_surface_glickos['point_glicko_rating'].getRating() - a_surface_glickos['point_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['point_glicko_rating'].getRating() + b_surface_glickos['point_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['point_glicko_rating'].getRating() + a_surface_glickos['point_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['point_glicko_rating'].getRating() - b_surface_glickos['point_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['point_glicko_rating'].getRating(),
        b_surface_glickos['point_glicko_rating'].getRating(),

        (a_surface_glickos['game_glicko_rating'].getRating() - a_surface_glickos['game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['game_glicko_rating'].getRating() + b_surface_glickos['game_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['game_glicko_rating'].getRating() + a_surface_glickos['game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['game_glicko_rating'].getRating() - b_surface_glickos['game_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['game_glicko_rating'].getRating(),
        b_surface_glickos['game_glicko_rating'].getRating(),

        (a_surface_glickos['set_glicko_rating'].getRating() - a_surface_glickos['set_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['set_glicko_rating'].getRating() + b_surface_glickos['set_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['set_glicko_rating'].getRating() + a_surface_glickos['set_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['set_glicko_rating'].getRating() - b_surface_glickos['set_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['set_glicko_rating'].getRating(),
        b_surface_glickos['set_glicko_rating'].getRating(),

        (a_surface_glickos['service_game_glicko_rating'].getRating() - a_surface_glickos['service_game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['return_game_glicko_rating'].getRating() + b_surface_glickos['return_game_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['service_game_glicko_rating'].getRating() + a_surface_glickos['service_game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['return_game_glicko_rating'].getRating() - b_surface_glickos['return_game_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['service_game_glicko_rating'].getRating(),
        b_surface_glickos['return_game_glicko_rating'].getRating(),

        (a_surface_glickos['return_game_glicko_rating'].getRating() - a_surface_glickos['return_game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['service_game_glicko_rating'].getRating() + b_surface_glickos['service_game_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['return_game_glicko_rating'].getRating() + a_surface_glickos['return_game_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['service_game_glicko_rating'].getRating() - b_surface_glickos['service_game_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['return_game_glicko_rating'].getRating(),
        b_surface_glickos['service_game_glicko_rating'].getRating(),

        (a_surface_glickos['tie_break_glicko_rating'].getRating() - a_surface_glickos['tie_break_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['tie_break_glicko_rating'].getRating() + b_surface_glickos['tie_break_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['tie_break_glicko_rating'].getRating() + a_surface_glickos['tie_break_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['tie_break_glicko_rating'].getRating() - b_surface_glickos['tie_break_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['tie_break_glicko_rating'].getRating(),
        b_surface_glickos['tie_break_glicko_rating'].getRating(),

        (a_surface_glickos['ace_glicko_rating'].getRating() - a_surface_glickos['ace_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['return_ace_glicko_rating'].getRating() + b_surface_glickos['return_ace_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['ace_glicko_rating'].getRating() + a_surface_glickos['ace_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['return_ace_glicko_rating'].getRating() - b_surface_glickos['return_ace_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['ace_glicko_rating'].getRating(),
        b_surface_glickos['return_ace_glicko_rating'].getRating(),

        (a_surface_glickos['return_ace_glicko_rating'].getRating() - a_surface_glickos['return_ace_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['ace_glicko_rating'].getRating() + b_surface_glickos['ace_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['return_ace_glicko_rating'].getRating() + a_surface_glickos['return_ace_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['ace_glicko_rating'].getRating() - b_surface_glickos['ace_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['return_ace_glicko_rating'].getRating(),
        b_surface_glickos['ace_glicko_rating'].getRating(),

        (a_surface_glickos['first_won_glicko_rating'].getRating() - a_surface_glickos['first_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['return_first_won_glicko_rating'].getRating() + b_surface_glickos['return_first_won_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['first_won_glicko_rating'].getRating() + a_surface_glickos['first_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['return_first_won_glicko_rating'].getRating() - b_surface_glickos['return_first_won_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['first_won_glicko_rating'].getRating(),
        b_surface_glickos['return_first_won_glicko_rating'].getRating(),

        (a_surface_glickos['return_first_won_glicko_rating'].getRating() - a_surface_glickos['return_first_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['first_won_glicko_rating'].getRating() + b_surface_glickos['first_won_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['return_first_won_glicko_rating'].getRating() + a_surface_glickos['return_first_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['first_won_glicko_rating'].getRating() - b_surface_glickos['first_won_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['return_first_won_glicko_rating'].getRating(),
        b_surface_glickos['first_won_glicko_rating'].getRating(),

        (a_surface_glickos['second_won_glicko_rating'].getRating() - a_surface_glickos['second_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['return_second_won_glicko_rating'].getRating() + b_surface_glickos['return_second_won_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['second_won_glicko_rating'].getRating() + a_surface_glickos['second_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['return_second_won_glicko_rating'].getRating() - b_surface_glickos['return_second_won_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['second_won_glicko_rating'].getRating(),
        b_surface_glickos['return_second_won_glicko_rating'].getRating(),

        (a_surface_glickos['return_second_won_glicko_rating'].getRating() - a_surface_glickos['return_second_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['second_won_glicko_rating'].getRating() + b_surface_glickos['second_won_glicko_rating'].get_pre_rating_rd(date)),
        
        (a_surface_glickos['return_second_won_glicko_rating'].getRating() + a_surface_glickos['return_second_won_glicko_rating'].get_pre_rating_rd(date)) -
        (b_surface_glickos['second_won_glicko_rating'].getRating() - b_surface_glickos['second_won_glicko_rating'].get_pre_rating_rd(date)),
        
        a_surface_glickos['return_second_won_glicko_rating'].getRating(),
        b_surface_glickos['second_won_glicko_rating'].getRating(),

        a_b_win,
        player_a_odds,
        player_b_odds
    ]

    return game_entry

