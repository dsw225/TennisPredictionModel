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

    new_header = ['player', 'last_date', 'match_number', 'matches_played', 'glicko_rating', 'point_glicko_rating', 'game_glicko_rating', 
                  'set_glicko_rating', 'service_game_glicko_rating', 'return_game_glicko_rating', 'tie_break_glicko_rating', 'ace_glicko_rating', 
                  'return_ace_glicko_rating', 'first_won_glicko_rating', 'return_first_won_glicko_rating', 'second_won_glicko_rating', 'return_second_won_glicko_rating',]

    data = {
        'player': players_to_glicko,   
        'matches_played': [0] * len(players_to_glicko),
        'glicko_rating': [Rating()] * len(players_to_glicko),
        'point_glicko_rating': [Rating()] * len(players_to_glicko),
        'game_glicko_rating': [Rating()] * len(players_to_glicko),
        'set_glicko_rating': [Rating()] * len(players_to_glicko),
        'service_game_glicko_rating': [Rating()] * len(players_to_glicko),
        'return_game_glicko_rating': [Rating()] * len(players_to_glicko),
        'tie_break_glicko_rating': [Rating()] * len(players_to_glicko),
        'ace_glicko_rating': [Rating()] * len(players_to_glicko),
        'return_ace_glicko_rating': [Rating()] * len(players_to_glicko),
        'first_won_glicko_rating': [Rating()] * len(players_to_glicko),
        'return_first_won_glicko_rating': [Rating()] * len(players_to_glicko),
        'second_won_glicko_rating': [Rating()] * len(players_to_glicko),
        'return_second_won_glicko_rating': [Rating()] * len(players_to_glicko),
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

        'a_glicko_rating',
        'a_glicko_rd',
        'a_glicko_vol',
        'a_point_glicko_rating',
        'a_point_glicko_rd',
        'a_point_glicko_vol',
        'a_game_glicko_rating',
        'a_game_glicko_rd',
        'a_game_glicko_vol',
        'a_set_glicko_rating',
        'a_set_glicko_rd',
        'a_set_glicko_vol',
        'a_service_game_glicko_rating',
        'a_service_game_glicko_rd',
        'a_service_game_glicko_vol',
        'a_return_game_glicko_rating',
        'a_return_game_glicko_rd',
        'a_return_game_glicko_vol',
        'a_tie_break_glicko_rating',
        'a_tie_break_glicko_rd',
        'a_tie_break_glicko_vol',
        'a_ace_glicko_rating',
        'a_ace_glicko_rd',
        'a_ace_glicko_vol',
        'a_return_ace_glicko_rating',
        'a_return_ace_glicko_rd',
        'a_return_ace_glicko_vol',
        'a_first_won_glicko_rating',
        'a_first_won_glicko_rd',
        'a_first_won_glicko_vol',
        'a_return_first_won_glicko_rating',
        'a_return_first_won_glicko_rd',
        'a_return_first_won_glicko_vol',
        'a_second_won_glicko_rating',
        'a_second_won_glicko_rd',
        'a_second_won_glicko_vol',
        'a_return_second_won_glicko_rating',
        'a_return_second_won_glicko_rd',
        'a_return_second_won_glicko_vol',

        'a_surface_glicko_rating',
        'a_surface_glicko_rd',
        'a_surface_glicko_vol',
        'a_surface_point_glicko_rating',
        'a_surface_point_glicko_rd',
        'a_surface_point_glicko_vol',
        'a_surface_game_glicko_rating',
        'a_surface_game_glicko_rd',
        'a_surface_game_glicko_vol',
        'a_surface_set_glicko_rating',
        'a_surface_set_glicko_rd',
        'a_surface_set_glicko_vol',
        'a_surface_service_game_glicko_rating',
        'a_surface_service_game_glicko_rd',
        'a_surface_service_game_glicko_vol',
        'a_surface_return_game_glicko_rating',
        'a_surface_return_game_glicko_rd',
        'a_surface_return_game_glicko_vol',
        'a_surface_tie_break_glicko_rating',
        'a_surface_tie_break_glicko_rd',
        'a_surface_tie_break_glicko_vol',
        'a_surface_ace_glicko_rating',
        'a_surface_ace_glicko_rd',
        'a_surface_ace_glicko_vol',
        'a_surface_return_ace_glicko_rating',
        'a_surface_return_ace_glicko_rd',
        'a_surface_return_ace_glicko_vol',
        'a_surface_first_won_glicko_rating',
        'a_surface_first_won_glicko_rd',
        'a_surface_first_won_glicko_vol',
        'a_surface_return_first_won_glicko_rating',
        'a_surface_return_first_won_glicko_rd',
        'a_surface_return_first_won_glicko_vol',
        'a_surface_second_won_glicko_rating',
        'a_surface_second_won_glicko_rd',
        'a_surface_second_won_glicko_vol',
        'a_surface_return_second_won_glicko_rating',
        'a_surface_return_second_won_glicko_rd',
        'a_surface_return_second_won_glicko_vol',

        'b_glicko_rating',
        'b_glicko_rd',
        'b_glicko_vol',
        'b_point_glicko_rating',
        'b_point_glicko_rd',
        'b_point_glicko_vol',
        'b_game_glicko_rating',
        'b_game_glicko_rd',
        'b_game_glicko_vol',
        'b_set_glicko_rating',
        'b_set_glicko_rd',
        'b_set_glicko_vol',
        'b_service_game_glicko_rating',
        'b_service_game_glicko_rd',
        'b_service_game_glicko_vol',
        'b_return_game_glicko_rating',
        'b_return_game_glicko_rd',
        'b_return_game_glicko_vol',
        'b_tie_break_glicko_rating',
        'b_tie_break_glicko_rd',
        'b_tie_break_glicko_vol',
        'b_ace_glicko_rating',
        'b_ace_glicko_rd',
        'b_ace_glicko_vol',
        'b_return_ace_glicko_rating',
        'b_return_ace_glicko_rd',
        'b_return_ace_glicko_vol',
        'b_first_won_glicko_rating',
        'b_first_won_glicko_rd',
        'b_first_won_glicko_vol',
        'b_return_first_won_glicko_rating',
        'b_return_first_won_glicko_rd',
        'b_return_first_won_glicko_vol',
        'b_second_won_glicko_rating',
        'b_second_won_glicko_rd',
        'b_second_won_glicko_vol',
        'b_return_second_won_glicko_rating',
        'b_return_second_won_glicko_rd',
        'b_return_second_won_glicko_vol',

        'b_surface_glicko_rating',
        'b_surface_glicko_rd',
        'b_surface_glicko_vol',
        'b_surface_point_glicko_rating',
        'b_surface_point_glicko_rd',
        'b_surface_point_glicko_vol',
        'b_surface_game_glicko_rating',
        'b_surface_game_glicko_rd',
        'b_surface_game_glicko_vol',
        'b_surface_set_glicko_rating',
        'b_surface_set_glicko_rd',
        'b_surface_set_glicko_vol',
        'b_surface_service_game_glicko_rating',
        'b_surface_service_game_glicko_rd',
        'b_surface_service_game_glicko_vol',
        'b_surface_return_game_glicko_rating',
        'b_surface_return_game_glicko_rd',
        'b_surface_return_game_glicko_vol',
        'b_surface_tie_break_glicko_rating',
        'b_surface_tie_break_glicko_rd',
        'b_surface_tie_break_glicko_vol',
        'b_surface_ace_glicko_rating',
        'b_surface_ace_glicko_rd',
        'b_surface_ace_glicko_vol',
        'b_surface_return_ace_glicko_rating',
        'b_surface_return_ace_glicko_rd',
        'b_surface_return_ace_glicko_vol',
        'b_surface_first_won_glicko_rating',
        'b_surface_first_won_glicko_rd',
        'b_surface_first_won_glicko_vol',
        'b_surface_return_first_won_glicko_rating',
        'b_surface_return_first_won_glicko_rd',
        'b_surface_return_first_won_glicko_vol',
        'b_surface_second_won_glicko_rating',
        'b_surface_second_won_glicko_rd',
        'b_surface_second_won_glicko_vol',
        'b_surface_return_second_won_glicko_rating',
        'b_surface_return_second_won_glicko_rd',
        'b_surface_return_second_won_glicko_vol',

        'a_b_win',
        'a_odds',
        'b_odds'
    ]

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

    new_format.to_csv('testout.csv', index=False)

    return new_format

async def create_new_game_df(game, players_glicko, player_surface_glickos):
    w_player = game['winner_name'] #Change to ID later
    l_player = game['loser_name'] #Change to ID later
    w_rank = game['winner_rank']
    l_rank = game['loser_rank']
    w_odds = game['winner_odds']
    l_odds = game['loser_odds']
    date = game['tourney_date']

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

    a_player_glickos = players_glicko[players_glicko['player'] == player_a]
    b_player_glickos = players_glicko[players_glicko['player'] == player_b]

    a_surface_glickos = player_surface_glickos[player_surface_glickos['player'] == player_a]
    b_surface_glickos = player_surface_glickos[player_surface_glickos['player'] == player_b]

    # Construct the game entry
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

        a_player_glickos['glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['point_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['point_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['point_glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['game_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['game_glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['set_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['set_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['set_glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['service_game_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['service_game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['service_game_glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['return_game_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['return_game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['return_game_glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['tie_break_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['tie_break_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['tie_break_glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['ace_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['ace_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['ace_glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['return_ace_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['return_ace_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['return_ace_glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['first_won_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['first_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['first_won_glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['return_first_won_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['return_first_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['return_first_won_glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['second_won_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['second_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['second_won_glicko_rating'].apply(lambda x: x.getVol()),
        a_player_glickos['return_second_won_glicko_rating'].apply(lambda x: x.getRating()),
        a_player_glickos['return_second_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_player_glickos['return_second_won_glicko_rating'].apply(lambda x: x.getVol()),

        a_surface_glickos['glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['point_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['point_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['point_glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['game_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['game_glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['set_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['set_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['set_glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['service_game_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['service_game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['service_game_glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['return_game_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['return_game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['return_game_glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['tie_break_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['tie_break_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['tie_break_glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['ace_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['ace_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['ace_glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['return_ace_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['return_ace_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['return_ace_glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['first_won_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['first_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['first_won_glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['return_first_won_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['return_first_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['return_first_won_glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['second_won_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['second_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['second_won_glicko_rating'].apply(lambda x: x.getVol()),
        a_surface_glickos['return_second_won_glicko_rating'].apply(lambda x: x.getRating()),
        a_surface_glickos['return_second_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        a_surface_glickos['return_second_won_glicko_rating'].apply(lambda x: x.getVol()),


        b_player_glickos['glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['point_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['point_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['point_glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['game_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['game_glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['set_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['set_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['set_glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['service_game_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['service_game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['service_game_glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['return_game_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['return_game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['return_game_glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['tie_break_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['tie_break_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['tie_break_glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['ace_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['ace_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['ace_glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['return_ace_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['return_ace_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['return_ace_glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['first_won_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['first_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['first_won_glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['return_first_won_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['return_first_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['return_first_won_glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['second_won_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['second_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['second_won_glicko_rating'].apply(lambda x: x.getVol()),
        b_player_glickos['return_second_won_glicko_rating'].apply(lambda x: x.getRating()),
        b_player_glickos['return_second_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_player_glickos['return_second_won_glicko_rating'].apply(lambda x: x.getVol()),

        b_surface_glickos['glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['point_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['point_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['point_glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['game_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['game_glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['set_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['set_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['set_glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['service_game_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['service_game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['service_game_glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['return_game_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['return_game_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['return_game_glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['tie_break_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['tie_break_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['tie_break_glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['ace_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['ace_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['ace_glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['return_ace_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['return_ace_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['return_ace_glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['first_won_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['first_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['first_won_glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['return_first_won_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['return_first_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['return_first_won_glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['second_won_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['second_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['second_won_glicko_rating'].apply(lambda x: x.getVol()),
        b_surface_glickos['return_second_won_glicko_rating'].apply(lambda x: x.getRating()),
        b_surface_glickos['return_second_won_glicko_rating'].apply(lambda x: x.get_pre_rating_rd(date)),
        b_surface_glickos['return_second_won_glicko_rating'].apply(lambda x: x.getVol()),

        a_b_win,
        player_a_odds,
        player_b_odds
    ]

    return game_entry
