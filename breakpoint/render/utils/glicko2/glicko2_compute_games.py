import pandas as pd
from tqdm.asyncio import tqdm
import asyncio
from datetime import datetime, timedelta
from math import pow, copysign, floor, ceil
import random
import traceback
import numpy as np
from render.utils.glicko2.glicko2_updater import *
from render.utils.glicko2.glicko2_functions import *
# import arff

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

    new_header = ['player', 'recent_matches']

    matches_headers = matches_df.columns.tolist()

    # Define additional headers for recent matches
    additional_headers = [
        "oppo_recent_match_number",
        "oppo_recent_matches_played",
        'oppo_recent_glicko_rating',
        'oppo_recent_set_glicko_rating',
        'oppo_recent_game_glicko_rating',
        'oppo_recent_point_glicko_rating',
        'oppo_recent_tb_glicko_rating',
        'oppo_recent_service_glicko_rating',
        'oppo_recent_return_glicko_rating',
        "oppo_recent_ace_glicko_rating",
        "oppo_recent_return_ace_glicko_rating",
        "oppo_recent_first_won_glicko_rating",
        "oppo_recent_return_first_won_glicko_rating",
        "oppo_recent_second_won_glicko_rating",
        "oppo_recent_return_second_won_glicko_rating",

        "oppo_recent_surface_match_number",
        "oppo_recent_surface_matches_played",
        'oppo_recent_surface_glicko_rating',
        'oppo_recent_surface_set_glicko_rating',
        'oppo_recent_surface_game_glicko_rating',
        'oppo_recent_surface_point_glicko_rating',
        'oppo_recent_surface_tb_glicko_rating',
        'oppo_recent_surface_service_glicko_rating',
        'oppo_recent_surface_return_glicko_rating',
        "oppo_recent_surface_ace_glicko_rating",
        "oppo_recent_surface_return_ace_glicko_rating",
        "oppo_recent_surface_first_won_glicko_rating",
        "oppo_recent_surface_return_first_won_glicko_rating",
        "oppo_recent_surface_second_won_glicko_rating",
        "oppo_recent_surface_return_second_won_glicko_rating",
    ]

    # Combine matches headers and additional headers
    recent_matches_structure = matches_headers + additional_headers

    def create_recent_matches_df():
        return pd.DataFrame(columns=recent_matches_structure)

    data = {
        'player': players_to_glicko,   
        'recent_matches': [create_recent_matches_df() for _ in players_to_glicko]
    }

    players_glicko = pd.DataFrame(data, columns=new_header)

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

        'a_recent_glicko_rating',
        'a_recent_point_glicko_rating',
        'a_recent_game_glicko_rating',
        'a_recent_set_glicko_rating',
        'a_recent_service_game_glicko_rating',
        'a_recent_return_game_glicko_rating',
        'a_recent_tie_break_glicko_rating',
        'a_latest_ace_glicko_rating',
        'a_latest_return_ace_glicko_rating',
        'a_latest_first_won_glicko_rating',
        'a_latest_return_first_won_glicko_rating',
        'a_latest_second_won_glicko_rating',
        'a_latest_return_second_won_glicko_rating',
        'a_recent_surface_glicko_rating',
        'a_recent_surface_point_glicko_rating',
        'a_recent_surface_game_glicko_rating',
        'a_recent_surface_set_glicko_rating',
        'a_recent_surface_service_game_glicko_rating',
        'a_recent_surface_return_game_glicko_rating',
        'a_recent_surface_tie_break_glicko_rating',
        'a_latest_surface_ace_glicko_rating',
        'a_latest_surface_return_ace_glicko_rating',
        'a_latest_surface_first_won_glicko_rating',
        'a_latest_surface_return_first_won_glicko_rating',
        'a_latest_surface_second_won_glicko_rating',
        'a_latest_surface_return_second_won_glicko_rating',

        'b_latest_glicko_rating',
        'b_latest_point_glicko_rating',
        'b_latest_game_glicko_rating',
        'b_latest_set_glicko_rating',
        'b_latest_service_game_glicko_rating',
        'b_latest_return_game_glicko_rating',
        'b_latest_tie_break_glicko_rating',
        'b_latest_ace_glicko_rating',
        'b_latest_return_ace_glicko_rating',
        'b_latest_first_won_glicko_rating',
        'b_latest_return_first_won_glicko_rating',
        'b_latest_second_won_glicko_rating',
        'b_latest_return_second_won_glicko_rating',
        'b_latest_surface_glicko_rating',
        'b_latest_surface_point_glicko_rating',
        'b_latest_surface_game_glicko_rating',
        'b_latest_surface_set_glicko_rating',
        'b_latest_surface_service_game_glicko_rating',
        'b_latest_surface_return_game_glicko_rating',
        'b_latest_surface_tie_break_glicko_rating',
        'b_latest_surface_ace_glicko_rating',
        'b_latest_surface_return_ace_glicko_rating',
        'b_latest_surface_first_won_glicko_rating',
        'b_latest_surface_return_first_won_glicko_rating',
        'b_latest_surface_second_won_glicko_rating',
        'b_latest_surface_return_second_won_glicko_rating',

        'a_b_win',
        'a_odds',
        'b_odds'
    ]

    new_format = pd.DataFrame(np.nan, index=range(0, len(matches_df)), columns=game_header)

    tasks = []
    # For surfaces + total glicko
    pbar = tqdm(total=len(matches_df), desc="Processing glickos")


    async def update_games(new_format: pd.DataFrame, index, row, players_glicko):
        new_format.iloc[index] = pd.Series(await create_new_game_df(row, players_glicko))
        pbar.update(1)


    for index, row in matches_df.iterrows():
        tasks.append(update_games(new_format, index, row, players_glicko))
        # print(index)

        if len(tasks) >= 1000:
            await asyncio.gather(*tasks)
            tasks = []  # Reset the tasks list

    # Process any remaining tasks
    if tasks:
        await asyncio.gather(*tasks)

    pbar.close()

    new_format.to_csv('testoverunder1.csv', index=False)

    return new_format

async def get_recent_stats(players_glicko, row):
    player_a_idx = players_glicko[players_glicko['player'] == row["winner_name"]].index
    player_b_idx = players_glicko[players_glicko['player'] == row["loser_name"]].index

    if player_a_idx.empty or player_b_idx.empty:
        print(f"Player not found: {row['winner_name']} or {row['loser_name']}")
        return

    player_a_idx = player_a_idx[0]
    player_b_idx = player_b_idx[0]

    recent_matches_a = players_glicko.at[player_a_idx, 'recent_matches']
    recent_matches_b = players_glicko.at[player_b_idx, 'recent_matches']

    player_a_stats, player_b_stats, updated_recent_matches_a, updated_recent_matches_b = await parse_recent(recent_matches_a, recent_matches_b, row)

    players_glicko.at[player_a_idx, 'recent_matches'] = updated_recent_matches_a
    players_glicko.at[player_b_idx, 'recent_matches'] = updated_recent_matches_b

    return player_a_stats, player_b_stats

async def create_new_game_df(game, players_glicko):
    w_player = game['winner_name'] #Change to ID later
    l_player = game['loser_name'] #Change to ID later
    w_rank = game['winner_rank']
    l_rank = game['loser_rank']
    w_odds = game['winner_odds']
    l_odds = game['loser_odds']

    # _, _, w_sets, l_sets, _, _, _ = get_score_stats(game)
    # total_sets = w_sets+l_sets
    # a_b_win = -23
    # if total_sets > 2 and game['best_of'] == 3:
    #     a_b_win = 1
    # elif game['best_of'] == 3:
    #     a_b_win = 0



    player_w_stats, player_l_stats = await get_recent_stats(players_glicko, game)

    if random.choice([True, False]):
        player_a, player_b = w_player, l_player
        player_a_rank, player_b_rank = w_rank, l_rank
        player_a_odds, player_b_odds = w_odds, l_odds
        player_a_stats, player_b_stats = player_w_stats, player_l_stats
        a_b_win = 1
    else:
        player_a, player_b = l_player, w_player
        player_a_rank, player_b_rank = l_rank, w_rank
        player_a_odds, player_b_odds = l_odds, w_odds
        player_a_stats, player_b_stats = player_l_stats, player_w_stats
        a_b_win = 0

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

        glicko_win_probability(player_a_stats['recent_glicko_rating'], player_b_stats['recent_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_point_glicko_rating'], player_b_stats['recent_point_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_game_glicko_rating'], player_b_stats['recent_game_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_set_glicko_rating'], player_b_stats['recent_set_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_service_game_glicko_rating'], player_b_stats['recent_return_game_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_return_game_glicko_rating'], player_b_stats['recent_service_game_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_tie_break_glicko_rating'], player_b_stats['recent_tie_break_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_ace_glicko_rating'], player_b_stats['recent_return_ace_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_return_ace_glicko_rating'], player_b_stats['recent_ace_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_first_won_glicko_rating'], player_b_stats['recent_return_first_won_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_return_first_won_glicko_rating'], player_b_stats['recent_first_won_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_second_won_glicko_rating'], player_b_stats['recent_return_second_won_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_return_second_won_glicko_rating'], player_b_stats['recent_second_won_glicko_rating']),

        glicko_win_probability(player_a_stats['recent_surface_glicko_rating'], player_b_stats['recent_surface_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_point_glicko_rating'], player_b_stats['recent_surface_point_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_game_glicko_rating'], player_b_stats['recent_surface_game_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_set_glicko_rating'], player_b_stats['recent_surface_set_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_service_game_glicko_rating'], player_b_stats['recent_surface_return_game_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_return_game_glicko_rating'], player_b_stats['recent_surface_service_game_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_tie_break_glicko_rating'], player_b_stats['recent_surface_tie_break_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_ace_glicko_rating'], player_b_stats['recent_surface_return_ace_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_return_ace_glicko_rating'], player_b_stats['recent_surface_ace_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_first_won_glicko_rating'], player_b_stats['recent_surface_return_first_won_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_return_first_won_glicko_rating'], player_b_stats['recent_surface_first_won_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_second_won_glicko_rating'], player_b_stats['recent_surface_return_second_won_glicko_rating']),
        glicko_win_probability(player_a_stats['recent_surface_return_second_won_glicko_rating'], player_b_stats['recent_surface_second_won_glicko_rating']),

        glicko_win_probability(player_a_stats['latest_glicko_rating'], player_b_stats['latest_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_point_glicko_rating'], player_b_stats['latest_point_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_game_glicko_rating'], player_b_stats['latest_game_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_set_glicko_rating'], player_b_stats['latest_set_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_service_game_glicko_rating'], player_b_stats['latest_return_game_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_return_game_glicko_rating'], player_b_stats['latest_service_game_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_tie_break_glicko_rating'], player_b_stats['latest_tie_break_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_ace_glicko_rating'], player_b_stats['latest_return_ace_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_return_ace_glicko_rating'], player_b_stats['latest_ace_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_first_won_glicko_rating'], player_b_stats['latest_return_first_won_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_return_first_won_glicko_rating'], player_b_stats['latest_first_won_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_second_won_glicko_rating'], player_b_stats['latest_return_second_won_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_return_second_won_glicko_rating'], player_b_stats['latest_second_won_glicko_rating']),

        glicko_win_probability(player_a_stats['latest_surface_glicko_rating'], player_b_stats['latest_surface_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_point_glicko_rating'], player_b_stats['latest_surface_point_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_game_glicko_rating'], player_b_stats['latest_surface_game_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_set_glicko_rating'], player_b_stats['latest_surface_set_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_service_game_glicko_rating'], player_b_stats['latest_surface_return_game_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_return_game_glicko_rating'], player_b_stats['latest_surface_service_game_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_tie_break_glicko_rating'], player_b_stats['latest_surface_tie_break_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_ace_glicko_rating'], player_b_stats['latest_surface_return_ace_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_return_ace_glicko_rating'], player_b_stats['latest_surface_ace_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_first_won_glicko_rating'], player_b_stats['latest_surface_return_first_won_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_return_first_won_glicko_rating'], player_b_stats['latest_surface_first_won_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_second_won_glicko_rating'], player_b_stats['latest_surface_return_second_won_glicko_rating']),
        glicko_win_probability(player_a_stats['latest_surface_return_second_won_glicko_rating'], player_b_stats['latest_surface_second_won_glicko_rating']),

        a_b_win,
        player_a_odds,
        player_b_odds
    ]

    return game_entry

