import pandas as pd
from tqdm.asyncio import tqdm
import asyncio
from datetime import datetime, timedelta
from math import pow, copysign, floor, ceil
import traceback
import numpy as np
from render.utils.elov1.elo_functions import *

async def gather_elos(df: pd.DataFrame, enddate: datetime.date):
    w1_iloc = df.columns.get_loc('w1')

    # Basic filter conditions
    conditions = (
        ~pd.isnull(df.iloc[:, w1_iloc].values) 
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

    tasks = []
    pbar = tqdm(total=len(matches_df), desc="Processing Elos")

    async def update_elos_with_progress(players_elo, row):
        await update_elos(players_elo, row)
        pbar.update(1)

    for index, row in matches_df.iterrows():
        tasks.append(update_elos_with_progress(players_elo, row))
        
        # To avoid accumulating too many tasks, you can process them in smaller batches
        if len(tasks) >= 1000:
            await asyncio.gather(*tasks)
            tasks = []  # Reset the tasks list

    # Process any remaining tasks
    if tasks:
        await asyncio.gather(*tasks)

    pbar.close()

    players_elo = await filter_games(players_elo, enddate)
    return players_elo
