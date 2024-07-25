import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import asyncio
import re
from tqdm.asyncio import tqdm

def dates_within_a_month(date1, date2):
    if isinstance(date1, str):
        date1 = datetime.strptime(date1, '%Y-%m-%d')
    if isinstance(date2, str):
        date2 = datetime.strptime(date2, '%Y-%m-%d')
    
    if date1 > date2:
        date1, date2 = date2, date1
    
    month_later = date1 + timedelta(days=31)
    return date2 <= month_later

async def match_combiner(games: pd.DataFrame, odds: pd.DataFrame):
    games['tourney_date'] = pd.to_datetime(games['tourney_date'])
    odds['Date'] = pd.to_datetime(odds['Date'])
    delimiters = [" ", "\'", "\"", ",", "|", ";", "!"]

    games_values = games[['tourney_name', 'winner_name', 'loser_name', 'surface', 'tourney_date', 'winner_odds', 'loser_odds']].values

    # if odds[odds['Date'].dt.year]  >= 2010:
    odds_values = odds[['Tournament', 'Location', 'Winner', 'Loser', 'Surface', 'Date', 'AvgW', 'AvgL']].values

    async def update_field(row):
        tourney_name = re.split('|'.join(map(re.escape, delimiters)), row[0].lower())[0]
        winner_name = row[1].lower()
        loser_name = row[2].lower()
        surface = row[3]
        tourney_date = row[4]

        for odds_row in odds_values:
            if (
                (tourney_name in odds_row[0].lower() or tourney_name in odds_row[1].lower()) and
                (odds_row[2].split(' ')[0].lower() in winner_name) and
                (odds_row[3].split(' ')[0].lower() in loser_name) and
                (odds_row[4] == surface) and
                dates_within_a_month(tourney_date, odds_row[5])
            ):
                return [odds_row[5], odds_row[6], odds_row[7]]

        return [row[4], row[5], row[6]]

    tasks = [update_field(row) for row in games_values]
    results = await tqdm.gather(*tasks)
    
    games[['tourney_date', 'winner_odds', 'loser_odds']] = results #Update date too

    return games

