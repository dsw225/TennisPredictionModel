import pandas as pd
from datetime import datetime
import re
import io
import aiofiles
import aiohttp
import asyncio
import os
import django
from django.conf import settings
from backend.breakpoint.render.models import TennisMatch

# https://stackoverflow.com/questions/76391586/async-read-csv-in-pandas
async def fetch_csv(path):
    async with aiofiles.open(path) as f:
        text = await f.read()
        with io.StringIO(text) as text_io:
            return pd.read_csv(text_io, parse_dates=['tourney_date'])


def filter_and_prepare_data(df, datestart, dateend):
    df = df[(df['tourney_date'] > datestart) & (df['tourney_date'] < dateend)]
    # df = df.drop_duplicates(subset=['match_num'])
    fill_values = {'best_of': 3}
    df = df.fillna(value=fill_values)
    df = df.fillna(-1)
    for col in df.select_dtypes(include='float').columns:
        df[col] = df[col].astype(int)
    df.replace(-1, '', inplace=True)
    return df


def parse_score_to_columns(row):
    for i in range(1, 6):
        row[f'w{i}'] = ''
        row[f'l{i}'] = ''
    scores = row['score'].split()
    try:
        for i in range(0, len(scores)):
            score = scores[i].strip()
            if any(x in score for x in ['W', 'R', 'D', '[']):
                row[f'w{i+1}'] = score
                row[f'l{i+1}'] = score
            elif '(' in score:
                winner_score, rest = score.split('-')
                loser_score = rest.split('(')[0]
                row[f'w{i+1}'] = int(winner_score)
                row[f'l{i+1}'] = int(loser_score)
            else:
                winner_score, loser_score = score.split('-')
                row[f'w{i+1}'] = int(winner_score)
                row[f'l{i+1}'] = int(loser_score)
    except Exception as e:
        print(f"Error processing {row['tourney_date']} with score {row['score']}: {e}")
    return row



async def convert_format(start, end, mw):
    datestart = datetime.strptime(start, "%Y%m%d")
    dateend = datetime.strptime(end, "%Y%m%d")

    if mw == 'm':   
        prefix = 'atp'
        input_path = 'csvs/ATP (Mens)/tennis_atp/atp_matches_'
    else:
        prefix = 'wta'
        input_path = 'csvs/WTA (Womens)/tennis_wta/wta_matches_'

    tasks = []
    for yr in range(datestart.year, dateend.year + 1):
        file_path = f"{input_path}{yr}.csv"
        print(f"{file_path}, being read")
        tasks.append(fetch_csv(file_path))

    all_matches = await asyncio.gather(*tasks)

    matches_df = pd.concat(all_matches).sort_values(by='tourney_date').reset_index(drop=True)
    matches_df = filter_and_prepare_data(matches_df, datestart, dateend)

    matches_df = matches_df.apply(parse_score_to_columns, axis=1)

    pd_headers = ['tourney_id', 'tourney_name', 'surface', 'draw_size', 'tourney_level', 'tourney_date',
                  'match_num', 'best_of', 'round', 'minutes', 'winner_id', 'winner_seed', 'winner_entry', 'winner_name', 'winner_hand',
                  'winner_ht', 'winner_ioc', 'winner_age', 'loser_id', 'loser_seed', 'loser_entry', 'loser_name',
                  'loser_hand', 'loser_ht', 'loser_ioc', 'loser_age', 'w1', 'w2', 'w3', 'w4', 'w5', 'w_ace',
                  'w_df', 'w_svpt', 'w_1stIn', 'w_1stWon', 'w_2ndWon', 'w_SvGms', 'w_bpSaved', 'w_bpFaced', 'l1', 'l2', 'l3', 'l4', 'l5', 'l_ace', 'l_df',
                  'l_svpt', 'l_1stIn', 'l_1stWon', 'l_2ndWon', 'l_SvGms', 'l_bpSaved', 'l_bpFaced', 'winner_rank', 'loser_rank']

    matches_df = matches_df[pd_headers]
    matches_df = matches_df.sort_values(by='tourney_date').reset_index(drop=True)

    for index, row in matches_df.iterrows():
        match_data = row.to_dict()
        match_data = {k: (v if pd.notna(v) else None) for k, v in match_data.items()}
        match_data = {k: (v if v != '' else None) for k, v in match_data.items()}
        
        TennisMatch.objects.update_or_create(
            match_num=match_data['match_num'],
            defaults=match_data
        )
    # output_path = f'csvs/Generated/{prefix}_matches_{datestart.year}_{dateend.year}.csv'

    # # print(matches_df)

    # csv_data = matches_df.to_csv(index=False)  # Convert DataFrame to CSV string
    # async with aiofiles.open(output_path, 'w') as f:
    #     await f.write(csv_data)

# Example usage:
asyncio.run(convert_format('19900101', '20231231', 'm'))
