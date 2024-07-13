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
from asgiref.sync import sync_to_async

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'breakpoint.settings')

django.setup()

from render.models import TennisMatch

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
                # row[f'w{i+1}'] = score
                # row[f'l{i+1}'] = score

                row[f'w{i+1}'] = ''
                row[f'l{i+1}'] = ''
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
        input_path = 'data/csvs/ATP (Mens)/tennis_atp/atp_matches_'
    else:
        prefix = 'wta'
        input_path = 'data/csvs/WTA (Womens)/tennis_wta/wta_matches_'

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

    def clean_data(value):
        if isinstance(value, str) and value.strip() == '':
            return None
        try:
            return int(value)  # Convert to integer if possible
        except Exception:
            try:
                return float(value)  # Convert to float if possible
            except Exception:
                return value  # Return the original value if conversion fails

    # Assuming df is your DataFrame
    matches_df = matches_df.applymap(clean_data)


    field_mapping = {
        'tourney_id': 'tourney_id',
        'tourney_name': 'tourney_name',
        'surface': 'surface',
        'draw_size': 'draw_size',
        'tourney_level': 'tourney_level',
        'tourney_date': 'tourney_date',
        'match_num': 'match_num',
        'best_of': 'best_of',
        'round': 'round',
        'minutes': 'minutes',
        'winner_id': 'winner_id',
        'winner_seed': 'winner_seed',
        'winner_entry': 'winner_entry',
        'winner_name': 'winner_name',
        'winner_hand': 'winner_hand',
        'winner_ht': 'winner_ht',
        'winner_ioc': 'winner_ioc',
        'winner_age': 'winner_age',
        'loser_id': 'loser_id',
        'loser_seed': 'loser_seed',
        'loser_entry': 'loser_entry',
        'loser_name': 'loser_name',
        'loser_hand': 'loser_hand',
        'loser_ht': 'loser_ht',
        'loser_ioc': 'loser_ioc',
        'loser_age': 'loser_age',
        'w1': 'w1',
        'w2': 'w2',
        'w3': 'w3',
        'w4': 'w4',
        'w5': 'w5',
        'w_ace': 'w_ace',
        'w_df': 'w_df',
        'w_svpt': 'w_svpt',
        'w_1stIn': 'w_1stIn',
        'w_1stWon': 'w_1stWon',
        'w_2ndWon': 'w_2ndWon',
        'w_SvGms': 'w_SvGms',
        'w_bpSaved': 'w_bpSaved',
        'w_bpFaced': 'w_bpFaced',
        'l1': 'l1',
        'l2': 'l2',
        'l3': 'l3',
        'l4': 'l4',
        'l5': 'l5',
        'l_ace': 'l_ace',
        'l_df': 'l_df',
        'l_svpt': 'l_svpt',
        'l_1stIn': 'l_1stIn',
        'l_1stWon': 'l_1stWon',
        'l_2ndWon': 'l_2ndWon',
        'l_SvGms': 'l_SvGms',
        'l_bpSaved': 'l_bpSaved',
        'l_bpFaced': 'l_bpFaced',
        'winner_rank': 'winner_rank',
        'loser_rank': 'loser_rank'
    }
    
    # Define default values for extra fields
    default_values = {
        'winner_odds': None,
        'loser_odds': None
    }
    
    # Insert the DataFrame into the model
    await insert_dataframe_into_model(matches_df, field_mapping, default_values)


async def insert_dataframe_into_model(df: pd.DataFrame, field_mapping: dict, default_values: dict = None):
    if default_values is None:
        default_values = {}

    model_instances = []
    for i, row in df.iterrows():
        # Create a dictionary to hold the mapped field values
        mapped_data = {model_field: row[df_column] for df_column, model_field in field_mapping.items() if df_column in row}

        # Ensure all NaN values are set to None
        mapped_data = {k: v if not pd.isna(v) else None for k, v in mapped_data.items()}
        
        # Add default values for fields not in the DataFrame
        for field, value in default_values.items():
            if field not in mapped_data:
                mapped_data[field] = value
        
        # Create the model instance with the mapped data
        try:
            await TennisMatch.objects.acreate(**mapped_data)
        except Exception as e:
            print(f"Exception at game {mapped_data['tourney_date']} with exception {e}")
        print(f"{i} Matches added")
    
    # Bulk create model instances
    # TennisMatch.objects.acreate(model_instances)

asyncio.run(convert_format('19900101', '20221231', 'm'))
