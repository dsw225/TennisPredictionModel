import pandas as pd
from datetime import datetime
import io
import aiofiles
import asyncio
import os
import django
from django.db import models
import traceback
from asgiref.sync import sync_to_async # type: ignore
from render.utils.odds_game_combine import *
import warnings
from tqdm.asyncio import tqdm

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'breakpoint.settings')
django.setup()

# Import Django models and necessary components
from django.core.management.base import BaseCommand, CommandError
from render.models import *

class Command(BaseCommand):
    help = 'Import tennis match data asynchronously'

    def handle(self, *args, **kwargs):
        asyncio.run(self.handle_async(*args, **kwargs))

    async def handle_async(self, *args, **kwargs):
        start_year =kwargs['start_year']
        end_year = kwargs['end_year']
        match_type = kwargs['match_type']

        if match_type == 'm':
            path = 'data/csvs/ATP (Mens)/Odds'
            type = MensTennisMatch
            testout = "testoutM.csv"
        else:
            path = 'data/csvs/WTA (Womens)/Odds'
            type = WomensTennisMatch
            testout = "testoutW.csv"

        try:
            new_data = await self.add_odds_dates(path, start_year, end_year, type)
            new_data.to_csv(testout)
            print("Successfully added all models")
        except Exception as e:
            traceback.print_exc()
            raise CommandError(f"Error during async operation: {e}")

    def add_arguments(self, parser):
        parser.add_argument('start_year', type=int, help='Start date in YYYY format')
        parser.add_argument('end_year', type=int, help='End date in YYYY format')
        parser.add_argument('match_type', type=str, help="Match type: 'm' for men's ATP matches, 'w' for women's WTA matches")

    async def add_odds_dates(self, path, datestart, dateend, type):
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
                'winner_odds': 'winner_odds',
                'loser_odds': 'loser_odds',
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

        all_new_entries = []

        for yr in range(datestart, dateend + 1):
            # I hate this way but it works
            games = await self.get_games_in_span(yr, yr, type)
            
            file_path = f"{path}/{yr}.xls"
            match_df = await self.fetch_excel(file_path)

            # Process matches for the current year
            new_entries = await match_combiner(games, match_df)
            all_new_entries.append(new_entries)

            await self.insert_dataframe_into_model(new_entries, field_mapping, type)
    
        all_new_entries_df = pd.concat(all_new_entries).reset_index(drop=True)
        print("Entries added successfully")
        return all_new_entries_df

    # https://stackoverflow.com/questions/76391586/async-read-csv-in-pandas
    async def fetch_excel(self, path):
        try:
            async with aiofiles.open(path, 'rb') as f:  # Open file as binary
                print(f"{path}, being read + added")
                content = await f.read()
                return pd.read_excel(io.BytesIO(content), engine='xlrd', parse_dates=['Date'])  # Use xlrd for .xls files
        except Exception as e:
            # print(f"Exception: {e}")
            try:
                async with aiofiles.open(path + 'x', 'rb') as f:  # Attempt to open as .xlsx
                    print(f"{path+'x'}, being read + added")
                    content = await f.read()
                    # Suppress the specific warning from openpyxl
                    with warnings.catch_warnings(record=True):
                        warnings.simplefilter("ignore", UserWarning)
                        return pd.read_excel(io.BytesIO(content), engine='openpyxl', parse_dates=['Date'])  # Use openpyxl for .xlsx files
            except Exception as b:
                print(f"Exception: {b}")
                return

    async def get_games_in_span(self, start, end, type: models.Model):
        startdate = datetime(start, 1, 1)
        enddate = datetime(end, 12, 31)
        try:
            # https://stackoverflow.com/questions/62530017/django-3-1-async-views-working-with-querysets
            query = type.objects.filter(
                tourney_date__range=(startdate, enddate)
            ).order_by('tourney_date')
            
            # Execute the query asynchronously
            games = await sync_to_async(list)(query.all().values())

            df = pd.DataFrame(games)

            return df
        except Exception as e:
            print(f"Exception while fetching games: {e}")
            traceback.print_exc()
            return []
        
    async def insert_dataframe_into_model(self, df, field_mapping, type: models.Model, default_values=None):
        if default_values is None:
            default_values = {}

        def map_row_to_model(row, field_mapping):
            mapped_data = {model_field: row[df_column] for df_column, model_field in field_mapping.items() if df_column in row}
            mapped_data = {k: v if pd.notna(v) else None for k, v in mapped_data.items()}
            return mapped_data

        # Update or create records
        for index, row in df.iterrows():
            mapped_data = map_row_to_model(row, field_mapping)
            custom_id = row.get('id')
            try:
                await type.objects.aupdate_or_create(
                    id=custom_id,
                    defaults=mapped_data
                )
            except Exception as e:
                print(f"Failed to add match {i + 1}/{len(df)} at date {mapped_data['tourney_date']} with exception {e}")

