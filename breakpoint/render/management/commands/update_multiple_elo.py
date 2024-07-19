import pandas as pd
from datetime import datetime
import asyncio
import os
import django
from django.db import models
import traceback
from asgiref.sync import sync_to_async # type: ignore
import render.utils.multiple_elo as elos
from render.utils.database_funcs import *

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
        start_date = datetime.strptime(kwargs['start_date'], '%Y%m%d').date()
        end_date = datetime.strptime(kwargs['end_date'], '%Y%m%d').date()
        match_type = kwargs['match_type']

        if match_type == 'm':   
            type = MensTennisMatch
            insert_db = MensFullEloStats
            grass_db = MensGrassEloStats
            clay_db = MensClayEloStats
            hard_db = MensHardEloStats
        else:
            type = WomensTennisMatch
            insert_db = WomensFullEloStats
            grass_db = WomensGrassEloStats
            clay_db = WomensClayEloStats
            hard_db = WomensHardEloStats

        try:
            games = await self.get_games_in_span(start_date, end_date, type)
            player_elos, grass_player_elos, clay_player_elos, hard_player_elos = await elos.gather_elos(games, end_date)
            await self.add_data_to_model(player_elos, insert_db)
            await self.add_data_to_model(grass_player_elos, grass_db)
            await self.add_data_to_model(clay_player_elos, clay_db)
            await self.add_data_to_model(hard_player_elos, hard_db)
            print("Successfully added all models")
        except Exception as e:
            traceback.print_exc()
            raise CommandError(f"Error during async operation: {e}")

    def add_arguments(self, parser):
        parser.add_argument('start_date', type=str, help='Start date in YYYYMMDD format')
        parser.add_argument('end_date', type=str, help='End date in YYYYMMDD format')
        parser.add_argument('match_type', type=str, help="Match type: 'm' for men's ATP matches, 'w' for women's WTA matches")

    async def get_games_in_span(self, start, end, type: models.Model):
        try:
            # https://stackoverflow.com/questions/62530017/django-3-1-async-views-working-with-querysets
            query = type.objects.filter(
                tourney_date__range=(start, end)
            ).order_by('tourney_date')
            
            # Execute the query asynchronously
            games = await sync_to_async(list)(query.all().values())

            df = pd.DataFrame(games)

            return df
        except Exception as e:
            print(f"Exception while fetching games: {e}")
            traceback.print_exc()
            return []
        
    async def add_data_to_model(self, df, insert_db: models.Model):
        await reset_database(insert_db)

        df['player_id'] = None
        df['player_slug'] = None

    # Mapping DataFrame columns to model fields
        field_mapping = {
            'player_id': 'player_id',
            'player': 'player_name',
            'player_slug': 'player_slug',
            'last_date': 'player_last_game',
            'match_number': 'player_match_number',
            'matches_played': 'player_matches_played',
            'elo_rating': 'elo_rating',
            'point_elo_rating': 'point_elo_rating',
            'game_elo_rating': 'game_elo_rating',
            'set_elo_rating': 'set_elo_rating',
            'service_game_elo_rating': 'service_game_elo_rating',
            'return_game_elo_rating': 'return_game_elo_rating',
            'tie_break_elo_rating': 'tie_break_elo_rating'
        }

        # Iterate over DataFrame rows and create model instances
        for i, row in df.iterrows():
            mapped_data = {model_field: row[df_column] for df_column, model_field in field_mapping.items() if df_column in row}
            mapped_data = {k: v if pd.notna(v) else None for k, v in mapped_data.items()}

            try:
                await insert_db.objects.acreate(**mapped_data)
            except Exception as e:
                print(f"Failed to add player {row['player']} with exception: {e}")
