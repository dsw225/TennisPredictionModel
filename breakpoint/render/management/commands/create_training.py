import pandas as pd
from datetime import datetime
import asyncio
import os
import django
from django.db import models
import traceback
from asgiref.sync import sync_to_async # type: ignore
from render.utils.database_funcs import *
import render.utils.compute_games as compute_games 

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
            insert_db = MensTennisMatchStats
        else:
            type = WomensTennisMatch
            insert_db = WomensTennisMatchStats

        try:
            games = await self.get_games_in_span(start_date, end_date, type)
            all_games = await compute_games.prior_games(games, end_date)
            print(type(all_games))
            await self.add_data_to_model(all_games, insert_db)
            print(f"Successfully added model")
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
        
    async def add_data_to_model(self, df: pd.DataFrame, insert_db: models.Model):
        await reset_database(insert_db)

        # df['player_id'] = None
        # df['player_slug'] = None

        # Mapping DataFrame columns to model fields
        field_mapping = {
            'tourney_id' : 'tourney_id',
            'tourney_name' : 'tourney_name',
            'tourney_date' : 'tourney_date',
            'surface' : 'surface',
            'best_of' : 'best_of',
            'match_num' : 'match_num',
            'a_player_id' : 'a_player_id',
            'a_player_name' : 'a_player_name',
            'a_player_slug' : 'a_player_slug',
            'b_player_id' : 'b_player_id',
            'b_player_name' : 'b_player_name',
            'b_player_slug' : 'b_player_slug',
            'a_elo_rating' : 'a_elo_rating',
            'a_point_elo_rating' : 'a_point_elo_rating',
            'a_game_elo_rating' : 'a_game_elo_rating',
            'a_set_elo_rating' : 'a_set_elo_rating',
            'a_service_game_elo_rating' : 'a_service_game_elo_rating',
            'a_return_game_elo_rating' : 'a_return_game_elo_rating',
            'a_tie_break_elo_rating' : 'a_tie_break_elo_rating',
            'a_surface_elo_rating' : 'a_surface_elo_rating',
            'a_surface_point_elo_rating' : 'a_surface_point_elo_rating',
            'a_surface_game_elo_rating' : 'a_surface_game_elo_rating',
            'a_surface_set_elo_rating' : 'a_surface_set_elo_rating',
            'a_surface_service_game_elo_rating' : 'a_surface_service_game_elo_rating',
            'a_surface_return_game_elo_rating' : 'a_surface_return_game_elo_rating',
            'a_surface_tie_break_elo_rating' : 'a_surface_tie_break_elo_rating',
            'a_win_percent' : 'a_win_percent',
            'a_serve_rating' : 'a_serve_rating',
            'a_return_rating' : 'a_return_rating',
            'a_pressure_rating' : 'a_pressure_rating',
            'a_avg_vs_elo' : 'a_avg_vs_elo',
            'a_matches_played' : 'a_matches_played',
            'b_elo_rating' : 'b_elo_rating',
            'b_point_elo_rating' : 'b_point_elo_rating',
            'b_game_elo_rating' : 'b_game_elo_rating',
            'b_set_elo_rating' : 'b_set_elo_rating',
            'b_service_game_elo_rating' : 'b_service_game_elo_rating',
            'b_return_game_elo_rating' : 'b_return_game_elo_rating',
            'b_tie_break_elo_rating' : 'b_tie_break_elo_rating',
            'b_surface_elo_rating' : 'b_surface_elo_rating',
            'b_surface_point_elo_rating' : 'b_surface_point_elo_rating',
            'b_surface_game_elo_rating' : 'b_surface_game_elo_rating',
            'b_surface_set_elo_rating' : 'b_surface_set_elo_rating',
            'b_surface_service_game_elo_rating' : 'b_surface_service_game_elo_rating',
            'b_surface_return_game_elo_rating' : 'b_surface_return_game_elo_rating',
            'b_surface_tie_break_elo_rating' : 'b_surface_tie_break_elo_rating',
            'b_win_percent' : 'b_win_percent',
            'b_serve_rating' : 'b_serve_rating',
            'b_return_rating' : 'b_return_rating',
            'b_pressure_rating' : 'b_pressure_rating',
            'b_avg_vs_elo' : 'b_avg_vs_elo',
            'b_matches_played' : 'b_matches_played',
            'a_b_win' : 'a_b_win',
            'a_odds' : 'a_odds',
            'b_odds' : 'b_odds'
        }

        # Iterate over DataFrame rows and create model instances
        for i, row in df.iterrows():
            mapped_data = {model_field: row[df_column] for df_column, model_field in field_mapping.items() if df_column in row}
            mapped_data = {k: v if pd.notna(v) else None for k, v in mapped_data.items()}

            try:
                await insert_db.objects.acreate(**mapped_data)
            except Exception as e:
                print(f"Failed to add match {row['match_num']} on {row['tourney_date']} with exception: {e}")
