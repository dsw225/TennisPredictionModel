import pandas as pd
from datetime import datetime
import io
import aiofiles
import asyncio
import os
import django
from django_pandas.io import read_frame # type: ignore
import render.utils.type_elo as elos
import traceback
from asgiref.sync import sync_to_async # type: ignore

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'breakpoint.settings')
django.setup()

# Import Django models and necessary components
from django.core.management.base import BaseCommand, CommandError
from render.models import TennisMatch

class Command(BaseCommand):
    help = 'Import tennis match data asynchronously'

    def handle(self, *args, **kwargs):
        asyncio.run(self.handle_async(*args, **kwargs))

    async def handle_async(self, *args, **kwargs):
        start_date = datetime.strptime(kwargs['start_date'], '%Y%m%d').date()
        end_date = datetime.strptime(kwargs['end_date'], '%Y%m%d').date()
        match_type = kwargs['match_type']
        surface_char = kwargs['surface_char']

        surface_ratios = {
            'H': "Hard", 'C': "Clay", 'G': "Grass"
        }
        surface = surface_ratios.get(surface_char, None)

        try:
            games = await self.get_games_in_span(start_date, end_date, surface)
            player_elos = await elos.gather_elos(games)
            print(player_elos)
        except Exception as e:
            raise CommandError(f"Error during async operation: {e}")

    def add_arguments(self, parser):
        parser.add_argument('start_date', type=str, help='Start date in YYYYMMDD format')
        parser.add_argument('end_date', type=str, help='End date in YYYYMMDD format')
        parser.add_argument('match_type', type=str, help="Match type: 'm' for men's ATP matches, 'w' for women's WTA matches")
        parser.add_argument('surface_char', type=str, help="Surface type: H - Hard, C - Clay, G - Grass, Anything Else - All")

    async def get_games_in_span(self, start, end, surface):
        try:
            # https://stackoverflow.com/questions/62530017/django-3-1-async-views-working-with-querysets
            query = TennisMatch.objects.filter(
                tourney_date__range=(start, end)
            ).order_by('tourney_date')

            if surface is not None:
                query = query.filter(surface__iexact=surface)
            
            # Execute the query asynchronously
            games = await sync_to_async(list)(query.all().values())

            df = pd.DataFrame(games)

            return df
        except Exception as e:
            print(f"Exception while fetching games: {e}")
            traceback.print_exc()
            return []
