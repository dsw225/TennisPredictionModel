import pandas as pd
from datetime import datetime
import io
import aiofiles
import asyncio
import os
import django

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'breakpoint.settings')
django.setup()

# Import Django models and necessary components
from django.core.management.base import BaseCommand
from render.models import TennisMatch

class Command(BaseCommand):
    help = 'Import tennis match data asynchronously'

    async def fetch_csv(self, path):
        async with aiofiles.open(path, mode='r', encoding='utf-8') as f:
            text = await f.read()
            return pd.read_csv(io.StringIO(text), parse_dates=['tourney_date'])

    def filter_and_prepare_data(self, df, datestart, dateend):
        df = df[(df['tourney_date'] > datestart) & (df['tourney_date'] < dateend)]
        fill_values = {'best_of': 3}
        df = df.fillna(value=fill_values)
        df = df.fillna(-1)
        for col in df.select_dtypes(include='float').columns:
            df[col] = df[col].astype(int)
        df.replace(-1, '', inplace=True)
        return df

    def parse_score_to_columns(self, row):
        for i in range(1, 6):
            row[f'w{i}'] = ''
            row[f'l{i}'] = ''
        scores = row['score'].split()
        try:
            for i in range(0, len(scores)):
                score = scores[i].strip()
                if any(x in score for x in ['W', 'R', 'D', '[']):
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

    async def convert_format(self, start, end, mw):
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
            tasks.append(self.fetch_csv(file_path))  # Corrected to self.fetch_csv

        all_matches = await asyncio.gather(*tasks)
        matches_df = pd.concat(all_matches).sort_values(by='tourney_date').reset_index(drop=True)
        matches_df = self.filter_and_prepare_data(matches_df, datestart, dateend)
        matches_df = matches_df.apply(self.parse_score_to_columns, axis=1)

        # Clean data function
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

        matches_df = matches_df.applymap(clean_data)

        # Define field mapping and default values
        field_mapping = {
            'tourney_id': 'tourney_id',
            'tourney_name': 'tourney_name',
            'surface': 'surface',
            # Add other field mappings as needed
        }

        default_values = {
            'winner_odds': None,
            'loser_odds': None
        }

        # Insert data into model asynchronously
        await self.insert_dataframe_into_model(matches_df, field_mapping, default_values)

    async def insert_dataframe_into_model(self, df, field_mapping, default_values=None):
        if default_values is None:
            default_values = {}

        for index, row in df.iterrows():
            mapped_data = {model_field: row[df_column] for df_column, model_field in field_mapping.items() if df_column in row}
            mapped_data = {k: v if pd.notna(v) else None for k, v in mapped_data.items()}
            
            for field, value in default_values.items():
                if field not in mapped_data:
                    mapped_data[field] = value

            try:
                await TennisMatch.objects.create(**mapped_data)
                print(f"Successfully added match {index + 1}/{len(df)}")
            except Exception as e:
                print(f"Failed to add match {index + 1}/{len(df)}: {e}")

    def handle(self, *args, **kwargs):
        start_date = '19900101'
        end_date = '20221231'
        match_type = 'm'  # 'm' for men's ATP matches, 'w' for women's WTA matches

        asyncio.run(self.convert_format(start_date, end_date, match_type))
