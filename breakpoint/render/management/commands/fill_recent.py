import datetime
import asyncio
import aiohttp
import aiofiles
import pandas as pd
import random
import unicodedata
import django
import os
import signal
from django.core.management.base import BaseCommand
from render.models import TennisMatch

base_url = "https://api.sofascore.com"
headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
pd_headers = ['tourney_id', 'tourney_name', 'surface', 'draw_size', 'tourney_level', 'tourney_date',
              'match_num', 'best_of', 'round', 'minutes', 'winner_id', 'winner_seed', 'winner_entry', 'winner_name', 'winner_hand',
              'winner_ht', 'winner_ioc', 'winner_age', 'loser_id', 'loser_seed', 'loser_entry', 'loser_name',
              'loser_hand', 'loser_ht', 'loser_ioc', 'loser_age', 'score', 'w1', 'w2', 'w3', 'w4', 'w5', 'w_ace',
              'w_df', 'w_svpt', 'w_1stIn', 'w_1stWon', 'w_2ndWon', 'w_SvGms', 'w_bpSaved', 'w_bpFaced', 'l1', 'l2', 'l3', 'l4', 'l5', 'l_ace', 'l_df',
              'l_svpt', 'l_1stIn', 'l_1stWon', 'l_2ndWon', 'l_SvGms', 'l_bpSaved', 'l_bpFaced', 'winner_rank', 'winner_rank_points', 'loser_rank', 'loser_rank_points']
matches_stored = []

# Global variable to control program exit
exit_program = False

# Setup Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'breakpoint.settings')
django.setup()

proxy_url = None
auth = None


class Command(BaseCommand):
    def handle(self, *args, **options):
        signal.signal(signal.SIGINT, handle_keyboard_interrupt)
        asyncio.run(main())

    def get_most_recent_entry(self):
        try:
            most_recent_entry = TennisMatch.objects.order_by('-tourney_date').first()
            if most_recent_entry:
                return most_recent_entry.date
            else:
                return None
        except Exception as e:
            print(f"Exception while fetching the most recent entry: {e}")
            return None

    async def read_proxies(self, file_path):
        try:
            async with aiofiles.open(file_path, 'r') as csvfile:
                proxies = [line.strip() for line in await csvfile.readlines()]
            proxy_parts = proxies[0].split(':')
            if len(proxy_parts) == 4:
                proxy_host, proxy_port, proxy_user, proxy_pass = proxy_parts
                proxy_url = f'http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}'
                auth = aiohttp.BasicAuth(proxy_user, proxy_pass)
            else:  # Proxy without authentication
                proxy_host, proxy_port = proxy_parts
                proxy_url = f'http://{proxy_host}:{proxy_port}'
                auth = None
            return proxy_url, auth
        except FileNotFoundError:
            print(f"Proxy file not found: {file_path}")
            return None, None

    async def fetch(self, session, url, proxy_url=None, auth=None):
        retries = 3  # Number of retries
        backoff_factor = 2  # Exponential backoff factor
        while retries > 0:
            try:
                if proxy_url:
                    print(f"Fetching URL: {url} with proxy: {proxy_url.split('@')[1]}")
                async with session.get(url, proxy=proxy_url, proxy_auth=auth, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    elif response.status == 404:
                        print("404 error: No statistics available for this match.")
                        return None
                    else:
                        print(f"Request failed with status: {response.status}. Retrying...")
            except aiohttp.ClientError as e:
                print(f"Client error: {e}. Retrying...")
            
            retries -= 1
            await asyncio.sleep(backoff_factor * (3 - retries))  # Exponential backoff
        
        return None

    async def get_year_to_date(self, mw):
        global most_recent_entry
        current_date = datetime.date.today()
        most_recent_entry = self.get_most_recent_entry()
        day_of_year = current_date.timetuple().tm_yday
        games_in_year = []
        try:
            if most_recent_entry:
                for day in range(day_of_year, (current_date - most_recent_entry).days, -1):
                    # Calculate the date for each day in reverse order
                    date = current_date - datetime.timedelta(days=day_of_year - day)
                    matches = await self.get_stats(mw, date)
                    games_in_year.extend(matches)
                    global exit_program
                    if exit_program:  # Check if keyboard interrupt thrown
                        raise Exception("Keyboard interruption")
                
                print("Completed Run Successfully")
        except Exception as e:
            print(f"Exception: {e}, saving data")
            pass

        all_games = pd.DataFrame(games_in_year, columns=pd_headers)
        all_games = all_games.sort_values(by='tourney_date').reset_index(drop=True)
        return all_games

    def save_data_to_csv(self, mw, data):
        prefix_mapping = {
            'm': 'atp_matches_', 'w': 'wta_matches_', 'mc': 'atp_matches_qual_chall_', 'wc': 'wta_matches_qual_chall_', 'mi': 'itf_men_matches_', 'wi': 'itf_women_matches_'
        }
        prefix = prefix_mapping.get(mw, '')
        filename = f'csvs/Generated/{prefix}{datetime.date.today().year}2.csv'
        data.to_csv(filename, index=False)
        print(f"Data saved to {filename}")

    async def get_stats(self, mw, date):
        prefix_mapping = {
            'm': '3', 'w': '6', 'mc': '72', 'wc': '871', 'mi': '785', 'wi': '213'
        }
        prefix = prefix_mapping.get(mw, '')
        date_stats = []
        date_str = date.strftime('%Y-%m-%d')
        url = f"/api/v1/category/{prefix}/scheduled-events/{date_str}" if prefix else f"/api/v1/sport/tennis/scheduled-events/{date_str}"

        connector = aiohttp.TCPConnector(use_dns_cache=False, force_close=True)
        async with aiohttp.ClientSession(trust_env=True, connector=connector) as session:
            json_data = await self.fetch(session, base_url + url, proxy_url, auth)
            if not json_data:
                print("No data returned.")
                return
            
            unsorted_matches = json_data.get('events', [])
            unsorted_matches = [match for match in unsorted_matches if not ('doubles' in match.get('tournament', {}).get('slug', ''))]  # Remove all doubles
            print(f"Found {len(unsorted_matches)} matches.")

            for match in unsorted_matches:
                match_id = match['id']
                match_stats_url = f"/api/v1/event/{match_id}/statistics"
                match_info_url = f"/api/v1/event/{match_id}"

                global exit_program
                if exit_program:  # Check if keyboard interrupt thrown
                    raise Exception("Keyboard interruption")
                
                if match_id in matches_stored:
                    print(f"Match ID {match_id} already stored")
                    continue

                matches_stored.append(match_id)

                match_stats = await self.fetch(session, base_url + match_stats_url, proxy_url, auth)
                if not match_stats:
                    print(f"No statistics found for match ID {match_id}. Skipping to next match.")
                    continue

                match_info = await self.fetch(session, base_url + match_info_url, proxy_url, auth)
                if not match_info:
                    print(f"Failed to fetch event info for match ID {match_id}.")
                    continue

                match_stats = await self.extract_match_stats(match_info, match_stats, date)
                date_stats.append(match_stats)

                print(f"Finished retrieval of match {match_id}")
                # await asyncio.sleep(random.randint(0, 4))  # Slow down repeated requests

        return date_stats

    async def extract_match_stats(self, match_info, match_stats, date):
        try:
            match_stats_all = match_stats["statistics"][0]["groups"]
            match_info_data = match_info["event"]
        except KeyError as e:
            print(f"Key error: {e}")
            return {}
        
        if match_info_data.get('status', {}).get('type', '') == 'canceled':
            print("Match was cancelled")
            return {}

        tourney_id = match_info_data.get('tournament', {}).get('id', '')
        tourney_name = match_info_data.get('tournament', {}).get('name', '')
        surface = match_info_data.get('tournament', {}).get('court', {}).get('surface', '')
        tourney_level = match_info_data.get('tournament', {}).get('category', {}).get('slug', '')
        winner_info = match_info_data.get('homeTeam', {}).get('name', 'NA')
        loser_info = match_info_data.get('awayTeam', {}).get('name', 'NA')
        score = match_info_data.get('finalResult', {}).get('display', '')

        try:
            winner_hand = match_stats_all[0]["statisticsItems"][0]["homeTeam"]
        except (KeyError, IndexError):
            winner_hand = "NA"
        
        match_stats = {
            "tourney_id": tourney_id,
            "tourney_name": tourney_name,
            "surface": surface,
            "draw_size": "NA",
            "tourney_level": tourney_level,
            "tourney_date": date,
            "match_num": "NA",
            "best_of": "NA",
            "round": "NA",
            "minutes": "NA",
            "winner_id": "NA",
            "winner_seed": "NA",
            "winner_entry": "NA",
            "winner_name": winner_info,
            "winner_hand": winner_hand,
            "winner_ht": "NA",
            "winner_ioc": "NA",
            "winner_age": "NA",
            "loser_id": "NA",
            "loser_seed": "NA",
            "loser_entry": "NA",
            "loser_name": loser_info,
            "loser_hand": "NA",
            "loser_ht": "NA",
            "loser_ioc": "NA",
            "loser_age": "NA",
            "score": score,
            "w1": "NA", "w2": "NA", "w3": "NA", "w4": "NA", "w5": "NA", "w_ace": "NA",
            "w_df": "NA", "w_svpt": "NA", "w_1stIn": "NA", "w_1stWon": "NA", "w_2ndWon": "NA", "w_SvGms": "NA",
            "w_bpSaved": "NA", "w_bpFaced": "NA", "l1": "NA", "l2": "NA", "l3": "NA", "l4": "NA", "l5": "NA",
            "l_ace": "NA", "l_df": "NA", "l_svpt": "NA", "l_1stIn": "NA", "l_1stWon": "NA", "l_2ndWon": "NA",
            "l_SvGms": "NA", "l_bpSaved": "NA", "l_bpFaced": "NA", "winner_rank": "NA", "winner_rank_points": "NA",
            "loser_rank": "NA", "loser_rank_points": "NA"
        }

        for stats_category in match_stats_all:
            category_name = stats_category["title"]
            for stat in stats_category["statisticsItems"]:
                title = stat["title"].lower().replace(' ', '_').replace('-', '_')
                match_stats[f"w_{title}"] = stat.get("homeTeam", "NA")
                match_stats[f"l_{title}"] = stat.get("awayTeam", "NA")

        return match_stats


def handle_keyboard_interrupt(signal_num, frame):
    global exit_program
    exit_program = True
    print("Keyboard Interrupt Received. Setting exit flag.")
