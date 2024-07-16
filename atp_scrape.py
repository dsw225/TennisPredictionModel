import datetime
import asyncio
import aiohttp
import aiofiles
import pandas as pd
import unicodedata
import signal
import traceback

base_url = "http://www.atptour.com"
headers = {'User-Agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'}
pd_headers = ['tourney_id','tourney_name','surface','draw_size','tourney_level','tourney_date',
              'match_num','best_of','round','minutes','winner_id','winner_seed','winner_entry','winner_name','winner_hand',
              'winner_ht','winner_ioc','winner_age','loser_id','loser_seed','loser_entry','loser_name',
              'loser_hand','loser_ht','loser_ioc','loser_age','winner_odds','loser_odds','score','w1','w2','w3','w4','w5','w_ace',
              'w_df','w_svpt','w_1stIn','w_1stWon','w_2ndWon','w_SvGms','w_bpSaved','w_bpFaced','l1','l2','l3','l4','l5','l_ace','l_df',
              'l_svpt','l_1stIn','l_1stWon','l_2ndWon','l_SvGms','l_bpSaved','l_bpFaced','winner_rank','loser_rank']
matches_stored = []

# Global variable to control program exit
exit_program = False

async def read_proxies(file_path):
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

async def fetch(session, url, proxy_url=None, auth=None):
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

async def get_year_to_date(mw):
    current_date = datetime.date.today()
    day_of_year = current_date.timetuple().tm_yday
    games_in_year = []
    try:
        for day in range(1, day_of_year, 1):
            # Calculate the date for each day in reverse order
            date = current_date - datetime.timedelta(days=day_of_year - day)
            matches = await get_stats(mw, date)
            games_in_year.extend(matches)
            global exit_program
            if exit_program:  # Check if keyboard interrupt thrown
                raise Exception("Keyboard interruption")
            
        print("Completed Run Successfully")
    except Exception as e:
        print(f"Exception: {e}, saving data")
        traceback.print_exc()
        pass

    all_games = pd.DataFrame(games_in_year, columns=pd_headers)
    all_games = all_games.sort_values(by='tourney_date').reset_index(drop=True)
    return all_games

def save_data_to_csv(mw, data):
    prefix_mapping = {
        'm': 'atp_matches_', 'w': 'wta_matches_', 'mc': 'atp_matches_qual_chall_', 'wc': 'wta_matches_qual_chall_', 'mi': 'itf_men_matches_', 'wi': 'itf_women_matches_'
    }
    prefix = prefix_mapping.get(mw, '')
    filename = f'data/csvs/Generated/{prefix}{datetime.date.today().year}_Updated_NoUpload.csv'
    data.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

async def get_stats(mw, date):
    prefix_mapping = {
        'm': '3', 'w': '6', 'mc': '72', 'wc': '871', 'mi': '785', 'wi': '213'
    }
    prefix = prefix_mapping.get(mw, '')
    date_stats = []
    date_str = date.strftime('%Y-%m-%d')
    url = f"/api/v1/category/{prefix}/scheduled-events/{date_str}" if prefix else f"/api/v1/sport/tennis/scheduled-events/{date_str}"

    connector = aiohttp.TCPConnector(use_dns_cache=False, force_close=True)
    async with aiohttp.ClientSession(trust_env=True, connector=connector) as session:
        json_data = await fetch(session, base_url+url, proxy_url, auth)
        if not json_data:
            print("No data returned.")
            return
        
        unsorted_matches = json_data.get('events', [])
        unsorted_matches = [match for match in unsorted_matches if not ('doubles' in match.get('tournament', {}).get('uniqueTournament', {}).get('slug', ''))] # Remove all doubles
        print(f"Found {len(unsorted_matches)} matches.")

        for match in unsorted_matches:
            match_id = match['id']
            match_stats_url = f"/api/v1/event/{match_id}/statistics"
            match_info_url = f"/api/v1/event/{match_id}"
            match_odds_url = f"/api/v1/event/{match_id}/odds/1/all" # Has other odds included if wanted in future

            global exit_program
            if exit_program:  # Check if keyboard interrupt thrown
                raise Exception("Keyboard interruption")
            
            if(match_id in matches_stored):
                print(f"Match ID {match_id} already stored")
                continue

            matches_stored.append(match_id)

            match_stats = await fetch(session, base_url+match_stats_url, proxy_url, auth)
            if not match_stats:
                print(f"No statistics found for match ID {match_id}. Skipping to next match.")
                continue

            match_info = await fetch(session, base_url+match_info_url, proxy_url, auth)
            if not match_info:
                print(f"Failed to fetch event info for match ID {match_id}.")
                continue

            match_odds = await fetch(session, base_url+match_odds_url, proxy_url, auth)
            if not match_odds:
                print(f"Failed to fetch event odds for match ID {match_id}.")
                continue

            match_stats = await extract_match_stats(match_info, match_stats, match_odds, date)
            date_stats.append(match_stats)

            print(f"Finished retrieval of match {match_id}")
            # await asyncio.sleep(random.randint(0, 4))  # Slow down repeated requests

    return date_stats

async def extract_match_stats(match_info, match_stats, match_odds, date):
    try:
        match_stats_all = match_stats["statistics"][0]["groups"]
        match_info_data = match_info["event"]
    except KeyError as e:
        print(f"Key error: {e}")
        return {}
    
    if match_info_data.get('status', {}).get('type', '') == 'canceled':
        print(f"Canceled match {match_info_data.get('id', '')}")
        return {}

    # Tourney Level
    game_level = {2000: 'G', 1000: 'M'}
    tourney_level = game_level.get(match_info_data.get("tournament", {}).get("uniqueTournament", {}).get("tennisPoints", 0), 'A')

    # Length of Match
    totalSeconds = 0
    time_data = match_info_data.get("time", {})
    for key, value in time_data.items():
        if "period" in key:
            totalSeconds += int(value) 
    
    minutes = round(totalSeconds/60) if totalSeconds > 0 else 0

    # Match Odds
    match_odds = match_odds.get("markets", {})[0]
    odds = get_player_odds(match_odds.get("choices", {})) if match_odds.get("isLive", '') != True else (-1, -1)

    # Round of Tournament
    round_level = {29: 'F', 28: 'SF', 27: 'QF', 5: 'R16', 6: 'R32', 32: 'R64', 64: 'R128', 1: 'RR'}
    round_ = round_level.get(match_info_data.get("roundInfo", {}).get("round", 0), 'RR')

    player_a_wins = match_info_data.get("homeScore", {}).get("current", 0)
    player_b_wins = match_info_data.get("awayScore", {}).get("current", 0)

    best_of = 5 if player_a_wins == 3 or player_b_wins == 3 else 3 if player_a_wins == 2 or player_b_wins == 2 else ''

    stats = initialize_stats(match_info_data, tourney_level, date, best_of, round_, minutes, odds)
    hand_table = {"right-handed": 'R', "left-handed": 'L'}
    
    if match_info_data.get('winnerCode') == 1:
        set_stats(stats, "w", "home", match_info_data.get("homeScore", {}), match_stats_all[0].get("statisticsItems", {}))
        set_stats(stats, "l", "away", match_info_data.get("awayScore", {}), match_stats_all[0].get("statisticsItems", {}))
        set_player_stats(stats, "winner", match_info_data.get("homeTeam", {}), match_info_data, "homeTeamSeed", hand_table)
        set_player_stats(stats, "loser", match_info_data.get("awayTeam", {}), match_info_data, "awayTeamSeed", hand_table)
    else:
        set_stats(stats, "w", "away", match_info_data.get("awayScore", {}), match_stats_all[0].get("statisticsItems", {}))
        set_stats(stats, "l", "home", match_info_data.get("homeScore", {}), match_stats_all[0].get("statisticsItems", {}))
        set_player_stats(stats, "winner", match_info_data.get("awayTeam", {}), match_info_data, "awayTeamSeed", hand_table)
        set_player_stats(stats, "loser", match_info_data.get("homeTeam", {}), match_info_data, "homeTeamSeed", hand_table)

    return stats

def get_player_odds(match_odds):
    winner_odds = -1 
    loser_odds = -1
    if match_odds[0].get("winning", 0) == True:
        winner_odds = match_odds[0].get("fractionalValue", '').split('/')
        loser_odds = match_odds[1].get("fractionalValue", '').split('/')
    else:
        loser_odds = match_odds[0].get("fractionalValue", '').split('/')
        winner_odds = match_odds[1].get("fractionalValue", '').split('/')

    computedWinner = (float(winner_odds[0]) / float(winner_odds[1])) + 1 if winner_odds != -1 and winner_odds[1] != '' and winner_odds[1] != 0 else -1
    computedLoser = (float(loser_odds[0]) / float(loser_odds[1])) + 1 if loser_odds != -1 and loser_odds[1] != '' and loser_odds[1] != 0 else -1

    return round(computedWinner, 2), round(computedLoser, 2)

def initialize_stats(match, tourney_level, date, best_of, round_, minutes, odds):
    return {
        "tourney_id": match.get("tournament", {}).get("uniqueTournament", {}).get("id", 0),
        "tourney_name": match.get("tournament", {}).get("uniqueTournament", {}).get("name", "N/A"),
        "surface": match.get("groundType", "Unknown"),
        "draw_size": "",
        "tourney_level": tourney_level,
        "tourney_date": datetime.datetime.fromtimestamp(match.get("startTimestamp", 0)).date().strftime('%Y%m%d') if match.get("startTimestamp", 0) != 0 else date.strftime('%Y%m%d'),
        "match_num": match.get("id", 0),
        "best_of": best_of,
        "round": round_,
        "minutes": minutes,
        "winner_id": "", "winner_seed": "", "winner_entry": "", "winner_name": "",
        "winner_hand": "", "winner_ht": "", "winner_ioc": "", "winner_age": "",
        "loser_id": "", "loser_seed": "", "loser_entry": "", "loser_name": "",
        "loser_hand": "", "loser_ht": "", "loser_ioc": "", "loser_age": "",
        "winner_odds": odds[0], "loser_odds": odds[1],
        "w1": 0, "w2": 0, "w3": 0, "w4": 0, "w5": 0, "w_ace": 0, "w_df": 0, 
        "w_svpt": 0, "w_1stIn": 0, "w_1stWon": 0, "w_2ndWon": 0, "w_SvGms": 0, 
        "w_bpSaved": 0, "w_bpFaced": 0, "l1": 0, "l2": 0, "l3": 0, "l4": 0, 
        "l5": 0, "l_ace": 0, "l_df": 0, "l_svpt": 0, "l_1stIn": 0, "l_1stWon": 0, 
        "l_2ndWon": 0, "l_SvGms": 0, "l_bpSaved": 0, "l_bpFaced": 0,
        "winner_rank": 0, "loser_rank": 0
    }

def set_player_stats(stats, prefix, team, match, key, hand_table):
    stats[f"{prefix}_rank"] = team.get("ranking", '')
    stats[f"{prefix}_name"] = get_full_name(team.get("name", "N/A"), team.get("slug", "N/A"))
    stats[f"{prefix}_id"] = team.get("id", "")
    stats[f"{prefix}_seed"] = match.get(key, '') if match.get(key, '').isdigit() else ''
    stats[f"{prefix}_entry"] = match.get(key, '') if not match.get(key, '').isdigit() else ''
    stats[f"{prefix}_hand"] = hand_table.get(team.get("playerTeamInfo", {}).get("plays", ''), 'U')
    stats[f"{prefix}_ht"] = team.get("playerTeamInfo", {}).get("height", '') * 100 if team.get("playerTeamInfo", {}).get("height", '') != '' else ''
    stats[f"{prefix}_ioc"] = team.get("country", {}).get("alpha3", '')
    stats[f"{prefix}_age"] = (datetime.datetime.now() - datetime.datetime.fromtimestamp(team.get("playerTeamInfo", {}).get("birthDateTimestamp", ''))).days // 365 if team.get("playerTeamInfo", {}).get("birthDateTimestamp", '') != '' else ''

def set_stats(stats, prefix, home_away, scores, stats_items):
    stats[prefix + "1"] = scores.get("period1", 0)
    stats[prefix + "2"] = scores.get("period2", 0)
    stats[prefix + "3"] = scores.get("period3", 0)
    stats[prefix + "4"] = scores.get("period4", 0)
    stats[prefix + "5"] = scores.get("period5", 0)
    stats[prefix + "_ace"] = stats_items[0].get(home_away + "Value", 0) if len(stats_items) >= 1 else ''
    stats[prefix + "_df"] = stats_items[1].get(home_away + "Value", 0) if len(stats_items) >= 2 else ''
    stats[prefix + "_svpt"] = stats_items[2].get(home_away + "Total", 0) if len(stats_items) >= 3 else ''
    stats[prefix + "_1stIn"] = stats_items[2].get(home_away + "Value", 0) if len(stats_items) >= 3 else ''
    stats[prefix + "_1stWon"] = stats_items[4].get(home_away + "Value", 0) if len(stats_items) >= 5 else ''
    stats[prefix + "_2ndWon"] = stats_items[5].get(home_away + "Value", 0) if len(stats_items) >= 6 else ''
    stats[prefix + "_SvGms"] = stats_items[6].get(home_away + "Value", 0) if len(stats_items) >= 7 else ''
    stats[prefix + "_bpSaved"] = stats_items[7].get(home_away + "Value", 0) if len(stats_items) >= 8 else ''
    stats[prefix + "_bpFaced"] = stats_items[7].get(home_away + "Total", 0) if len(stats_items) >= 8 else ''

def get_full_name(name, slug):
    if slug == "N/A" or name == "N/A":
        return slug

    # Replace special characters in slug with regular characters
    name = name.replace('-', ' ')
    name = ''.join((c for c in unicodedata.normalize('NFD', name) if unicodedata.category(c) != 'Mn'))

    slug_parts = slug.split('-')
    capitalized_slug = ' '.join([part.capitalize() for part in slug_parts])

    name_parts = name.replace('\'', '').split(' ')
    capitalized_name_parts = [part.capitalize() for part in name_parts]
    last_name = ' '.join(capitalized_name_parts[:-1])

    temp_first_name = capitalized_slug.replace(last_name, '')
    first_name = temp_first_name.lstrip()
    last_name = last_name.lstrip()
    # print(f"First Name: {first_name}    Last Name: {last_name}")

    #Just in case
    last_name = last_name.replace('-', ' ')
    first_name = first_name.replace('-', ' ')

    return f"{first_name} {last_name}"

async def main():
    global proxy_url
    global auth
    proxy_url, auth = await read_proxies("data/scrapers/proxy_addresses/smartproxy.csv")

    mw = 'm'

    games_data = await get_year_to_date(mw)
    save_data_to_csv(mw, games_data)

def handle_keyboard_interrupt(signal, frame):
    global exit_program
    exit_program = True

if __name__ == "__main__":
    signal.signal(signal.SIGINT, handle_keyboard_interrupt)
    asyncio.run(main())