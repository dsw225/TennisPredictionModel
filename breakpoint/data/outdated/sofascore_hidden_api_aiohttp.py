import json
from datetime import datetime, timedelta
import time
import random
import asyncio
from proxies.asyncproxies import read_proxies, get_new_conn, get_with_proxy

iproxy = 0
base_url = "https://api.sofascore.com"

async def get_stats(mw, date):
    global conn
    global iproxy
    prefix_mapping = {
        'm': '3', 'w': '6', 'mc': '72', 'wc': '871', 'mi': '785', 'wi': '213'
    }
    prefix = prefix_mapping.get(mw, '')
    date_str = date.strftime('%Y-%m-%d')
    url = f"/api/v1/category/{prefix}/scheduled-events/{date_str}" if prefix else f"/api/v1/sport/tennis/scheduled-events/{date_str}"

    json_data, conn = await get_new_conn(base_url + url, iproxy, proxies)

    unsorted_matches = json_data.get('events', [])
    
    for match in unsorted_matches:
        home_team = match.get("homeTeam", {}).get("slug", "N/A")
        away_team = match.get("awayTeam", {}).get("slug", "N/A")
        print(f"Match: {home_team} vs. {away_team}")
        match_stats = await extract_match_stats(match, date)

        print("Extracted Stats:", match_stats)
        await asyncio.sleep(random.randint(0, 3))  # Slow down repeated
        # break  # Only process the first match for testing

async def extract_match_stats(match, date):
    global conn
    global iproxy
    match_id = match['id']
    match_stats_url = f"/api/v1/event/{match_id}/statistics"

    try:
        match_stats_data, conn = await get_with_proxy(base_url + match_stats_url, iproxy, conn, proxies)
    except Exception as e:
        match_stats_data, conn = await get_new_conn(base_url + match_stats_url, iproxy, proxies)

    match_info_url = f"/api/v1/event/{match_id}"

    # try:
    #     match_info_data, conn = await get_with_proxy(base_url + match_info_url, iproxy, conn, proxies)
    # except Exception as e:
    #     match_info_data, conn = await get_new_conn(base_url + match_info_url, iproxy, proxies)

    match_info_data = match

    try:
        match_stats_all = match_stats_data["statistics"][0]["groups"]
    except KeyError as e:
        print(f"Key error: {e}")
        return {}

    game_level = {2000: 'G', 1000: 'M'}
    tourney_level = game_level.get(match_info_data["tournament"]["uniqueTournament"].get("tennisPoints", 0), 'A')

    elapsed_time = datetime.fromtimestamp(match_info_data["changes"]["changeTimestamp"]) - datetime.fromtimestamp(match_info_data["time"]["currentPeriodStartTimestamp"])
    minutes = int(elapsed_time.total_seconds() // 60)

    round_level = {29: 'F', 28: 'SF', 27: 'QF', 5: 'R16', 6: 'R32', 32: 'R64', 64: 'R128', 1: 'RR'}
    round = round_level.get(match_info_data["roundInfo"].get("round", 0), 'RR')

    player_a_wins = match_info_data["homeScore"]["current"]
    player_b_wins = match_info_data["awayScore"]["current"]

    best_of = 3 if player_a_wins == 2 or player_b_wins == 2 else 5 if player_a_wins == 3 or player_b_wins == 3 else 0

    stats = initialize_stats(match_info_data, tourney_level, date, best_of, round, minutes)
    hand_table = {"right-handed": 'R', "left-handed": 'L'}
    
    if match_info_data['winnerCode'] == 1:
        set_stats(stats, "w", "home", match_info_data["homeScore"], match_stats_all[0]["statisticsItems"])
        set_stats(stats, "l", "away", match_info_data["awayScore"], match_stats_all[0]["statisticsItems"])
        set_player_stats(stats, "winner", match_info_data["homeTeam"], match_info_data, "homeTeamSeed", hand_table)
        set_player_stats(stats, "loser", match_info_data["awayTeam"], match_info_data, "awayTeamSeed", hand_table)
    else:
        set_stats(stats, "w", "away", match_info_data["awayScore"], match_stats_all[0]["statisticsItems"])
        set_stats(stats, "l", "home", match_info_data["homeScore"], match_stats_all[0]["statisticsItems"])
        set_player_stats(stats, "winner", match_info_data["awayTeam"], match_info_data, "awayTeamSeed", hand_table)
        set_player_stats(stats, "loser", match_info_data["homeTeam"], match_info_data, "homeTeamSeed", hand_table)

    return stats

def initialize_stats(match, tourney_level, date, best_of, round, minutes):
    return {
        "tourney_id": match["tournament"]["uniqueTournament"]["id"], 
        "tourney_name": match["tournament"]["uniqueTournament"]["name"],
        "surface": match["groundType"],
        "draw_size": 32,
        "tourney_level": tourney_level,
        "tourney_date": date.strftime('%Y%m%d'),
        "match_num": match["id"],
        "best_of": best_of,
        "round": round,
        "minutes": minutes,
        "winner_id": "", "winner_seed": "", "winner_entry": "", "winner_name": "",
        "winner_hand": "", "winner_ht": "", "winner_ioc": "", "winner_age": "",
        "loser_id": "", "loser_seed": "", "loser_entry": "", "loser_name": "",
        "loser_hand": "", "loser_ht": "", "loser_ioc": "", "loser_age": "",
        "w1": 0, "w2": 0, "w3": 0, "w4": 0, "w5": 0, "w_ace": 0, "w_df": 0, 
        "w_svpt": 0, "w_1stIn": 0, "w_1stWon": 0, "w_2ndWon": 0, "w_SvGms": 0, 
        "w_bpSaved": 0, "w_bpFaced": 0, "l1": 0, "l2": 0, "l3": 0, "l4": 0, 
        "l5": 0, "l_ace": 0, "l_df": 0, "l_svpt": 0, "l_1stIn": 0, "l_1stWon": 0, 
        "l_2ndWon": 0, "l_SvGms": 0, "l_bpSaved": 0, "l_bpFaced": 0,
        "winner_rank": 0, "loser_rank": 0
    }


def set_player_stats(stats, prefix, team, match, key, hand_table):
    stats[f"{prefix}_rank"] = team.get("ranking", '')
    stats[f"{prefix}_name"] = team.get("slug", "N/A")
    stats[f"{prefix}_id"] = team.get("id", "")
    stats[f"{prefix}_seed"] = match.get(key, '') if match.get(key, '') != 'Q' else ''
    stats[f"{prefix}_entry"] = match.get(key, '') if match.get(key, '') == 'Q' else ''
    stats[f"{prefix}_hand"] = hand_table.get(team["playerTeamInfo"].get("plays", ''), 'U')
    stats[f"{prefix}_ht"] = team["playerTeamInfo"].get("height", '') * 100 if team["playerTeamInfo"].get("height", '') != '' else ''
    stats[f"{prefix}_ioc"] = team.get("country", {}).get("alpha3", '')
    stats[f"{prefix}_age"] = (datetime.now() - datetime.fromtimestamp(team["playerTeamInfo"].get("birthDateTimestamp", ''))).days // 365 if team["playerTeamInfo"].get("birthDateTimestamp", '') != '' else ''

def set_stats(stats, prefix, home_away, scores, stats_items):
    stats[prefix + "1"] = scores.get("period1", 0)
    stats[prefix + "2"] = scores.get("period2", 0)
    stats[prefix + "3"] = scores.get("period3", 0)
    stats[prefix + "4"] = scores.get("period4", 0)
    stats[prefix + "5"] = scores.get("period5", 0)
    stats[prefix + "_ace"] = stats_items[0].get(home_away + "Value", 0)
    stats[prefix + "_df"] = stats_items[1].get(home_away + "Value", 0)
    stats[prefix + "_svpt"] = stats_items[2].get(home_away + "Total", 0)
    stats[prefix + "_1stIn"] = stats_items[2].get(home_away + "Value", 0)
    stats[prefix + "_1stWon"] = stats_items[4].get(home_away + "Value", 0)
    stats[prefix + "_2ndWon"] = stats_items[5].get(home_away + "Value", 0)
    stats[prefix + "_SvGms"] = stats_items[6].get(home_away + "Value", 0)
    stats[prefix + "_bpSaved"] = stats_items[7].get(home_away + "Value", 0)
    stats[prefix + "_bpFaced"] = stats_items[7].get(home_away + "Total", 0)


async def main():
    global proxies
    proxies = await read_proxies("scrapers/proxy_addresses/smartproxy.csv")
    await get_stats('m', datetime.now() - timedelta(days=0))

if __name__ == "__main__":
    asyncio.run(main())
