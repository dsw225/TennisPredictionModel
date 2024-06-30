import json
from datetime import datetime, timedelta
import time
import random
import asyncio
from proxies.asyncproxies import read_proxies, get_new_conn, get_with_proxy

iproxy = 0
proxies = read_proxies("scrapers/proxies/selected_proxies.csv")
base_url = "http://api.sofascore.com"

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

    try:
        match_stats_all = match_stats_data["statistics"][0]["groups"]
    except KeyError as e:
        print(f"Key error: {e}")
        return {}

    game_level = {2000: 'G', 1000: 'M'}
    tourney_level = game_level.get(match["tournament"]["uniqueTournament"].get("tennisPoints", 0), 'A')

    elapsed_time = datetime.fromtimestamp(match["changes"]["changeTimestamp"]) - datetime.fromtimestamp(match["time"]["currentPeriodStartTimestamp"])
    minutes = int(elapsed_time.total_seconds() // 60)

    round_level = {29: 'F', 28: 'SF', 27: 'QF', 5: 'R16', 6: 'R32', 32: 'R64', 64: 'R128', 1: 'RR'}
    round = round_level.get(match["roundInfo"].get("round", 0), 'RR')

    player_a_wins = match["homeScore"]["current"]
    player_b_wins = match["awayScore"]["current"]

    best_of = 3 if player_a_wins == 2 or player_b_wins == 2 else 5 if player_a_wins == 3 or player_b_wins == 3 else 0

    stats = {
        "tourney_id": match["tournament"]["uniqueTournament"]["id"], "tourney_name": match["tournament"]["uniqueTournament"]["name"],"surface": match["groundType"],
        "draw_size" : 32,"tourney_level": tourney_level,"tourney_date": date.strftime('%Y%m%d'),"match_num": match["id"], "best_of": best_of, "round": round, "minutes": minutes,
        "winner_id": "","winner_seed": "","winner_entry": "","winner_name": "","winner_hand": "","winner_ht": "","winner_ioc": "","winner_age": "",
        "loser_id": "","loser_seed": "","loser_entry": "","loser_name": "","loser_hand": "","loser_ht": "","loser_ioc": "","loser_age": "",
        "w1": 0, "w2": 0, "w3": 0, "w4": 0, "w5": 0, "w_ace": 0, "w_df": 0, "w_svpt": 0, "w_1stIn": 0, "w_1stWon": 0, "w_2ndWon": 0, "w_SvGms": 0, "w_bpSaved": 0, "w_bpFaced": 0,
        "l1": 0, "l2": 0, "l3": 0, "l4": 0, "l5": 0, "l_ace": 0, "l_df": 0, "l_svpt": 0, "l_1stIn": 0, "l_1stWon": 0, "l_2ndWon": 0, "l_SvGms": 0, "l_bpSaved": 0, "l_bpFaced": 0,
        "winner_rank": 0, "loser_rank": 0
    }

    if match['winnerCode'] == 1:
        set_stats(stats, "w", "home", match["homeScore"], match_stats_all[0]["statisticsItems"])
        set_stats(stats, "l", "away", match["awayScore"], match_stats_all[0]["statisticsItems"])
        stats["winner_rank"], stats["loser_rank"] = match["homeTeam"].get("ranking", ''), match["awayTeam"].get("ranking", '')
        stats["winner_name"], stats["loser_name"] = match.get("homeTeam", {}).get("slug", "N/A"), match.get("awayTeam", {}).get("slug", "N/A")
    else:
        set_stats(stats, "w", "away", match["awayScore"], match_stats_all[0]["statisticsItems"])
        set_stats(stats, "l", "home", match["homeScore"], match_stats_all[0]["statisticsItems"])
        stats["winner_rank"], stats["loser_rank"] = match["awayTeam"].get("ranking", ''), match["homeTeam"].get("ranking", '')
        stats["winner_name"], stats["loser_name"] = match.get("awayTeam", {}).get("slug", "N/A"), match.get("homeTeam", {}).get("slug", "N/A")

    return stats

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
    await get_stats('m', datetime.now() - timedelta(days=10))

if __name__ == "__main__":
    asyncio.run(main())
