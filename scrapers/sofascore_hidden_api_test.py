import http.client
import json
from datetime import datetime, timedelta

conn = http.client.HTTPSConnection("api.sofascore.com")

## 3: ATP Games 6: WTA Games 72: Challenger Mens 871: Challenger Womens 785: ITF Mens 213: ITF Women
def get_stats(mw, date):
    if mw == 'm':
        prefix = '3'
    elif mw == 'w':
        prefix = '6'
    elif mw == 'mc':
        prefix = '72'
    elif mw == 'wc':
        prefix = '871'
    elif mw == 'mi':
        prefix = '785'
    elif mw == 'wi':
        prefix = '213'
    else:
        prefix = ''
    
    url = f"/api/v1/category/{prefix}/scheduled-events/{date.strftime('%Y-%m-%d')}" if prefix != '' else f"api.sofascore.com/api/v1/sport/tennis/scheduled-events/{date.strftime('%Y-%m-%d')}"

    payload = ''
    headers = {}

    conn.request("GET", url, payload, headers)
    res = conn.getresponse()
    data = res.read()

    json_data = json.loads(data.decode("utf-8"))
    unsorted_matches = json_data['events']
    
    for match in unsorted_matches:
        home_team = match.get("homeTeam", {}).get("slug", "N/A")
        away_team = match.get("awayTeam", {}).get("slug", "N/A")
        print(f"Match: {home_team} vs. {away_team}")
        match_stats = extract_stats(match)
        print("Extracted Stats:", match_stats)
        break  # Only process the first match for testing

def extract_stats(match):
    match_id = match['id']
    match_stats_url = f"api/v1/event/{match_id}/statistics"
    
    payload = ''
    headers = {}

    conn.request("GET", match_stats_url, payload, headers)
    res = conn.getresponse()
    data = res.read()

    match_stats_data = json.loads(data.decode("utf-8"))

    # Handling potential key errors and accessing nested data safely
    try:
        match_stats_all = match_stats_data["statistics"][0]["groups"]
    except KeyError as e:
        print(f"Key error: {e}")
        return {}

    elapsed_time = datetime.fromtimestamp(match["changes"]["changeTimestamp"]) - datetime.fromtimestamp(match["time"]["currentPeriodStartTimestamp"])
    minutes = int(elapsed_time.total_seconds() // 60)
    print(f"Minutes: {minutes}")
    round = match["roundInfo"]["slug"]
    best_of = 3 #match["defaultPeriodCount"] temp fix

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

    stats = {
        "w1": 0, "w2": 0, "w3": 0, "w4": 0, "w5": 0, "w_ace": 0, "w_df": 0, "w_svpt": 0, "w_1stIn": 0, "w_1stWon": 0, "w_2ndWon": 0, "w_SvGms": 0, "w_bpSaved": 0, "w_bpFaced": 0,
        "l1": 0, "l2": 0, "l3": 0, "l4": 0, "l5": 0, "l_ace": 0, "l_df": 0, "l_svpt": 0, "l_1stIn": 0, "l_1stWon": 0, "l_2ndWon": 0, "l_SvGms": 0, "l_bpSaved": 0, "l_bpFaced": 0,
        "winner_rank": 0, "loser_rank": 0
    }

    if match['winnerCode'] == 1:
        set_stats(stats, "w", "home", match["homeScore"], match_stats_all[0]["statisticsItems"])
        set_stats(stats, "l", "away", match["awayScore"], match_stats_all[0]["statisticsItems"])
        stats["winner_rank"], stats["loser_rank"] = match["homeTeam"]["ranking"], match["awayTeam"]["ranking"]
    else:
        set_stats(stats, "w", "away", match["awayScore"], match_stats_all[0]["statisticsItems"])
        set_stats(stats, "l", "home", match["homeScore"], match_stats_all[0]["statisticsItems"])
        stats["winner_rank"], stats["loser_rank"] = match["awayTeam"]["ranking"], match["homeTeam"]["ranking"]

    return stats

get_stats('m', datetime.now() - timedelta(days=5))
