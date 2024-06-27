import requests
import json
import os
import time
import csv
from datetime import datetime
from difflib import SequenceMatcher

OUTPUT_DIR = "C:/Temp/Tennis_CSV_Output"
SOFASCORE_URL = "https://api.sofascore.com/api/v1/sport/tennis/events/live"
TEMP_FILE = "C:/Temp/Sofascore_Tennis_Build.csv"
FINAL_FILE = "C:/Temp/Sofascore_Tennis_Final.csv"

market_list = []
update_count = 0

def fetch_data(url):
    response = requests.get(url)
    return response.json()

def string_fuzzy_compare(str1, str2):
    return SequenceMatcher(None, str1, str2).ratio() * 100

def return_teams(home_team, away_team):
    return list(set(home_team.split() + away_team.split()))

def get_stats(event_id):
    url = f"https://api.sofascore.com/api/v1/event/{event_id}/statistics"
    data = fetch_data(url)
    if data:
        home_stats = data['statistics'][0]['groups'][0]['statisticsItems'][2]['home']
        away_stats = data['statistics'][0]['groups'][0]['statisticsItems'][2]['away']
        return home_stats, away_stats
    return None, None

def main():
    global update_count
    while True:
        update_count += 1
        file_list = [f for f in os.listdir(OUTPUT_DIR) if f.startswith("Output_") and f.endswith(".csv")]

        if file_list:
            for file_name in file_list:
                market_name = file_name.replace("Output_", "").replace(".csv", "")
                if market_name not in market_list:
                    market_list.append([market_name, ""])
                os.remove(os.path.join(OUTPUT_DIR, file_name))

            live_events = fetch_data(SOFASCORE_URL)['events']

            for market in market_list:
                if market[1] == "":
                    final_array = []
                    for i, event in enumerate(live_events):
                        home_team = event['homeTeam']['name']
                        away_team = event['awayTeam']['name']
                        split_teams = return_teams(home_team, away_team)
                        matched = sum([1 for team in split_teams if team in market[0]])

                        final_array.append([market[0], f"{home_team} v {away_team}", i, "", matched])

                    if final_array:
                        for item in final_array:
                            fuzzy_result = string_fuzzy_compare(item[0].replace(" - Match Odds", ""), item[1])
                            item[3] = fuzzy_result

                        final_array.sort(key=lambda x: (x[4], x[3]))
                        if final_array[0][4] > 0 and final_array[0][3] < 20:
                            market[1] = live_events[final_array[0][2]]['id']

        if market_list:
            market_list = [m for m in market_list if m[1] != ""]
            with open(TEMP_FILE, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["1"])

                for market in market_list:
                    event_id = market[1]
                    event_data = fetch_data(f"https://api.sofascore.com/api/v1/event/{event_id}")
                    if event_data['event']['status']['description'] != "Ended":
                        set_no = next((i for i in range(1, 6) if f"{i}st" in event_data['event']['status']['description']), 1)
                        home_build_string = f"{market[0]},*,*,1,E,SetNo,{set_no},"
                        away_build_string = f"{market[0]},*,*,2,"

                        home_stats, away_stats = get_stats(event_id)
                        if home_stats and away_stats:
                            home_build_string += f"S,1stServeIn%,{home_stats},S,1stServePointsWon%,{home_stats},S,2ndServeIn%,{home_stats},S,2ndServePointsWon%,{home_stats},S,OppReceivePointsWon,{home_stats},S,BPAgainst,{home_stats},S,BPSaved%,{home_stats}"
                            away_build_string += f"S,1stServeIn%,{away_stats},S,1stServePointsWon%,{away_stats},S,2ndServeIn%,{away_stats},S,2ndServePointsWon%,{away_stats},S,OppReceivePointsWon,{away_stats},S,BPAgainst,{away_stats},S,BPSaved%,{away_stats}"
                        
                        writer.writerow([home_build_string])
                        writer.writerow([away_build_string])
                        print(f"{datetime.now().time()} {market[0]}")
                    else:
                        market_list.remove(market)
            
            os.replace(TEMP_FILE, FINAL_FILE)

        time.sleep(15)

if __name__ == "__main__":
    main()
