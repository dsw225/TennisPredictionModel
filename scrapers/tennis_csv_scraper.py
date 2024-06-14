import requests
from bs4 import BeautifulSoup
import json
import csv

URL = "https://www.pgatour.com/stats/detail/02675" #Total strokes gained from PGA
page = requests.get(URL)
soup = BeautifulSoup(page.content, "html.parser")
data = soup.find('script', id='__NEXT_DATA__') #Cause JS live
json_data = json.loads(data.string)
unsorted_total_sg = json_data['props']['pageProps']['statDetails']['rows']

player_stats = {}

with open('player_stats_avg.csv', 'w', newline='') as csvfile:
    fieldnames = ['Player', 'Avg SG:T', 'Avg SG:T2G', 'Avg SG:P', 'Measured Rounds']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()

    for player in unsorted_total_sg:
        player_name = player['playerName']
        player_stats[player_name] = {}
        for stat in player["stats"]:
            stat_name = stat["statName"]
            stat_value = stat["statValue"]
            player_stats[player_name][stat_name] = stat_value
        writer.writerow({'Player': player_name, 'Avg SG:T': player_stats[player_name]['Avg'], 'Avg SG:T2G': round(float(player_stats[player_name]['Total SG:T2G'])/float(player_stats[player_name]['Measured Rounds']), 3), 'Avg SG:P': round(float(player_stats[player_name]['Total SG:P'])/float(player_stats[player_name]['Measured Rounds']), 3), 'Measured Rounds': player_stats[player_name]['Measured Rounds']})