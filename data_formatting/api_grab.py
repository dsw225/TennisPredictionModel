import csv
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import http.client

url = "https://api.sportradar.com/tennis/trial/v3/en/rankings.json?api_key=uIr9UTeUVh2WwGEf3l0Bo92sMsMWgB5S6YAfC79R"

# url = f"https://api.sportradar.com/tennis/trial/v3/en/competitors/sr:competitor:94747/summaries.json?api_key=uIr9UTeUVh2WwGEf3l0Bo92sMsMWgB5S6YAfC79R"

api_key = "uIr9UTeUVh2WwGEf3l0Bo92sMsMWgB5S6YAfC79R"
headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)

json_data = json.loads(response.text)


# print(unsortedPlayers)

unsortedPlayers = json_data['rankings'][0]['competitor_rankings']

player_summaries = []
name_and_scores = []
for i in range(0, 5): # for i in range(0, len(unsortedPlayers)):
    url = f"https://api.sportradar.com/tennis/trial/v3/en/competitors/{str(unsortedPlayers[i]['competitor']['id'])}/summaries.json?api_key={api_key}"

    response = requests.get(url, headers=headers)

    json_data = json.loads(response.text)
    
    if 'summaries' in json_data:
        summaries = json_data['summaries'] # Safer for error

        losses = 0
        wins = 0
        games = 0
        for j in range(0, len(summaries)):
            games += 1
            if(summaries[j]['sport_event_status']['winner_id'] != unsortedPlayers[i]['competitor']['id']):
                losses += 1
            else:
                wins += 1
                
        name_and_scores.append(str(unsortedPlayers[i]['competitor']['name']) + ": " + str(float(wins)/games))
        player_summaries.append(summaries)
    else:
        print("No summaries found for player:", unsortedPlayers[i]['competitor']['name'])


    # player_summaries.append(json_data['summaries'])
    # print(str(unsortedPlayers[i]['rank']) + ": " + str(unsortedPlayers[i]['competitor']['name']) + " " + str(unsortedPlayers[i]['competitor']['id']) +  " Points: " + str(unsortedPlayers[i]['points']))

# player name, id, global rank, points, win/lossess, win rate, average sets, 
# service points won/lost - can be used for court speed

print(name_and_scores)


# odds, total win/losses, recent win rate, and average sets in wins/losses
# df = pd.DataFrame()
