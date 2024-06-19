import pandas as pd
from datetime import datetime

# Loading and preparing data
files = ["atp_matches_2021.csv", "atp_matches_2022.csv"]  # Example file names
matches_raw = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
matches = matches_raw[["winner_name", "loser_name", "tourney_level", "tourney_date", "match_num"]]
first_date = datetime.strptime("1900-01-01", "%Y-%m-%d")
matches["tourney_date"] = pd.to_datetime(matches["tourney_date"], format='%Y%m%d', origin='1900-01-01')
matches = matches.sort_values(by=["tourney_date", "match_num"]).reset_index(drop=True)

# Elo computation
players_to_elo = {}
matches_count = {}

def update_matches_count(player_a, player_b):
    matches_count[player_a] = matches_count.get(player_a, 0) + 1
    matches_count[player_b] = matches_count.get(player_b, 0) + 1

def update_elo(player_a, player_b, winner, level, match_date, match_num):
    rA = players_to_elo[player_a][-1]["ranking"] if player_a in players_to_elo else 1500
    rB = players_to_elo[player_b][-1]["ranking"] if player_b in players_to_elo else 1500
    
    eA = 1 / (1 + 10 ** ((rB - rA) / 400))
    eB = 1 / (1 + 10 ** ((rA - rB) / 400))
    
    if winner == player_a:
        sA, sB = 1, 0
    else:
        sA, sB = 0, 1
    
    kA = 250 / ((matches_count.get(player_a, 0) + 5) ** 0.4)
    kB = 250 / ((matches_count.get(player_b, 0) + 5) ** 0.4)
    k = 1.1 if level == "G" else 1
    
    rA_new = rA + (k * kA) * (sA - eA)
    rB_new = rB + (k * kB) * (sB - eB)
    
    if player_a not in players_to_elo:
        players_to_elo[player_a] = [{"ranking": 1500, "date": first_date, "num": 0}]
    if player_b not in players_to_elo:
        players_to_elo[player_b] = [{"ranking": 1500, "date": first_date, "num": 0}]
    
    players_to_elo[player_a].append({"ranking": rA_new, "date": match_date, "num": match_num})
    players_to_elo[player_b].append({"ranking": rB_new, "date": match_date, "num": match_num})

def compute_elo():
    for idx, row in matches.iterrows():
        update_matches_count(row["winner_name"], row["loser_name"])
        update_elo(row["winner_name"], row["loser_name"], row["winner_name"], row["tourney_level"], row["tourney_date"], row["match_num"])
    
    return players_to_elo

# Example peak performance analysis
def between_dates(date1, date2):
    players_to_max = {"ranking": 1500, "meanr": 1500, "medianr": 1500, "name": "Nobody"}
    for player, elo_data in players_to_elo.items():
        player_data = [entry for entry in elo_data if date1 <= entry["date"] <= date2]
        if player_data:
            max_ranking = max(player_data, key=lambda x: x["ranking"])["ranking"]
            mean_ranking = sum(entry["ranking"] for entry in player_data) / len(player_data)
            median_ranking = sorted(player_data, key=lambda x: x["ranking"])[len(player_data) // 2]["ranking"]
            players_to_max[player] = {"ranking": max_ranking, "meanr": mean_ranking, "medianr": median_ranking, "name": player}
    
    return players_to_max

# Example usage
players_to_elo = compute_elo()
print("Players to Elo Ratings:", players_to_elo)
