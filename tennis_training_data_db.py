import csv
import pandas as pd

# class Player:
#     def __init__(self, player_id, name):
#         self.player_id = player_id
#         self.name = name
#         self.score = 0  # Default score, can be updated later if needed
    
#     def __repr__(self):
#         return f"Player ID: {self.player_id}, Name: {self.name}, Score: {self.score}"

player_data = {}

def add_player(player_id, data):
    player_data[player_id] = data

def get_player_data(player_id):
    return player_data.get(player_id)

def update_player_data(player_id, new_data):
    if player_id in player_data:
        add_player(player_id=player_id, data=new_data)
        # current_player = get_player_data(player_id=player_id)
        # #Add data to player BASE IDEA V ADD SURFACE
        # player_data[player_id] = current_player + new_data
    else:
        add_player(player_id=player_id, data=new_data)



main_data = "csvs/WTA (Womens)/tennis_wta/wta_matches_2022.csv"
# split_data = "csvs/WTA (Womens)/tennis_wta/wta_matches_2023.csv" # Will use for testing accuracy of model

data = pd.read_csv(main_data)

def score_to_num(score, winner_num): #winner num is 1 loser is 2
    entries = score.split()
    sum = 0

    for entry in entries:
        numbers = entry.split('-')
        
        if winner_num == 1:
            num_to_sum = int(numbers[0])  # sum the first number
        elif winner_num == 2:
            num_to_sum = int(numbers[1])  # sum the second number
        else:
            raise ValueError("which_number should be either 1 or 2")
        sum += num_to_sum
    return sum

for i in range(0, 3): #for i in range(0, len(data)):
    #Winning Player NO CALCULATIONS HERE
    winning_player = (data['winner_id'][i], {
        'name': data['winner_name'][i],
        'rank': data['winner_rank'][i],
        'rank_points': data['winner_rank_points'][i],
        'win': 1,
        'sets_won': score_to_num(data['score'][i], 1),
        'sets_lost': score_to_num(data['score'][i], 2),
        'aces': data['w_ace'][i],
        'd_faults': data['w_df'][i],
        'serve_points_played': data['w_svpt'][i],
        '1st_serves_in': data['w_1stIn'][i],
        '1st_serves_won': data['w_1stWon'][i],
        '2nd_serves_won': data['w_2ndWon'][i],
        'service_games_played': data['w_SvGms'][i],
        'break_points_saved': data['w_bpSaved'][i],
        'break_points_faced': data['w_bpFaced'][i]
    })
    # ADD SURFACE TYPE HERE MAYBE
    update_player_data(winning_player[0], winning_player[1])

    #Losing Player NO CALCULATIONS HERE
    losing_player = (data['loser_id'][i], {
        'name': data['loser_name'][i],
        'rank': data['loser_rank'][i],
        'rank_points': data['loser_rank_points'][i],
        'win': 0, # 1 for win 0 for loss
        'sets_won': score_to_num(data['score'][i], 2),
        'sets_lost': score_to_num(data['score'][i], 1),
        'aces': data['l_ace'][i],
        'd_faults': data['l_df'][i],
        'serve_points_played': data['l_svpt'][i],
        '1st_serves_in': data['l_1stIn'][i],
        '1st_serves_won': data['l_1stWon'][i],
        '2nd_serves_won': data['l_2ndWon'][i],
        'service_games_played': data['l_SvGms'][i],
        'break_points_saved': data['l_bpSaved'][i],
        'break_points_faced': data['l_bpFaced'][i],
    })
    update_player_data(losing_player[0], losing_player[1])

print(player_data)