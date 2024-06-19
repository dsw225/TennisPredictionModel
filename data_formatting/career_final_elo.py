import pandas as pd
from datetime import datetime

def career_stats(date, mw):
    dateend = datetime.strptime(date, "%Y%m%d")
    datestart = datetime(1968, 1, 1)
    if mw == 'm':   
        prefix = 'atp'
        input_path = 'csvs/ATP (Mens)/tennis_atp/'
    else:
        prefix = 'wta'
        input_path = 'csvs/WTA (Womens)/tennis_wta/'

    output_path = 'career_elos_' + prefix + '_' + str(datestart.year) + '_' + str(dateend.year) + '.csv'
    
    all_matches = []

    for yr in range(datestart.year, dateend.year+1):
        file_path = f"{input_path}{prefix}_matches_{yr}.csv"
        df = pd.read_csv(file_path, parse_dates=['tourney_date'])

        # Filter relevant matches
        df = df[
            (df['tourney_date'] > datestart) & 
            (df['tourney_date'] < dateend) & 
            ~(
                df.iloc[:, 23].str.contains('W') | 
                df.iloc[:, 23].str.contains('R')
            )
        ]
        all_matches.append(df)

    matches_df = pd.concat(all_matches).sort_values(by='tourney_date').reset_index(drop=True)

    combined_names = pd.concat([matches_df['winner_name'], matches_df['loser_name']])

    players_to_elo = combined_names.drop_duplicates().tolist()

    new_header = ['Player', 'Date', 'Number', 'Matches', 'Wins', 'Elo', 'Lefty Elo', 'Righty Elo', 'Hard Elo', 'Clay Elo', 'Grass Elo']

    data = {
        'Player': players_to_elo,
        'Date': [datetime(1900, 1, 1)] * len(players_to_elo),
        'Number': [0] * len(players_to_elo),
        'Matches': [0] * len(players_to_elo),
        'Wins': [0] * len(players_to_elo),
        'Elo': [1500] * len(players_to_elo),
        'Lefty Elo': [1500] * len(players_to_elo),
        'Righty Elo': [1500] * len(players_to_elo),
        'Hard Elo': [1500] * len(players_to_elo),
        'Clay Elo': [1500] * len(players_to_elo),
        'Grass Elo': [1500] * len(players_to_elo)
    }

    global players_elo
    players_elo = pd.DataFrame(data, columns=new_header)

    for index, row in matches_df.iterrows():
        print(f"Processing match index: {index}, Winner: {row['winner_name']}, Loser: {row['loser_name']}")
        try:
            update_elo(
                players_elo.loc[players_elo['Player'] == row["winner_name"]], 
                players_elo.loc[players_elo['Player'] == row["loser_name"]], 
                row["winner_name"], 
                row["tourney_level"], 
                row["tourney_date"], 
                row["match_num"],
                row["surface"],
                row["winner_hand"],
                row["loser_hand"]
            )
        except Exception as e:
            print(f"An error occurred: {e}")
            pass
    
    players_elo.to_csv('csvs/Generated/' + output_path, index=False)
    print("Done")


def update_elo(player_a, player_b, winner, level, match_date, match_num, surface, winner_hand, loser_hand):
    if player_a.empty or player_b.empty:
        print("Error: One of the players not found in players_elo DataFrame")
        return

    idxA = player_a.index[0]
    idxB = player_b.index[0]

    rA = players_elo.at[idxA, 'Elo']
    rB = players_elo.at[idxB, 'Elo']

    # Determine which Elo ratings to use/update
    # if winner_hand == 'L':
    #     rB_hand = players_elo.at[idxB, 'Lefty Elo']
    # else:
    #     rB_hand = players_elo.at[idxB, 'Righty Elo']
    
    # if loser_hand == 'L':
    #     rA_hand = players_elo.at[idxA, 'Lefty Elo']
    # else:
    #     rA_hand = players_elo.at[idxA, 'Righty Elo']

    # Surface-specific Elo
    if surface == 'Hard':
        rA_surface = players_elo.at[idxA, 'Hard Elo']
        rB_surface = players_elo.at[idxB, 'Hard Elo']
    elif surface == 'Clay':
        rA_surface = players_elo.at[idxA, 'Clay Elo']
        rB_surface = players_elo.at[idxB, 'Clay Elo']
    elif surface == 'Grass':
        rA_surface = players_elo.at[idxA, 'Grass Elo']
        rB_surface = players_elo.at[idxB, 'Grass Elo']
    else:
        rA_surface = rA
        rB_surface = rB

    eA = 1 / (1 + 10 ** ((rB - rA) / 400))
    eB = 1 / (1 + 10 ** ((rA - rB) / 400))
    
    eA_surface = 1 / (1 + 10 ** ((rB_surface - rA_surface) / 400))
    eB_surface = 1 / (1 + 10 ** ((rA_surface - rB_surface) / 400))

    # eA_hand = 1 / (1 + 10 ** ((rB_hand - rA_hand) / 400))
    # eB_hand = 1 / (1 + 10 ** ((rA_hand - rB_hand) / 400))

    if winner == players_elo.at[idxA, 'Player']:
        sA, sB = 1, 0
    else:
        sA, sB = 0, 1
    
    kA = 250 / ((players_elo.at[idxA, 'Matches'] + 5) ** 0.4)
    kB = 250 / ((players_elo.at[idxB, 'Matches'] + 5) ** 0.4)
    k = 1.1 if level == "G" else 1
    
    rA_new = rA + (k * kA) * (sA - eA)
    rB_new = rB + (k * kB) * (sB - eB)
    
    rA_surface_new = rA_surface + (k * kA) * (sA - eA_surface)
    rB_surface_new = rB_surface + (k * kB) * (sB - eB_surface)

    # rA_hand_new = rA_hand + (k * kA) * (sA - eA_hand)
    # rB_hand_new = rB_hand + (k * kB) * (sB - eB_hand)
    
    # if winner_hand == 'L':
    #     players_elo.at[idxB, 'Lefty Elo'] = rB_hand_new
    # else:
    #     players_elo.at[idxB, 'Righty Elo'] = rB_hand_new

    # if loser_hand == 'L':
    #     players_elo.at[idxA, 'Lefty Elo'] = rA_hand_new
    # else:
    #     players_elo.at[idxA, 'Righty Elo'] = rA_hand_new

    # Update surface-specific Elo
    if surface == 'Hard':
        players_elo.at[idxA, 'Hard Elo'] = rA_surface_new
        players_elo.at[idxB, 'Hard Elo'] = rB_surface_new
    elif surface == 'Clay':
        players_elo.at[idxA, 'Clay Elo'] = rA_surface_new
        players_elo.at[idxB, 'Clay Elo'] = rB_surface_new
    elif surface == 'Grass':
        players_elo.at[idxA, 'Grass Elo'] = rA_surface_new
        players_elo.at[idxB, 'Grass Elo'] = rB_surface_new

    # General updates
    players_elo.at[idxA, 'Elo'] = rA_new
    players_elo.at[idxA, 'Date'] = match_date
    players_elo.at[idxA, 'Number'] = match_num
    players_elo.at[idxA, 'Matches'] += 1

    players_elo.at[idxB, 'Elo'] = rB_new
    players_elo.at[idxB, 'Date'] = match_date
    players_elo.at[idxB, 'Number'] = match_num
    players_elo.at[idxB, 'Matches'] += 1

career_stats('20231231','m')
