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

        # df1 = pd.read_csv(file_path_1, parse_dates=['tourney_date'])
        # df2 = pd.read_csv(file_path_2, parse_dates=['tourney_date'])

        # # Concatenate the DataFrames
        # df = pd.concat([df1, df2], ignore_index=True)

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

    new_header = ['Player', 'Date', 'Number', 'Matches', 'Sets Played', 'Elo', 'Sets Elo', 'Lefty Elo', 'Righty Elo', 'Hard Elo', 'Clay Elo', 'Grass Elo']

    data = {
        'Player': players_to_elo,
        'Date': [datetime(1900, 1, 1)] * len(players_to_elo),
        'Number': [0] * len(players_to_elo),
        'Matches': [0] * len(players_to_elo),
        'Sets Played': [0] * len(players_to_elo),
        'Elo': [1500] * len(players_to_elo),
        'Sets Elo': [1500] * len(players_to_elo),
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
            update_elos(
                players_elo.loc[players_elo['Player'] == row["winner_name"]], 
                players_elo.loc[players_elo['Player'] == row["loser_name"]], 
                row
            )
        except Exception as e:
            print(f"An error occurred: {e}")
            pass
    
    players_elo.to_csv('csvs/Generated/' + output_path, index=False)
    print("Done")


def update_elos(player_a, player_b, row):
    if player_a.empty or player_b.empty:
        print("Error: One of the players not found in players_elo DataFrame")
        return

    idxA = player_a.index[0]
    idxB = player_b.index[0]

    rA = players_elo.at[idxA, 'Elo']
    rB = players_elo.at[idxB, 'Elo']

    # Surface-specific Elo
    surface = row['surface']
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

    # We know player A won
    sA, sB = 1, 0
    
    # Add future changes:
    # Tournament level adjustment is: Grand Slam 100%, Tour Finals 90%, Masters 85%, Olympics 80%, ATP 500 75% and all others 70%
    if "G" == row['tourney_name']:
        tournament_level = 1.0
    elif "Tour Finals" in row['tourney_name']:
        tournament_level = .9
    elif "M" in row['tourney_name']:
        tournament_level = .85
    elif "Olympics" in row['tourney_name']:
        tournament_level = .8
    else:
        tournament_level = .7
    # Match round adjustment is: Final 100%, Semi-Final 90%, Quarter-Final and Round-Robin 85%, Rounds of 16 and 32 80%, Rounds of 64 and 128 75% and For Bronze Medal 95%
    match_round = .8 #Temp
    # Walkover is 50%

    # K-factor base value is 32
    k_base = 32

    # Best-of sets adjustment: Best-of-5 100% and Best-of-3 90%
    best_factor = .9 if row['best_of'] == 3 else 1
    
    # # Current rating adjustment (this allows lower ranked players to advance more rapidly, while stabilizes ratings at the top):
    kFunctionA = 1 + 18 / (1 + 2 * (rA - 1500) / 63)
    kFunctionB = 1 + 18 / (1 + 2 * (rB - 1500) / 63)

    kA = 5 / ((players_elo.at[idxA, 'Matches'] + 5) ** 0.4)
    kB = 5 / ((players_elo.at[idxB, 'Matches'] + 5) ** 0.4)
    
    kA = best_factor * rating_adjustmentA * tournament_level * match_round * kA
    kB = best_factor * rating_adjustmentB * tournament_level * match_round * kB
    
    
    rA_new = rA +  kA * (sA - eA)
    rB_new = rB +  kB * (sB - eB)
    
    rA_surface_new = rA_surface + (k_base * kA) * (sA - eA_surface)
    rB_surface_new = rB_surface + (k_base * kB) * (sB - eB_surface)

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
    # calculate_sets_elo(player_a, player_b, winner, score)
    

    match_date = row['tourney_date']
    match_num = row['match_num']
    players_elo.at[idxA, 'Elo'] = rA_new
    players_elo.at[idxA, 'Date'] = match_date
    players_elo.at[idxA, 'Number'] = match_num
    players_elo.at[idxA, 'Matches'] += 1

    players_elo.at[idxB, 'Elo'] = rB_new
    players_elo.at[idxB, 'Date'] = match_date
    players_elo.at[idxB, 'Number'] = match_num
    players_elo.at[idxB, 'Matches'] += 1


def calculate_sets_elo(player_a, player_b, winner, score):
    if player_a.empty or player_b.empty:
        print("Error: One of the players not found in players_elo DataFrame")
        return

    idxA = player_a.index[0]
    idxB = player_b.index[0]

    score = score.split(' ')
    sA = 0
    sB = 0
    for set_score in score:
        # Remove tie-break scores if present    
        if '(' in set_score:
            set_score = set_score.split('(')[0]
        
        # Split the set score into individual games
        set_score = set_score.split('-')
    
        sA += int(set_score[0]) if winner == players_elo.at[idxA, 'Player'] else int(set_score[1])
        sB += int(set_score[1]) if winner == players_elo.at[idxA, 'Player'] else int(set_score[0])

    total_sets = sA + sB

    sA = sA / total_sets
    sB = sB / total_sets

    rA_sets = players_elo.at[idxA, 'Sets Elo']
    rB_sets = players_elo.at[idxB, 'Sets Elo']

    kA = 250 / ((players_elo.at[idxA, 'Sets Played'] + 5) ** 0.4)
    kB = 250 / ((players_elo.at[idxB, 'Sets Played'] + 5) ** 0.4)
    # k = 1.1 if level == "G" else 1
    k=1

    eA = 1 / (1 + 10 ** ((rB_sets - rA_sets) / 400))
    eB = 1 / (1 + 10 ** ((rA_sets - rB_sets) / 400))

    rA_sets_new = rA_sets + (k * kA) * (sA - eA)
    rB_sets_new = rB_sets + (k * kB) * (sB - eB)

    players_elo.at[idxA, 'Sets Elo'] = rA_sets_new
    players_elo.at[idxA, 'Sets Played'] += total_sets

    players_elo.at[idxB, 'Sets Elo'] = rB_sets_new
    players_elo.at[idxB, 'Sets Played'] += total_sets

        
career_stats('20231231','m')
