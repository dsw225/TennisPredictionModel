import pandas as pd
from datetime import datetime
from math import pow, copysign

START_RATING = 1500
RATING_SCALE = 480.0 # https://en.wikipedia.org/wiki/Elo_rating_system#Suggested_modification Try for a little
K_FACTOR = 32.0
K_FUNCTION_AMPLIFIER = 10.0
K_FUNCTION_AMPLIFIER_GRADIENT = 63.0
K_FUNCTION_MULTIPLIER = 2.0 * (K_FUNCTION_AMPLIFIER - 1.0)
DELTA_RATING_CAP = 200.0

RECENT_K_FACTOR = 2.0
SET_K_FACTOR = 0.5
GAME_K_FACTOR = 0.0556
SERVICE_GAME_K_FACTOR = 0.1667
RETURN_GAME_K_FACTOR = 0.1667
TIE_BREAK_K_FACTOR = 1.5

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
                df.iloc[:, 23].str.contains('R') |
                (df.iloc[:, 27:45].isnull().values.any(axis=1)) #For service and stuff etc
            )
        ]
        all_matches.append(df)

    matches_df = pd.concat(all_matches).sort_values(by='tourney_date').reset_index(drop=True)

    combined_names = pd.concat([matches_df['winner_name'], matches_df['loser_name']])

    players_to_elo = combined_names.drop_duplicates().tolist()

    new_header = ['player', 'last_date', 'match_number', 'matches_played', 'elo_rating', 'sets_elo_rating', 'lefty_elo_rating', 
                  'righty_elo_rating', 'hard_elo_rating', 'clay_elo_rating', 'grass_elo_rating', 'outdoor_elo_rating', 'indoor_elo_rating', 
                  'set_elo_rating', 'game_elo_rating', 'service_game_elo_rating', 'return_game_elo_rating', 'tie_break_elo_rating']

    data = {
        'player': players_to_elo,
        'last_date': [datetime(1900, 1, 1)] * len(players_to_elo),
        'match_number': [0] * len(players_to_elo),
        'matches_played': [0] * len(players_to_elo),
        'elo_rating': [START_RATING] * len(players_to_elo),
        'sets_elo_rating': [START_RATING] * len(players_to_elo),
        'lefty_elo_rating': [START_RATING] * len(players_to_elo),
        'righty_elo_rating': [START_RATING] * len(players_to_elo),
        'hard_elo_rating': [START_RATING] * len(players_to_elo),
        'clay_elo_rating': [START_RATING] * len(players_to_elo),
        'grass_elo_rating': [START_RATING] * len(players_to_elo),
        'outdoor_elo_rating': [START_RATING] * len(players_to_elo),
        'indoor_elo_rating': [START_RATING] * len(players_to_elo),
        'set_elo_rating': [START_RATING] * len(players_to_elo),
        'game_elo_rating': [START_RATING] * len(players_to_elo),
        'service_game_elo_rating': [START_RATING] * len(players_to_elo),
        'return_game_elo_rating': [START_RATING] * len(players_to_elo),
        'tie_break_elo_rating': [START_RATING] * len(players_to_elo),
    }

    global players_elo
    players_elo = pd.DataFrame(data, columns=new_header)

    for index, row in matches_df.iterrows():
        if index % 1000 == 0:
            print(f"Processing Elos @ Match indexes: {index} - {index+1000}")
        try:
            update_elos(
                players_elo.loc[players_elo['player'] == row["winner_name"]], 
                players_elo.loc[players_elo['player'] == row["loser_name"]], 
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

    rA = players_elo.at[idxA, 'elo_rating']
    rB = players_elo.at[idxB, 'elo_rating']

    # We know player A won
    delta = delta_rating(rA, rB, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    
    rA_new = new_rating(rA, delta)
    rB_new = new_rating(rB, -delta)

    # Innacurate way to determine surface elo - need better method
    surface = row['surface']
    if(surface=='Hard'):
        rA = players_elo.at[idxA, 'hard_elo_rating']
        rB = players_elo.at[idxB, 'hard_elo_rating']

        delta = delta_rating(rA, rB, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

        players_elo.at[idxA, 'hard_elo_rating'] = new_rating(rA, delta)
        players_elo.at[idxB, 'hard_elo_rating'] = new_rating(rB, -delta)
    elif(surface=='Clay'):
        rA = players_elo.at[idxA, 'clay_elo_rating']
        rB = players_elo.at[idxB, 'clay_elo_rating']

        delta = delta_rating(rA, rB, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

        players_elo.at[idxA, 'clay_elo_rating'] = new_rating(rA, delta)
        players_elo.at[idxB, 'clay_elo_rating'] = new_rating(rB, -delta)
    elif(surface=='Grass'):
        rA = players_elo.at[idxA, 'grass_elo_rating']
        rB = players_elo.at[idxB, 'grass_elo_rating']

        delta = delta_rating(rA, rB, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

        players_elo.at[idxA, 'grass_elo_rating'] = new_rating(rA, delta)
        players_elo.at[idxB, 'grass_elo_rating'] = new_rating(rB, -delta)


    # Other elos
    rAset = players_elo.at[idxA, 'set_elo_rating']
    rAgame = players_elo.at[idxA, 'game_elo_rating']
    rAservice = players_elo.at[idxA, 'service_game_elo_rating']
    rAreturn = players_elo.at[idxA, 'return_game_elo_rating']

    rBset = players_elo.at[idxB, 'set_elo_rating']
    rBgame = players_elo.at[idxB, 'game_elo_rating']
    rBservice = players_elo.at[idxB, 'service_game_elo_rating']
    rBreturn = players_elo.at[idxB, 'return_game_elo_rating']

    # We know player A won
    deltaSet = delta_rating_extra(rAset, rBset, row, 'N/A', 's')
    deltaGame = delta_rating_extra(rAgame, rBgame, row, 'N/A', 'g')
    deltaService = delta_rating_extra(rAservice, rBservice, row, 'N/A', 'sg')
    deltaReturn = delta_rating_extra(rAreturn, rBreturn, row, 'N/A', 'rg')
    
    players_elo.at[idxA, 'set_elo_rating'] = new_rating(rAset, deltaSet)
    players_elo.at[idxB, 'set_elo_rating'] = new_rating(rBset, -deltaSet)
    players_elo.at[idxA, 'game_elo_rating'] = new_rating(rAgame, deltaGame)
    players_elo.at[idxB, 'game_elo_rating'] = new_rating(rBgame, -deltaGame)
    players_elo.at[idxA, 'service_game_elo_rating'] = new_rating(rAservice, deltaService)
    players_elo.at[idxB, 'service_game_elo_rating'] = new_rating(rBservice, -deltaService)
    players_elo.at[idxA, 'return_game_elo_rating'] = new_rating(rAreturn, deltaReturn)
    players_elo.at[idxB, 'return_game_elo_rating'] = new_rating(rBreturn, -deltaReturn)

    #remaining
    match_date = row['tourney_date']
    match_num = row['match_num']
    players_elo.at[idxA, 'elo_rating'] = rA_new
    players_elo.at[idxA, 'last_date'] = match_date
    players_elo.at[idxA, 'match_number'] = match_num
    players_elo.at[idxA, 'matches_played'] += 1

    players_elo.at[idxB, 'elo_rating'] = rB_new
    players_elo.at[idxB, 'last_date'] = match_date
    players_elo.at[idxB, 'match_number'] = match_num
    players_elo.at[idxB, 'matches_played'] += 1


def k_factor(level, tourney_name, round, best_of, outcome):
    k = K_FACTOR
    if "G" == level:
        k *= 1.0
    elif "Tour Finals" in tourney_name:
        k *= .9
    elif "M" in level:
        k *= .85
    elif "Olympics" in tourney_name:
        k *= .8
    elif "A" in level:
        k *= .7
    else:
        k *=.65

    # Match round adjustment is: Final 100%, Semi-Final 90%, Quarter-Final and Round-Robin 85%, Rounds of 16 and 32 80%, Rounds of 64 and 128 75% and For Bronze Medal 95%
    round_factors = {
        "F": 1.0, "BR": 0.95, "SF": 0.90, "QF": 0.85, "R16": 0.80, "R32": 0.80,
        "R64": 0.75, "R128": 0.75, "RR": 0.85
    }

    k *= round_factors.get(round, 1.0)
    
    if best_of < 5:
        k *= 0.90

    # if outcome == "W/O":
    #     k *= 0.50
    
    return k

def delta_rating(winner_rating, loser_rating, level, tourney_name, round, best_of, outcome):
    if outcome == "ABD":
        return 0.0
    delta = 1.0 / (1.0 + pow(10.0, (winner_rating - loser_rating) / RATING_SCALE))
    return k_factor(level, tourney_name, round, best_of, outcome) * delta

def new_rating(rating, delta): #GOOD
    return rating + cap_delta_rating(delta * k_function(rating))

def cap_delta_rating(delta): #GOOD
    return copysign(min(abs(delta), DELTA_RATING_CAP), delta)

def k_function(rating): #good
    return 1.0 + K_FUNCTION_MULTIPLIER / (1.0 + pow(2.0, (rating - START_RATING) / K_FUNCTION_AMPLIFIER_GRADIENT))

def elo_win_probability(elo_rating1, elo_rating2):
    return 1.0 / (1.0 + pow(10.0, (elo_rating2 - elo_rating1) / RATING_SCALE))

# Stolen and translated from https://github.com/mcekovic/tennis-crystal-ball/blob/master/tennis-stats/src/main/java/org/strangeforest/tcb/stats/model/elo/EloCalculator.java need to implement
def delta_rating_extra(winner_rating, loser_rating, row, outcome, type):
    level = row['tourney_level']
    name = row['tourney_name']
    round = row['round']
    best_of = int(row['best_of'])
    surface = row['surface']

    delta = delta_rating(winner_rating, loser_rating, level, name, round, best_of, outcome)
    
    w_delta = delta
    l_delta = delta_rating(loser_rating, winner_rating, level, name, round, best_of, outcome)
    w_sv_gms = save_games(row['w_SvGms'], row['w_bpFaced'], row['w_bpSaved'])
    l_sv_gms = save_games(row['l_SvGms'], row['l_bpFaced'], row['l_bpSaved'])
    w_rt_gms = return_games(row['w_bpFaced'], row['w_bpSaved'])
    l_rt_gms = return_games(row['l_bpFaced'], row['l_bpSaved'])
    score = row['score']
    w_games, l_games = 0, 0
    w_sets, l_sets = 0, 0

    sets = score.split()

    for set_score in sets:
        games = set_score.split('-')
        
        w_set = int(games[0].split('(')[0])
        l_set = int(games[1].split('(')[0])
        
        w_games += w_set
        l_games += l_set
        
        if w_set > l_set:
            w_sets += 1
        else:
            l_sets += 1

    if type == "s":
        return SET_K_FACTOR * (w_delta * w_sets - l_delta * l_sets)
    if type == "g":
        return GAME_K_FACTOR * (w_delta * w_games - l_delta * l_games)
    if type == "sg":
        return SERVICE_GAME_K_FACTOR * (w_delta * w_sv_gms * return_to_serve_ratio(surface) - l_delta * l_rt_gms)
    if type == "rg":
        return RETURN_GAME_K_FACTOR * (w_delta * w_rt_gms - l_delta * l_sv_gms * return_to_serve_ratio(surface))
    if type == "tb":
        w_tbs = row['w_tbs'] #Doesnt exist need to fio
        l_tbs = row['l_tbs']
        if l_tbs > w_tbs:
            w_delta, l_delta = l_delta, w_delta
        return TIE_BREAK_K_FACTOR * (w_delta * w_tbs - l_delta * l_tbs)
    
    raise ValueError("Invalid type")

def return_to_serve_ratio(surface):
    surface_ratios = {
        "Hard": 0.281, "Clay": 0.365, "Grass": 0.227, "P": 0.243
    }
    return surface_ratios.get(surface, .297)

def save_games(sv_gms, bp_faced, bp_saved):
    return int(sv_gms) - (int(bp_faced) - int(bp_saved))

def return_games(bp_faced, bp_saved):
    return int(bp_faced)- int(bp_saved)

career_stats('20231231','m')
