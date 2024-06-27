import pandas as pd
from datetime import datetime
from math import pow, copysign

START_RATING = 1500
RATING_SCALE = 480.0 # https://en.wikipedia.org/wiki/Elo_rating_system#Suggested_modification https://en.chessbase.com/post/the-elo-rating-system-correcting-the-expectancy-tables
K_FACTOR = 32.0
K_FUNCTION_AMPLIFIER = 10.0
K_FUNCTION_AMPLIFIER_GRADIENT = 63.0
K_FUNCTION_MULTIPLIER = 2.0 * (K_FUNCTION_AMPLIFIER - 1.0)
DELTA_RATING_CAP = 200.0
# Added to normalize elos - make more standard ratings - needs adjustment .409596
SERVE_RETURN_K_FACTOR = 4.2
TB_K_FACTOR = 3.275

def career_stats(date, mw):
    dateend = datetime.strptime(date, "%Y%m%d")
    datestart = datetime(2022, 1, 1)
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

    new_header = ['player', 'last_date', 'match_number', 'matches_played', 'elo_rating', 'lefty_elo_rating', 
                  'righty_elo_rating', 'hard_elo_rating', 'clay_elo_rating', 'grass_elo_rating', 'outdoor_elo_rating', 'indoor_elo_rating', 
                  'set_elo_rating', 'game_elo_rating', 'service_game_elo_rating', 'return_game_elo_rating', 'tie_break_elo_rating']

    data = {
        'player': players_to_elo,
        'last_date': [datetime(1900, 1, 1)] * len(players_to_elo),
        'match_number': [0] * len(players_to_elo),
        'matches_played': [0] * len(players_to_elo),
        'elo_rating': [START_RATING] * len(players_to_elo),
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
    players_elo['last_date'] = pd.to_datetime(players_elo['last_date'])
    players_elo = players_elo[~(
                (players_elo['matches_played'] < 10) |
                (players_elo['last_date'] < pd.to_datetime('2023-02-01'))
            )]
    players_elo = players_elo.sort_values(by='elo_rating', ascending=False)
    players_elo.to_csv('csvs/Generated/' + output_path, index=False)
    print("Done")

def update_elos(player_a, player_b, row):
    if player_a.empty or player_b.empty:
        print("Error: One of the players not found in players_elo DataFrame")
        return
    
    score = row['score']
    sets = score.split()

    w_games, l_games = 0, 0
    w_sets, l_sets = 0, 0
    tie_breaks_won_winner, tie_breaks_won_loser = 0, 0

    for set_score in sets:
        if '(' in set_score:
            # There was a tie-break in this set
            normal_score, tie_break_score = set_score.split('(')
            tie_break_score = tie_break_score.rstrip(')')
            
            # Parse normal score
            w_set, l_set = map(int, normal_score.split('-'))
            
            # Determine tie-break winner
            if w_set > l_set:
                w_sets += 1
                tie_breaks_won_winner += 1
            else:
                tie_breaks_won_loser += 1
                l_sets += 1
        else:
            # Parse normal score
            w_set, l_set = map(int, set_score.split('-'))
            if w_set > l_set:
                w_sets += 1
            else:
                l_sets += 1

        w_games += w_set
        l_games += l_set
        
    deciding_set = True if len(sets) == 3 and row['best_of'] == 3 or len(sets) == 5 and row['best_of'] == 5 else False

    tie_breaks_played = tie_breaks_won_winner + tie_breaks_won_loser

    # We know player A won

    # Current surface_elo method -- Ok?
    surface_elos(player_a, player_b, row)

    sets_games_elo(player_a, player_b, row, w_sets, l_sets, w_games, l_games)

    tb_elo(player_a, player_b, row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played, deciding_set)

    return_serve_elo(player_a, player_b, row)

    primary_elo(player_a, player_b, row)

    
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

def delta_rating(winner_rating, loser_rating, outcome):
    if outcome == "ABD":
        return 0.0
    delta = 1.0 / (1.0 + pow(10.0, (winner_rating - loser_rating) / RATING_SCALE))
    return delta

def new_rating(rating, delta, level, tourney_name, round, best_of, outcome): #GOOD
    return rating + cap_delta_rating(k_factor(level, tourney_name, round, best_of, outcome) * delta * k_function(rating))

def cap_delta_rating(delta): #GOOD
    return copysign(min(abs(delta), DELTA_RATING_CAP), delta)

def k_function(rating): #GOOD
    return 1.0 + K_FUNCTION_MULTIPLIER / (1.0 + pow(2.0, (rating - START_RATING) / K_FUNCTION_AMPLIFIER_GRADIENT))

def elo_win_probability(elo_rating1, elo_rating2):
    return 1.0 / (1.0 + pow(10.0, (elo_rating2 - elo_rating1) / RATING_SCALE))

def primary_elo(player_a, player_b, row):
    idxA = player_a.index[0]
    idxB = player_b.index[0]

    rA = players_elo.at[idxA, 'elo_rating']
    rB = players_elo.at[idxB, 'elo_rating']

    delta = delta_rating(rA, rB, "N/A")

    rA_new = new_rating(rA, delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    rB_new = new_rating(rB, -delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

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

def surface_elos(player_a, player_b, row):
    idxA = player_a.index[0]
    idxB = player_b.index[0]

    surface = row['surface']
    if(surface=='Hard'):
        rA = players_elo.at[idxA, 'hard_elo_rating']
        rB = players_elo.at[idxB, 'hard_elo_rating']

        delta = delta_rating(rA, rB, "N/A")

        players_elo.at[idxA, 'hard_elo_rating'] = new_rating(rA, delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
        players_elo.at[idxB, 'hard_elo_rating'] = new_rating(rB, -delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    elif(surface=='Clay'):
        rA = players_elo.at[idxA, 'clay_elo_rating']
        rB = players_elo.at[idxB, 'clay_elo_rating']

        delta = delta_rating(rA, rB, "N/A")

        players_elo.at[idxA, 'clay_elo_rating'] = new_rating(rA, delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
        players_elo.at[idxB, 'clay_elo_rating'] = new_rating(rB, -delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    elif(surface=='Grass'):
        rA = players_elo.at[idxA, 'grass_elo_rating']
        rB = players_elo.at[idxB, 'grass_elo_rating']

        delta = delta_rating(rA, rB, "N/A")

        players_elo.at[idxA, 'grass_elo_rating'] = new_rating(rA, delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
        players_elo.at[idxB, 'grass_elo_rating'] = new_rating(rB, -delta, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

# Stolen and changed from https://github.com/mcekovic/tennis-crystal-ball/blob/master/tennis-stats/src/main/java/org/strangeforest/tcb/stats/model/elo/EloCalculator.java need to implement
def sets_games_elo(player_a, player_b, row, w_sets, l_sets, w_games, l_games):
    idxA = player_a.index[0]
    idxB = player_b.index[0]

    rAset = players_elo.at[idxA, 'set_elo_rating']
    rBset = players_elo.at[idxB, 'set_elo_rating']

    rAgame = players_elo.at[idxA, 'game_elo_rating']
    rBgame = players_elo.at[idxB, 'game_elo_rating']

    deltaSet = delta_rating(rAset, rBset, 'N/A')
    deltaGame = delta_rating(rAgame, rBgame, 'N/A')

    deltaSetNew = deltaSet * ((w_sets -  l_sets)/(w_sets + l_sets))
    deltaGameNew = deltaGame * ((w_games - l_games)/(w_games + l_games))

    players_elo.at[idxA, 'set_elo_rating'] = new_rating(rAset, deltaSetNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    players_elo.at[idxB, 'set_elo_rating'] = new_rating(rBset, -deltaSetNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

    players_elo.at[idxA, 'game_elo_rating'] = new_rating(rAgame, deltaGameNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    players_elo.at[idxB, 'game_elo_rating'] = new_rating(rBgame, -deltaGameNew, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

def tb_elo(player_a, player_b, row, tie_breaks_won_winner, tie_breaks_won_loser, tie_breaks_played, deciding_set):
    idxA = player_a.index[0]
    idxB = player_b.index[0]

    rAtb = players_elo.at[idxA, 'tie_break_elo_rating']
    rBtb = players_elo.at[idxB, 'tie_break_elo_rating']

    player_a_pressure_rating = pressure_rating(row['w_bpFaced'], row['w_bpSaved'], row['l_bpFaced'], row['l_bpSaved'], tie_breaks_won_winner, tie_breaks_played, deciding_set, True)
    player_b_pressure_rating = pressure_rating(row['l_bpFaced'], row['l_bpSaved'], row['w_bpFaced'], row['w_bpSaved'], tie_breaks_won_loser, tie_breaks_played, deciding_set, False)

    delta = delta_rating(rAtb, rBtb, "N/A")
    player_a_service = TB_K_FACTOR * ((player_a_pressure_rating) / (player_a_pressure_rating + player_b_pressure_rating) - (1 - delta)) if (player_a_pressure_rating + player_b_pressure_rating) > 0 else TB_K_FACTOR * (1 - delta)

    players_elo.at[idxA, 'tie_break_elo_rating'] = new_rating(rAtb, player_a_service, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    players_elo.at[idxB, 'tie_break_elo_rating'] = new_rating(rBtb, -player_a_service, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

def return_serve_elo(player_a, player_b, row):
    idxA = player_a.index[0]
    idxB = player_b.index[0]
    surface = row['surface']

    rAservice = players_elo.at[idxA, 'service_game_elo_rating']
    rBservice = players_elo.at[idxB, 'service_game_elo_rating']

    rAreturn = players_elo.at[idxA, 'return_game_elo_rating']
    rBreturn = players_elo.at[idxB, 'return_game_elo_rating']

    playerA_serveRating = serve_rating(row['w_svpt'], row['w_ace'], row['w_df'], row['w_1stIn'], row['w_1stWon'], row['w_2ndWon'], row['w_bpFaced'], row['w_bpSaved'], row['w_SvGms'])
    playerB_serveRating = serve_rating(row['l_svpt'], row['l_ace'], row['l_df'], row['l_1stIn'], row['l_1stWon'], row['l_2ndWon'], row['l_bpFaced'], row['l_bpSaved'], row['l_SvGms'])

    playerA_returnRating = return_rating(row['l_svpt'], row['l_1stIn'], row['l_1stWon'], row['l_2ndWon'], row['l_bpFaced'], row['l_bpSaved'], row['l_SvGms'])
    playerB_returnRating = return_rating(row['w_svpt'], row['w_1stIn'], row['w_1stWon'], row['w_2ndWon'], row['w_bpFaced'], row['w_bpSaved'], row['w_SvGms'])

    delta = delta_rating(rAservice, rBreturn, "N/A")
    player_a_service = SERVE_RETURN_K_FACTOR * ((playerA_serveRating) / (playerA_serveRating + playerB_returnRating * return_to_serve_ratio(surface)) - (1 - delta))
    delta = delta_rating(rBservice, rAreturn, "N/A")
    player_b_service =  SERVE_RETURN_K_FACTOR * ((playerB_serveRating) / (playerB_serveRating + playerA_returnRating * return_to_serve_ratio(surface)) - (1 - delta))

    players_elo.at[idxA, 'service_game_elo_rating'] = new_rating(rAservice, player_a_service, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    players_elo.at[idxB, 'service_game_elo_rating'] = new_rating(rBservice, player_b_service, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

    players_elo.at[idxA, 'return_game_elo_rating'] = new_rating(rAreturn, -player_b_service, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")
    players_elo.at[idxB, 'return_game_elo_rating'] = new_rating(rBreturn, -player_a_service, row['tourney_level'], row['tourney_name'], row['round'], int(row['best_of']), "N/A")

def return_to_serve_ratio(surface):
    surface_ratios = {
        "Hard": 1.871, "Clay": 1.654, "Grass": 2.044
    }
    return surface_ratios.get(surface, 1.817)

# # Ultimate Tennis Serve Rating = Ace % - Double Faults % + 1st Serve % + 1st Serve Points Won % + 2nd Serve Points Won % + Break Points Saved % + Service Games Won %
# # ATP Serve Rating Official = Aces - Double Faults + 1st Serve % + 1st Serve Points Won % + 2nd Serve Points Won % + Service Games Won %
def serve_rating(sv_pt, ace, df, firstServe, firstServeWon, secondServeWon, bp_faced, bp_saved, sv_gms):
    secondServe = sv_pt - firstServe
    # ace_pct = ace/sv_pt * 100
    # df_pct = df/sv_pt * 100
    firstServe_pct = firstServe/sv_pt * 100 if sv_pt > 0 else 65
    firstServeWon_pct = firstServeWon/firstServe * 100 if firstServe > 0 else 72
    secondServeWon_pct = secondServeWon/secondServe * 100 if secondServe > 0 else 50
    # bp_saved_pct = bp_saved/bp_faced * 100
    service_games_won_pct = (sv_gms - (bp_faced - bp_saved))/ sv_gms * 100 if sv_gms > 0 else 79
    return ace - df + firstServe_pct + firstServeWon_pct + secondServeWon_pct + service_games_won_pct

# # Opponents Stats Used
# # Ultimate Tennis Return Rating = 1st Serve Return Points Won % + 2nd Serve Return Points Won % + Break Points Converted % + Return Games Won %
# # ATP Serve Rating Official = 1st Serve Return Points Won % + 2nd Serve Return Points Won % + Break Points Converted % + Return Games Won %
def return_rating(rt_pt, firstServeReturn, firstServeReturnWon, secondServeReturnWon, bp_faced_oppo, bp_saved_oppo, rt_gms):
    secondServeReturn = rt_pt - firstServeReturn
    firstServeReturnWon_pct = (firstServeReturn - firstServeReturnWon)/firstServeReturn * 100 if firstServeReturn > 0 else 25
    secondServeReturnWon_pct = (secondServeReturn - secondServeReturnWon)/secondServeReturn * 100 if secondServeReturn > 0 else 50
    bp_converted_pct = (bp_faced_oppo - bp_saved_oppo)/bp_faced_oppo * 100 if bp_faced_oppo > 0 else 38
    return_games_won_pct = (bp_faced_oppo - bp_saved_oppo)/ rt_gms * 100 if rt_gms > 0 else 19.5
    return firstServeReturnWon_pct + secondServeReturnWon_pct + bp_converted_pct + return_games_won_pct

# # Ultimate Tennis Tie Break Elo = wDelta * wTBs - lDelta * lTBs
# # ATP Serve Rating Official = Break Points Converted % + Break Points Saved % + Tie Breaks Won % + Deciding Sets %
def pressure_rating(bp_faced, bp_saved, bp_faced_oppo, bp_saved_oppo, tb_won, tb_total, deciding_set, won):
    bp_saved_pct = bp_saved/bp_faced * 100 if bp_faced > 0 else 62
    bp_converted_pct = (bp_faced_oppo - bp_saved_oppo)/bp_faced_oppo * 100 if bp_faced_oppo > 0 else 38
    tb_won_pct = tb_won/tb_total * 100 if tb_total > 0 else 50
    if deciding_set and won:
        deciding_sets_won_winner_pct = 100
    elif deciding_set:
        deciding_sets_won_winner_pct = 0
    else:
        deciding_sets_won_winner_pct = 50
    return bp_saved_pct + bp_converted_pct + tb_won_pct + deciding_sets_won_winner_pct


career_stats('20231231','w')
