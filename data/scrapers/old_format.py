import pandas as pd

file_path = "csvs/Generated/atp_matches_2024.csv"
df = pd.read_csv(file_path)
df = df.drop_duplicates(subset=['match_num'])  # Assign the result back to df

fill_values = {
    'best_of': 3,
}

df = df.fillna(value=fill_values)
df = df.fillna(-1)

# Convert appropriate columns to int
for col in df.select_dtypes(include='float'):
    df[col] = df[col].astype(int)

df.replace(-1, '', inplace=True)
df['surface'].replace('Hardcourt outdoor', 'Hard', inplace=True)
df['surface'].replace('Hardcourt indoor', 'Hard', inplace=True)
df['surface'].replace('Red clay', 'Clay', inplace=True)

def format_score(row):
    scores = []
    rounds = row['best_of']
    for i in range(1, rounds+1):
        winner_score = row[f'w{i}']
        loser_score = row[f'l{i}']
        if winner_score == 7:
            scores.append(f'{winner_score}-{loser_score}({2})')
        else:
            scores.append(f'{winner_score}-{loser_score}')
    return ' '.join(scores)
 
df['score'] = df.apply(format_score, axis=1)
df = df[['tourney_id','tourney_name','surface','draw_size','tourney_level','tourney_date','match_num','winner_id','winner_seed','winner_entry','winner_name','winner_hand','winner_ht','winner_ioc','winner_age','loser_id','loser_seed','loser_entry','loser_name','loser_hand','loser_ht','loser_ioc','loser_age','score','best_of','round','minutes','w_ace','w_df','w_svpt','w_1stIn','w_1stWon','w_2ndWon','w_SvGms','w_bpSaved','w_bpFaced','l_ace','l_df','l_svpt','l_1stIn','l_1stWon','l_2ndWon','l_SvGms','l_bpSaved','l_bpFaced','winner_rank','winner_rank_points','loser_rank','loser_rank_points']]

df = df.sort_values(by='tourney_date').reset_index(drop=True)
df.to_csv('csvs/Generated/atp_matches_2024_revised.csv', index=False)
