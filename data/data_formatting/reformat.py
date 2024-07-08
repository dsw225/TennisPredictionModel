import pandas as pd
from datetime import datetime, timedelta
import re

mw = 'm'

# Define paths based on mw value
if mw == 'm':   
    prefix = 'atp'
    input_path = 'csvs/ATP (Mens)/tennis_atp/atp_matches_'
else:
    prefix = 'wta'
    input_path = 'csvs/WTA (Womens)/tennis_wta/wta_matches_'

year = 2023
file_path = f"{input_path}{year}.csv"

# Load and preprocess the data
df = pd.read_csv(file_path)
# df = df.drop_duplicates(subset=['match_num'])

# Fill missing values
fill_values = {'best_of': 3}
df = df.fillna(value=fill_values)
df = df.fillna(-1)

# Convert appropriate columns to int
for col in df.select_dtypes(include='float').columns:
    df[col] = df[col].astype(int)

df.replace(-1, '', inplace=True)



def parse_score_to_columns(row):
    # Initialize columns for scores
    for i in range(1, 6):
        row[f'w{i}'] = ''
        row[f'l{i}'] = ''
    print(row['score'])
    scores = row['score'].split(' ')
    for i in range(0, len(scores)):
        if 'W' in scores[i] or 'R' in scores[i] or 'D' in scores[i] or '[' in scores[i]:
            return row
        if '(' in scores[i]:
            winner_score, rest = scores[i].split('-')
            loser_score = rest.split('(')[0]
            row[f'w{i+1}'] = int(winner_score)
            row[f'l{i+1}'] = int(loser_score)
        else:
            winner_score, loser_score = scores[i].split('-')
            row[f'w{i+1}'] = int(winner_score)
            row[f'l{i+1}'] = int(loser_score)
    print(row['w1'])
    print(row['l1'])
    return row

# Apply the score parsing function
df = df.apply(parse_score_to_columns, axis=1)

# Define the desired columns order
pd_headers = ['tourney_id','tourney_name','surface','draw_size','tourney_level','tourney_date',
              'match_num','best_of','round','minutes','winner_id','winner_seed','winner_entry','winner_name','winner_hand',
              'winner_ht','winner_ioc','winner_age','loser_id','loser_seed','loser_entry','loser_name',
              'loser_hand','loser_ht','loser_ioc','loser_age','w1','w2','w3','w4','w5','w_ace',
              'w_df','w_svpt','w_1stIn','w_1stWon','w_2ndWon','w_SvGms','w_bpSaved','w_bpFaced','l1','l2','l3','l4','l5','l_ace','l_df',
              'l_svpt','l_1stIn','l_1stWon','l_2ndWon','l_SvGms','l_bpSaved','l_bpFaced','winner_rank','loser_rank']

# Reorder the DataFrame
df = df[pd_headers]
# print(df.head)

# Sort and save the DataFrame
df = df.sort_values(by='tourney_date').reset_index(drop=True)
output_path = f'csvs/Generated/{prefix}_matches_{year}_revised.csv'
df.to_csv(output_path, index=False)
