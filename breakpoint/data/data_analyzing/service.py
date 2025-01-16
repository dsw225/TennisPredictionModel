import csv
import pandas as pd
import numpy as np
file_path = 'data/csvs/ATP (Mens)/tennis_atp/atp_matches_2022.csv'

df = pd.read_csv(file_path)

more_serve_won = 0
more_serve_lost = 0

#they did serve first, but only reason we know is because they won - most likely on service
for i, row in df.iterrows():



    if '(' not in row['score'] and ')' not in row['score'] and '[' not in row['score'] and ']' not in row['score'] and not any(char.isalpha() for char in row['score']):
        temp = row['score'].split(' ')
        scores = temp[-1].split('-')
        total = int(scores[0]) + int(scores[1])
        if total % 2 == 0:
            if row['w_SvGms'] > row['l_SvGms']:
                print(row['score'])
                more_serve_won+=1
            elif row['l_SvGms'] > row['w_SvGms']:
                more_serve_lost+=1
        

print(f"More serve won: {more_serve_won/(more_serve_won+more_serve_lost)} on total matches: {more_serve_won+more_serve_lost} out of {len(df)}")