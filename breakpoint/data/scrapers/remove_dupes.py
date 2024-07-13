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
df['surface'].replace('Red clay', 'Clay', inplace=True)

df = df.sort_values(by='tourney_date').reset_index(drop=True)
df.to_csv('csvs/Generated/atp_matches_2024_revised.csv', index=False)
