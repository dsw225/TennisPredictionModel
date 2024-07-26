import csv
import pandas as pd
file_path = 'data/csvs/WTA (Womens)/Odds/2023.xlsx'

data = pd.read_excel(file_path)

vegasCorrect = 0
vegasWrong = 0
splitOdds = 0

favoriteTotalUnits = 0.0
underdogTotalUnits = 0.0
# print(len(data))

right = 0
total = 0

for i in range(1, len(data)):
    # print(data.loc[i, 'AvgW'], end = " vs. ")
    # print(data.loc[i, 'AvgL'])

    # Favorite Wins
    total += 1
    if 2 >= float(data.loc[i, 'AvgW']) or 2 >= float(data.loc[i, 'AvgL']):
        if(float(data.loc[i, 'AvgW']) < float(data.loc[i, 'AvgL'])):
            vegasCorrect += 1

            underdogTotalUnits -= 1
            favoriteTotalUnits += (data.loc[i, 'AvgW'] - 1)
        elif(float(data.loc[i, 'AvgL']) < float(data.loc[i, 'AvgW'])):
            vegasWrong += 1

            underdogTotalUnits += (data.loc[i, 'AvgW'] - 1)
            favoriteTotalUnits -= 1
        else:
            splitOdds += 1

            
print("Favorite won " + str((float(vegasCorrect)/(vegasCorrect+vegasWrong))*100) + "% of the time")
print("Favorite Units: " + str(favoriteTotalUnits))
print("Underdog Units: " + str(underdogTotalUnits))
print("From a total of: " + str(total) + " $1 bets")
# print(right)