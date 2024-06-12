import csv
import pandas as pd

file_path = '2023 Womens Complete.csv'

data = pd.read_csv(file_path)

vegasCorrect = 0
vegasWrong = 0
splitOdds = 0

favoriteTotalUnits = 0.0
underdogTotalUnits = 0.0
# print(len(data))

right = 0

for i in range(1, len(data)):
    # print(data.loc[i, 'AvgW'], end = " vs. ")
    # print(data.loc[i, 'AvgL'])

    # Favorite Wins
    if(float(data.loc[i, 'AvgW']) < float(data.loc[i, 'AvgL'])):
        vegasCorrect += 1

        if(float(data.loc[i, 'LRank'])<20):
            underdogTotalUnits -= 1
            right += 1

        # underdogTotalUnits -= 1
        favoriteTotalUnits += (data.loc[i, 'AvgW'] - 1)
    elif(float(data.loc[i, 'AvgL']) < float(data.loc[i, 'AvgW'])):
        vegasWrong += 1

        if(float(data.loc[i, 'WRank'])<20):
            underdogTotalUnits += (data.loc[i, 'AvgW'] - 1)
            right += 1

        # underdogTotalUnits += (data.loc[i, 'AvgW'] - 1)
        favoriteTotalUnits -= 1
    else:
        splitOdds += 1
print(float(vegasCorrect)/(vegasCorrect+vegasWrong))
print("Favorite Units: " + str(favoriteTotalUnits))
print("Underdog Units: " + str(underdogTotalUnits))
# print(right)