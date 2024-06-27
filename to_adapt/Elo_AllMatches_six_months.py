import datetime
import pandas as pd
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import logging
from playsound import playsound
import datetime
from dateutil.relativedelta import *

origin = repo.remotes[0]
origin.pull()

devengine = create_engine("sqlite:///C:/Git/tennis_atp/database/bets_sqllite.db")

date_today = datetime.datetime.now() + relativedelta(days=0)
date_six_months_ago = date_today + relativedelta(months=-24)
# date_six_months_ago = date_today + relativedelta(years=-1, months=-1)


date_six_months_ago_formatted = date_six_months_ago.strftime("%Y-%m-%d")
# print(date_six_months_ago_formatted)


def Elo(surface):
    data = pd.read_sql_query(
        "Select distinct Player_1_Rank_High as Winner_Rank_High,Player_2_Rank_High as Loser_Rank_High, Surface,Date,Sex,Player_1 as Winner, Player_2 as Loser, Player_1_Odds as Winner_Odds, Player_2_Odds as Loser_Odds,Player_1_Rank as Winner_Rank, Player_2_Rank as Loser_Rank FROM AllMatches where surface like '{}' and tournament not like '%UK Pro%'  and tournament not like '%UTR%' and tournament not like '%Davis%' and date >='{}' and Winner_Sets>1".format(
            surface, date_six_months_ago
        ),
        con=devengine,
    )

    data2 = pd.read_sql_query(
        f"Select distinct Player_1_Rank_High as Winner_Rank_High,Player_2_Rank_High as Loser_Rank_High,Surface,Date,Sex,Player_1 as Winner, Player_2 as Loser, Player_1_Odds as Winner_Odds, Player_2_Odds as Loser_Odds,Resulted,Time,Player_1_Rank as Winner_Rank, Player_2_Rank as Loser_Rank FROM TodaysMatches where surface like '{surface}' and tournament not like '%UK Pro%'  and tournament not like '%UTR%' and tournament not like '%Davis%' ",
        con=devengine,
    )
    data = pd.concat([data, data2])

    data = data.sort_values("Date")  # sort data frame by date
    data["Surface"].str.replace("'b", "")  # drop the b' prefix from the Surface column
    data["Winner"] = data[
        "Winner"
    ].str.strip()  # remove leading and trailing whitespaces from names
    data["Loser"] = data["Loser"].str.strip()
    data = data.reset_index(drop=True)

    def get_elo_rankings(data):
        """
        Function that given the list on matches in chronological order, for each match, computes
        the elo ranking of the 2 players at the beginning of the match.

        Parameters: data(pandas DataFrame) - DataFrame that contains needed information on tennis matches, e.g players names,
        winners, losesrs , surfaces etc

        Return: elo_ranking(pandas DataFrame) - DataFrame that contains the calculated Elo Ratings and the Pwin.

        """
        players = list(
            pd.Series(list(data.Winner) + list(data.Loser)).value_counts().index
        )  # create list of all players
        elo = pd.Series(
            np.ones(len(players)) * 1500, index=players
        )  # create series with initialised elo rating for all players
        matches_played = pd.Series(
            np.zeros(len(players)), index=players
        )  # create series with players' matches initialised at 0 and updated after each match
        ranking_elo = [(1500, 1500)]  # create initial elo's list
        for i in range(1, len(data)):
            w = data.iloc[i - 1, :].Winner  # identify winning player
            l = data.iloc[i - 1, :].Loser  # identify losing player
            elow = elo[w]
            elol = elo[l]
            matches_played_w = matches_played[w]
            matches_played_l = matches_played[l]
            pwin = 1 / (
                1 + 10 ** ((elol - elow) / 400)
            )  # compute prob of winner to win
            ploss = 1 / (
                1 + 10 ** ((elow - elol) / 400)
            )  # compute prob of winner to win
            K_win = 250 / ((matches_played_w + 5) ** 0.4)  # K-factor of winning player
            K_los = 250 / ((matches_played_l + 5) ** 0.4)  # K-factor of losing player
            new_elow = elow + K_win * (1 - pwin)  # winning player new elo
            new_elol = elol - K_los * (1 - pwin)  # losing player new elo
            elo[w] = new_elow
            elo[l] = new_elol
            matches_played[w] += 1  # update total matches of players
            matches_played[l] += 1
            ranking_elo.append(
                (elo[data.iloc[i, :].Winner], elo[data.iloc[i, :].Loser])
            )

        ranking_elo = pd.DataFrame(ranking_elo, columns=["Elo_Winner", "Elo_Loser"])
        ranking_elo["Prob_Elo"] = 1 / (
            1 + 10 ** ((ranking_elo["Elo_Loser"] - ranking_elo["Elo_Winner"]) / 400)
        )
        ranking_elo["Prob_Elo_Loser"] = 1 / (
            1 + 10 ** ((ranking_elo["Elo_Winner"] - ranking_elo["Elo_Loser"]) / 400)
        )
        return ranking_elo

    elo_rankings = get_elo_rankings(data)
    data = pd.concat([data, elo_rankings], axis=1)

    def get_prob(a):
        """Function that convert decimal odds to probabilities.
        Parameters: a - decimal odd (float)
        Return: a - probability (float)
        """
        a = 1 / a
        return a

    data["Elo_Fav"] = data.apply(
        lambda x: x["Winner"] if x["Elo_Winner"] > x["Elo_Loser"] else x["Loser"],
        axis=1,
    )
    data["Elo_Dog"] = data.apply(
        lambda x: x["Winner"] if x["Elo_Winner"] < x["Elo_Loser"] else x["Loser"],
        axis=1,
    )
    data["Elo_Fav_Odds"] = data.apply(
        lambda x: x["Winner_Odds"]
        if x["Elo_Winner"] > x["Elo_Loser"]
        else x["Loser_Odds"],
        axis=1,
    )
    data["Elo_Dog_Odds"] = data.apply(
        lambda x: x["Loser_Odds"]
        if x["Elo_Fav_Odds"] == x["Winner_Odds"]
        else x["Winner_Odds"],
        axis=1,
    )
    data["Elo_Fav_Est_Odds"] = data.apply(
        lambda x: 1 / x["Prob_Elo"]
        if x["Elo_Fav_Odds"] == x["Winner_Odds"]
        else 1 / x["Prob_Elo_Loser"],
        axis=1,
    )
    data["Elo_Dog_Est_Odds"] = data.apply(
        lambda x: 1 / x["Prob_Elo_Loser"]
        if x["Elo_Fav_Odds"] == x["Winner_Odds"]
        else 1 / x["Prob_Elo"],
        axis=1,
    )
    tomorrow = datetime.datetime.now() + datetime.timedelta(days=0)
    current_date = tomorrow.strftime("%Y-%m-%d")
    data[
        ["Elo_Fav_Odds", "Elo_Dog_Odds", "Elo_Fav_Est_Odds", "Elo_Dog_Est_Odds"]
    ] = data[
        ["Elo_Fav_Odds", "Elo_Dog_Odds", "Elo_Fav_Est_Odds", "Elo_Dog_Est_Odds"]
    ].astype(
        "float"
    )
    """
    data["Wins"] = data.groupby("Winner").cumcount() + 1
    data["Losses"] = data.groupby("Loser").cumcount() + 1
    data2 = data.copy(deep=True)
    dataloser = data.copy(deep=True)
    data2["Winner"] = data2["Loser"]
    data3 = pd.concat([data, data2]).sort_values("Date")
    data3.reset_index(drop=True, inplace=True)
    data3["WinnerTotal"] = data3.groupby("Winner").cumcount() + 1
    data3 = data3[pd.notnull(data3["Surface"])]
    data4 = data3.merge(
        data,
        how="inner",
        left_on=["Date", "Winner", "Loser"],
        right_on=["Date", "Winner", "Loser"],
    )
    dataloser["Loser"] = dataloser["Winner"]
    data9 = pd.concat([data, dataloser]).sort_values("Date")
    data9.reset_index(drop=True, inplace=True)
    data9["LoserTotal"] = data9.groupby("Loser").cumcount() + 1
    data = data9.merge(
        data4,
        how="inner",
        left_on=["Date", "Winner", "Loser"],
        right_on=["Date", "Winner", "Loser"],
    )

    # data1 = data[data["Date"] != current_date]
    data1 = data
    data = data1
    """
    data = data[data.columns.drop(list(data.filter(regex="_y")))]
    data = data[data.columns.drop(list(data.filter(regex="_x")))]
    """
    data = data.drop(
        columns=["Prob_Elo", "Prob_Elo_Loser"],
        axis=1,
    )
    """
    data["Elo_Fav_Elo"] = data.apply(
        lambda x: x["Elo_Winner"] if x["Winner"] == x["Elo_Fav"] else x["Elo_Loser"],
        axis=1,
    )
    data["Elo_Dog_Elo"] = data.apply(
        lambda x: x["Elo_Winner"] if x["Winner"] != x["Elo_Fav"] else x["Elo_Loser"],
        axis=1,
    )
    """
    data["Elo_Fav_Total"] = data.apply(
        lambda x: x["WinnerTotal"] if x["Winner"] == x["Elo_Fav"] else x["LoserTotal"],
        axis=1,
    )
    data["Elo_Dog_Total"] = data.apply(
        lambda x: x["WinnerTotal"] if x["Winner"] != x["Elo_Fav"] else x["LoserTotal"],
        axis=1,
    )
    """

    data["Elo_Fav_Rank"] = data.apply(
        lambda x: x["Winner_Rank"] if x["Winner"] == x["Elo_Fav"] else x["Loser_Rank"],
        axis=1,
    ).astype(float)
    data["Elo_Dog_Rank"] = data.apply(
        lambda x: x["Winner_Rank"] if x["Winner"] != x["Elo_Fav"] else x["Loser_Rank"],
        axis=1,
    ).astype(float)

    data["Fav"] = data.apply(
        lambda x: x["Winner"]
        if x["Loser_Odds"] > x["Winner_Odds"]
        else (x["Loser"] if x["Loser_Odds"] < x["Winner_Odds"] else "Pickem"),
        axis=1,
    )
    data["Dog"] = data.apply(
        lambda x: x["Winner"]
        if x["Loser_Odds"] < x["Winner_Odds"]
        else (x["Loser"] if x["Loser_Odds"] > x["Winner_Odds"] else "Pickem"),
        axis=1,
    )

    data["Fav_Odds"] = data.apply(
        lambda x: x["Winner_Odds"]
        if x["Loser_Odds"] > x["Winner_Odds"]
        else (
            x["Loser_Odds"] if x["Loser_Odds"] < x["Winner_Odds"] else x["Loser_Odds"]
        ),
        axis=1,
    )
    data["Dog_Odds"] = data.apply(
        lambda x: x["Winner_Odds"]
        if x["Loser_Odds"] < x["Winner_Odds"]
        else (
            x["Loser_Odds"] if x["Loser_Odds"] > x["Winner_Odds"] else x["Loser_Odds"]
        ),
        axis=1,
    )
    data["Fav_Rank"] = data.apply(
        lambda x: x["Winner_Rank"] if x["Winner"] == x["Fav"] else x["Loser_Rank"],
        axis=1,
    ).astype(float)
    data["Dog_Rank"] = data.apply(
        lambda x: x["Winner_Rank"] if x["Winner"] != x["Fav"] else x["Loser_Rank"],
        axis=1,
    ).astype(float)
    data["Fav_Rank_High"] = data.apply(
        lambda x: x["Winner_Rank_High"]
        if x["Winner"] == x["Fav"]
        else x["Loser_Rank_High"],
        axis=1,
    ).astype(float)
    data["Dog_Rank_High"] = data.apply(
        lambda x: x["Winner_Rank_High"]
        if x["Winner"] != x["Fav"]
        else x["Loser_Rank_High"],
        axis=1,
    ).astype(float)
    data = data[data["Fav"] != "Pickem"]
    data = data.drop(
        columns=[
            "Elo_Loser",
            "Elo_Winner",
            "Winner_Rank",
            "Loser_Rank",
            "Winner_Rank_High",
            "Loser_Rank_High",
        ],
        axis=1,
    )
    data = data[
        (data["Fav_Rank"] < 1000)
        & (data["Dog_Rank"] < 1000)
        & (data["Elo_Fav_Elo"].ne(1500))
        & (data["Elo_Dog_Elo"].ne(1500))
    ]
    if surface == "%":
        surface = "All"
    data.to_sql(
        f"Elo_AllMatches_{surface}", con=devengine, if_exists="replace", index=False
    )
    # data2 = data[data["Date"] == current_date]
    # data2.to_sql("Elo_AllMatches_Today", con=devengine, if_exists="replace", index=False)
    # playsound(r"C:\Users\chris\Music\beep-09.mp3")


# Elo("Hard")
# Elo("Clay")
