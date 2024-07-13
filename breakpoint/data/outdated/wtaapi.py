import functools, json
from requests_html import AsyncHTMLSession
import pandas as pd
import requests
from sqlalchemy import create_engine
import logging


devengine = create_engine("sqlite:///C:/Git/tennis_atp/database/bets_sqllite.db")


async def async_get(url, id):
    # r = await Pool.get(url, headers={"user-agent": ""})

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
    }
    player_API = requests.get(
        "https://api.wtatennis.com/tennis/players/{}/year/2022".format(id),
        headers={"user-agent": ""},
    )
    player_API = await Pool.get(
        "https://api.wtatennis.com/tennis/players/{}/year/2022".format(id),
        headers={"user-agent": ""},
    )
    player_txt = player_API.text

    player_json = json.loads(player_txt)
    if player_API.ok == False:
        return {
            "Name": None,
            **{
                "Service Games Won": "{}".format(0),
                "Return Games Won": "{}".format(0),
            },
        }
    else:
        name = player_json["player"]["fullName"]
        if "stats" in player_json:
            service_games = player_json["stats"]["service_games_won_percent"]
            return_games = player_json["stats"]["return_games_won_percent"]
        else:
            # print(f"No info found for {name}")
            service_games = 0
            return_games = 0
        return {
            "Name": name,
            **{
                "Service Games Won": "{}".format(service_games),
                "Return Games Won": "{}".format(return_games),
            },
        }


data = pd.DataFrame()
todays_matches = pd.read_sql_query(
    "Select Time,Player_1, Player_2, Player_1_Odds, Player_2_Odds from TodaysMatches where resulted = 'False' and Sex='Womens'",
    con=devengine,
)
if todays_matches.empty == True:
    print("No Matches Today")
    blankdf = pd.DataFrame()
    blankdf.to_excel("servers_today_womens.xlsx", index=False)
else:
    for x in range(0, 5):
        print(x)

        response_API = requests.get(
            "https://api.wtatennis.com/tennis/players/ranked?page={}&pageSize=500&type=rankSingles&sort=asc&name=&metric=SINGLES&at=2022-11-30&nationality=".format(
                x
            ),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"
            },
        )

        response_txt = response_API.text
        response_json = json.loads(response_txt)
        url_list = []
        for player in response_json:
            player_fname = player["player"]["firstName"]
            player_lname = player["player"]["lastName"]
            player_id = player["player"]["id"]
            url = "https://www.wtatennis.com/players/{}/{}-{}#stats".format(
                player_id, player_fname, player_lname
            )
            url_list.append(url)

        Pool = AsyncHTMLSession()

        results = Pool.run(
            *(functools.partial(async_get, tag, tag.split("/")[4]) for tag in url_list)
        )
        serve_return_stats = pd.read_json(json.dumps(results, indent=2))
        data = pd.concat([data, serve_return_stats])
    name_dict = pd.read_csv(r"C:\Git\tennis_atp\name_lookup_serving.csv")
    for _, item in name_dict.iterrows():
        data["Name"] = data["Name"].str.replace(item.old, item.new, regex=True)

    cols_to_convert = ["Service Games Won", "Return Games Won"]
    data[cols_to_convert] = data[cols_to_convert].astype(float) / 100
    # Round the converted values to 2 decimal places
    data[cols_to_convert] = data[cols_to_convert].round(2)

    data.to_csv("womensserving.csv", index=False)
    todays_matches = pd.read_sql_query(
        "Select Time,Player_1, Player_2, Player_1_Odds, Player_2_Odds from TodaysMatches where resulted = 'False' and Sex='Womens'",
        con=devengine,
    )

    todays_matches["Fav"] = todays_matches.apply(
        lambda x: x["Player_1"]
        if x["Player_2_Odds"] > x["Player_1_Odds"]
        else (x["Player_2"] if x["Player_2_Odds"] < x["Player_1_Odds"] else "Pickem"),
        axis=1,
    )
    todays_matches["Dog"] = todays_matches.apply(
        lambda x: x["Player_1"]
        if x["Player_2_Odds"] < x["Player_1_Odds"]
        else (x["Player_2"] if x["Player_2_Odds"] > x["Player_1_Odds"] else "Pickem"),
        axis=1,
    )

    combine = pd.merge(todays_matches, data, how="left", left_on="Fav", right_on="Name")
    combine2 = pd.merge(combine, data, how="left", left_on="Dog", right_on="Name")
    combine2[["Service Games Won_x", "Service Games Won_y"]] = combine2[
        ["Service Games Won_x", "Service Games Won_y"]
    ].astype(float)
    combine2.rename(
        columns={
            "Service Games Won_x": "Fav_Serve%",
            "Service Games Won_y": "Dog_Serve%",
            "Return Games Won_x": "Fav_Return%",
            "Return Games Won_y": "Dog_Return%",
        },
        inplace=True,
    )
    combine2 = combine2[
        [
            "Time",
            "Fav",
            # "Player_1_Odds",
            "Fav_Serve%",
            "Dog_Return%",
            "Dog",
            # "Player_2_Odds",
            "Dog_Serve%",
            "Fav_Return%",
        ]
    ]
    combine2.sort_values(by="Time").to_excel("servers_today_womens.xlsx", index=False)
