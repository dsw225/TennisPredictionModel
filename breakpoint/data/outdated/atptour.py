import functools, json
from requests_html import AsyncHTMLSession, HTMLSession
import pandas as pd
from sqlalchemy import create_engine

devengine = create_engine("sqlite:///C:/Git/tennis_atp/database/bets_sqllite.db")


async def async_get(url):
    replace_url = url.replace("overview", "player-stats?year=2022&surfaceType=all")
    r = await Pool.get(replace_url, headers={"user-agent": ""})
    name = r.html.find(".player-profile-hero-name", first=True).text.replace("\n", " ")
    for row in r.html.find(".mega-table")[:-1]:
        service_games = row.find("td")[17].text
    for row in r.html.find(".mega-table"):
        return_games = row.find("td")[11].text
    return {
        "Name": name,
        **{
            "Service Games Won": service_games.replace("%", ""),
            "Return Games Won": return_games.replace("%", ""),
        },
    }


url = "https://www.atptour.com/en/rankings/singles?rankRange=1-600&rankDate=2023-01-02"

r = HTMLSession().get(url, headers={"user-agent": ""})
url_list = []
for tag in r.html.find(".player-cell-wrapper"):
    if len(tag.absolute_links) > 1:
        temp = list(tag.absolute_links)
        if "topcourt" in temp[0]:
            url_list.append(temp[1])
        else:
            url_list.append(temp[0])

    else:
        poo = list(tag.absolute_links)
        url_list.append(poo[0])

Pool = AsyncHTMLSession()

results = Pool.run(*(functools.partial(async_get, tag) for tag in url_list))
serve_return_stats = pd.read_json(json.dumps(results, indent=2))
serve_return_stats["Name"] = (
    serve_return_stats["Name"]
    .str.replace("de Minaur", "De Minaur")
    .str.replace("Auger-Aliassime", "Auger Aliassime")
    .str.replace("McDonald", "Mcdonald")
    .str.replace("Ramos-Vinolas", "Ramos Vinolas")
)
name_dict = pd.read_csv(r"C:\Git\tennis_atp\name_lookup_serving.csv")
for _, item in name_dict.iterrows():
    serve_return_stats["Name"] = serve_return_stats["Name"].str.replace(
        item.old, item.new, regex=True
    )
todays_matches = pd.read_sql_query(
    "Select Time,Player_1, Player_2, Player_1_Odds, Player_2_Odds from TodaysMatches where resulted = 'False' and Sex='Mens'",
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

combine = pd.merge(
    todays_matches, serve_return_stats, how="left", left_on="Fav", right_on="Name"
)
combine2 = pd.merge(
    combine, serve_return_stats, how="left", left_on="Dog", right_on="Name"
)
combine2[["Service Games Won_x", "Service Games Won_y"]] = combine2[
    ["Service Games Won_x", "Service Games Won_y"]
].astype(float)
combine2 = combine2[
    (
        ((combine2["Service Games Won_x"]).ge(1))
        # & ((combine2["Service Games Won_y"]).ge(1))
    )
    | (
        ((combine2["Service Games Won_y"]).ge(1))
        # & ((combine2["Service Games Won_x"]).ge(1))
    )
]
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
combine2.sort_values(by="Time").to_excel("servers_today.xlsx", index=False)

cols_to_convert = ["Service Games Won", "Return Games Won"]
serve_return_stats[cols_to_convert] = (
    serve_return_stats[cols_to_convert].astype(float) / 100
)
serve_return_stats.to_csv("mensserving.csv", index=False)
