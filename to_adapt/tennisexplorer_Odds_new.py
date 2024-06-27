import datetime
import requests
from bs4 import BeautifulSoup
import argparse
import pandas as pd
from sqlalchemy import create_engine
from playsound import playsound
from git.repo import Repo


repo = Repo(r"C:\Git\tennis_atp")
origin = repo.remotes[0]
origin.pull()

devengine = create_engine("sqlite:///C:/Git/tennis_atp/database/bets_sqllite.db")
connection = devengine.connect()


def Main(url, current_date, suffix, check):
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="count", default=0)

    args = parser.parse_args()
    response = requests.get(url)

    # # Analysis with beautifulsoup
    soup = BeautifulSoup(response.content, "html.parser")

    table = soup.find("table", {"class": "result"})
    table = soup.findAll("table", {"class": "result"})[1]
    if check == 1:
        table = soup.find("table", {"class": "result"})
    table_body = table.find("tbody")

    rows = table_body.find_all("tr")
    tournament_idx_lst = []
    for i, row in enumerate(rows):
        if '<tr class="head flags">' in str(row):
            t_name = row.find("td", {"class": "t-name"}).text
            tournament_idx_lst.append(i)

    tournament_idx_lst.append(len(rows))

    def getPlayersFullName(playerUrl):
        player_url = "https://www.tennisexplorer.com" + playerUrl
        player_response = requests.get(player_url)
        player_soup = BeautifulSoup(player_response.content, "html.parser")
        player_table = player_soup.find("table", {"class": "plDetail"})
        player_table_body = player_table.find("tbody")
        try:
            player_rank = player_table_body.text.split(
                "Current/Highest rank - singles: "
            )[1].split(".")[0]
            player_rank_high = player_table_body.text.split(
                "Current/Highest rank - singles: "
            )[1].split(".")[1]
            if "-" in player_rank:
                player_rank = "10000"
        except:
            player_rank = "10000"
            player_rank_high = "10000"
        try:
            player_hand = player_table_body.text.split("Plays: ")[1].split(".")[0]
        except:
            player_hand = "right"
        player_name = player_table_body.find_all("h3")
        name = " ".join(player_name[0].text.split())
        splitname = name.split(" ")
        first_name = splitname[-1]
        last_name = name.replace(" " + first_name, "")
        name = first_name + " " + last_name
        name_dict = pd.read_csv(r"C:\Git\tennis_atp\name_lookup.csv")
        for _, item in name_dict.iterrows():
            name = name.replace(item.old, item.new)
        try:
            highrank = player_rank_high.split("/ ")[1]
        except:
            highrank = str(10000)
        return (
            name.strip().replace("-", " ")
            + "("
            + player_rank
            + ")"
            + " ["
            + highrank
            + "]"
        )

    tournament_dict = {}
    for i, item in enumerate(tournament_idx_lst[:-1]):
        tournament_name = rows[item].find("td", class_="t-name").text.strip()
        if (
            "Futures" not in tournament_name
            and "ITF" not in tournament_name
            and "Davis Cup" not in tournament_name
            and "UTR" not in tournament_name
            and "UK Pro" not in tournament_name
        ):
            tournament_url = (
                rows[item].find("td", class_="t-name").contents[0].attrs["href"]
            )
            tournament_url = "https://www.tennisexplorer.com" + tournament_url.replace(
                "'", ""
            )
            response = requests.get(tournament_url)

            # # Analysis with beautifulsoup
            soup = BeautifulSoup(response.content, "html.parser")
            court_type = (
                soup.find("div", {"id": "center"})
                .text.split("\n")[2]
                .split(")")[0]
                .split(",")[-2]
                .strip()
            )
            court_type = (
                court_type.replace("indoors", "Hard")
                .replace("clay", "Clay")
                .replace("hard", "Hard")
            )

            if not "Futures" in tournament_name:
                if not tournament_dict.get(tournament_name):
                    tournament_dict[tournament_name + str(item)] = {}
                    tournament_dict[tournament_name + str(item)][current_date] = []
                for c in range(item + 1, tournament_idx_lst[i + 1], 2):
                    test = rows[c].findAll("td", class_="course")
                    # print(len(test))
                    if len(test) > 1:
                        tournament_dict[tournament_name + str(item)][
                            current_date
                        ].append(
                            getPlayersFullName(
                                rows[c].find("td", class_="t-name").a["href"]
                            )
                            + " vs "
                            + getPlayersFullName(
                                rows[c + 1].find("td", class_="t-name").a["href"]
                            )
                            + ":"
                            + court_type
                            + ":"
                            + rows[c].findAll("td", class_="course")[0].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="course")[1].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="result")[0].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="result")[1].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="score")[0].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="score")[1].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="score")[2].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="score")[3].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="score")[4].contents[0]
                            + "_"
                            + rows[c + 1].findAll("td", class_="score")[0].contents[0]
                            + "_"
                            + rows[c + 1].findAll("td", class_="score")[1].contents[0]
                            + "_"
                            + rows[c + 1].findAll("td", class_="score")[2].contents[0]
                            + "_"
                            + rows[c + 1].findAll("td", class_="score")[3].contents[0]
                            + "_"
                            + rows[c + 1].findAll("td", class_="score")[4].contents[0]
                        )
                    else:
                        tournament_dict[tournament_name + str(item)][
                            current_date
                        ].append(
                            getPlayersFullName(
                                rows[c].find("td", class_="t-name").a["href"]
                            )
                            + " vs "
                            + getPlayersFullName(
                                rows[c + 1].find("td", class_="t-name").a["href"]
                            )
                            + ":"
                            + court_type
                            + ":"
                            + rows[c].findAll("td", class_="coursew")[0].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="course")[0].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="result")[0].contents[0]
                            + "_"
                            + rows[c + 1].findAll("td", class_="result")[0].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="score")[0].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="score")[1].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="score")[2].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="score")[3].contents[0]
                            + "_"
                            + rows[c].findAll("td", class_="score")[4].contents[0]
                            + "_"
                            + rows[c + 1].findAll("td", class_="score")[0].contents[0]
                            + "_"
                            + rows[c + 1].findAll("td", class_="score")[1].contents[0]
                            + "_"
                            + rows[c + 1].findAll("td", class_="score")[2].contents[0]
                            + "_"
                            + rows[c + 1].findAll("td", class_="score")[3].contents[0]
                            + "_"
                            + rows[c + 1].findAll("td", class_="score")[4].contents[0]
                        )

    for key, value in tournament_dict.items():
        # print(value)
        datefilename = current_date.replace("-", "")

        # with xlsxwriter.Workbook(r"C:\Users\chris\OneDrive\Desktop\Tennis\\" + key + datefilename + suffix + ".xlsx") as workbook:
        for i, date in value.items():
            for match in date:
                # print(match)
                match1 = match.split(":")[0]
                players = match1.split(" vs ")
                player1 = players[0].split("(")[0]
                player2 = players[1].split("(")[0]
                odds = match.split(":")[2]
                player1odds = odds.split("_")[0]
                player2odds = odds.split("_")[1]
                player1_rank = (
                    (players[0].split("(")[1]).replace(")", "").split(" [")[0]
                )
                player2_rank = (
                    (players[1].split("(")[1]).replace(")", "").split(" [")[0]
                )

                player1_rank_high = (players[0].split("[")[1]).replace("]", "")
                player2_rank_high = (players[1].split("[")[1]).replace("]", "")
                Surface = match.split(":")[1]
                winner_games = match.split("_")[2]
                loser_games = match.split("_")[3]
                winner_first = match.split("_")[4]
                winner_second = match.split("_")[5]
                winner_third = match.split("_")[6]
                winner_forth = match.split("_")[7]
                winner_fifth = match.split("_")[8]
                loser_first = match.split("_")[9]
                loser_second = match.split("_")[10]
                loser_third = match.split("_")[11]
                loser_forth = match.split("_")[12]
                loser_fifth = match.split("_")[13]
                table = [
                    [
                        "Date",
                        "Sex",
                        "Tournament",
                        "Player_1",
                        "Player_2",
                        "Player_1_Rank",
                        "Player_2_Rank",
                        "Player_1_Rank_High",
                        "Player_2_Rank_High",
                        "Player_1_Odds",
                        "Player_2_Odds",
                        "Surface",
                        "Winner_Sets",
                        "Loser_Sets",
                        "w1",
                        "w2",
                        "w3",
                        "w4",
                        "w5",
                        "l1",
                        "l2",
                        "l3",
                        "l4",
                        "l5",
                    ],
                    [
                        current_date,
                        suffix.replace("_", ""),
                        key,
                        player1,
                        player2,
                        player1_rank,
                        player2_rank,
                        player1_rank_high,
                        player2_rank_high,
                        player1odds,
                        player2odds,
                        Surface,
                        winner_games,
                        loser_games,
                        winner_first,
                        winner_second,
                        winner_third,
                        winner_forth,
                        winner_fifth,
                        loser_first,
                        loser_second,
                        loser_third,
                        loser_forth,
                        loser_fifth,
                    ],
                ]
                df = pd.DataFrame(table)
                headers = df.iloc[0]
                new_df = pd.DataFrame(df.values[1:], columns=headers)
                # filter=new_df['Elo Probability'].gt(0.5)|new_df['Estimated Odds Clay'].gt(0.5)|new_df['Estimated Odds Hard'].gt(0.5)
                # new_df=new_df[filter]
                new_df = new_df[
                    (new_df["Player_1_Odds"] != " ")
                    & (new_df["Player_1"] != "")
                    & (new_df["Player_2"] != "")
                ]
                new_df.to_sql(
                    "AllMatches",
                    con=devengine,
                    if_exists="append",
                    index=False,
                )


# for x in range(81,90):
for x in reversed(range(1, 2)):
    # for x in range(933, 1000):
    print(x)

    # connection.execute('Delete FROM Test_Yesterday')
    # connection.execute('Delete FROM bets_today')
    # # Get the current date
    tomorrow = datetime.datetime.now() + datetime.timedelta(days=-x)
    print(tomorrow.strftime("%Y-%m-%d"))
    year, month, day = tomorrow.year, tomorrow.month, tomorrow.day
    current_date = tomorrow.strftime("%Y-%m-%d")
    # print('https://www.tennisexplorer.com/matches/?type=atp-single&year={}&month={}&day={}'.format(year, month, day))

    Main(
        "https://www.tennisexplorer.com/matches/?type=atp-single&year={}&month={}&day={}&timezone=+10".format(
            year, month, day
        ),
        current_date,
        "_Mens",
        1,
    )

    Main(
        "https://www.tennisexplorer.com/matches/?type=wta-single&year={}&month={}&day={}&timezone=+10".format(
            year, month, day
        ),
        current_date,
        "_Womens",
        1,
    )

    Main(
        "https://www.tennisexplorer.com/matches/?type=atp-single&year={}&month={}&day={}&timezone=+10".format(
            year, month, day
        ),
        current_date,
        "_Mens",
        0,
    )
    Main(
        "https://www.tennisexplorer.com/matches/?type=wta-single&year={}&month={}&day={}&timezone=+10".format(
            year, month, day
        ),
        current_date,
        "_Womens",
        0,
    )

# playsound(r"C:\Users\chris\Music\beep-09.mp3")

repo.index.add([r"C:\Git\tennis_atp\database\bets_sqllite.db"])
repo.index.commit("commit from python")


origin.push()
