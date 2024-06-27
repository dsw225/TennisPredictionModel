# Import necessary libraries
def analysis():
    from sqlalchemy import create_engine
    import pandas as pd
    import numpy as np
    import datetime

    def get_match_data(start_date, time_now_formatted, devengine):
        # Get historical match data on hard surface between start date and yesterday
        elo_hard = pd.read_sql_query(
            f"Select DISTINCT * From Elo_AllMatches_Hard where Date > '{start_date}' and Date not like '{time_now_formatted}'",
            con=devengine,
        )

        # Get historical match data on clay surface between start date and yesterday
        elo_clay = pd.read_sql_query(
            f"Select DISTINCT * From Elo_AllMatches_Clay where Date > '{start_date}' and Date not like '{time_now_formatted}'",
            con=devengine,
        )
        # Get historical match data on clay surface between start date and yesterday
        elo_grass = pd.read_sql_query(
            f"Select DISTINCT * From Elo_AllMatches_grass where Date > '{start_date}' and Date not like '{time_now_formatted}'",
            con=devengine,
        )
        # Get today's matches on hard surface that haven't yet been resulted
        elo_data_hard = pd.read_sql_query(
            f"Select DISTINCT * From Elo_AllMatches_Hard where Date like '{time_now_formatted}' --and resulted like 'False'",
            con=devengine,
        )

        # Get today's matches on clay surface that haven't yet been resulted
        elo_data_clay = pd.read_sql_query(
            f"Select DISTINCT * From Elo_AllMatches_Clay where Date like '{time_now_formatted}' --and resulted like 'False'",
            con=devengine,
        )

        return elo_hard, elo_clay, elo_data_hard, elo_data_clay

    # Connect to SQLite database using SQLAlchemy's create_engine
    devengine = create_engine("sqlite:///C:/Git/tennis_atp/database/bets_sqllite.db")
    # Get current date and time
    time_now = datetime.datetime.now()

    # Format current date as string in YYYY-MM-DD format
    time_now_formatted = time_now.strftime("%Y-%m-%d")

    # Get the start date two years ago from today
    today = time_now
    two_years_ago = (today - datetime.timedelta(days=365 * 2)).strftime("%Y-%m-%d")

    (
        elo_hard,
        elo_clay,
        elo_data_hard,
        elo_data_clay,
    ) = get_match_data(two_years_ago, time_now_formatted, devengine)

    def get_player_record(player, opponent_rank, history, range_low, range_high, auto):
        if auto:
            opponent_rank_low = opponent_rank - range_low
            opponent_rank_high = opponent_rank + range_high
        else:
            opponent_rank_low = range_low
            opponent_rank_high = range_high

        player_history = history[
            (
                (history["Fav"] == player)
                & (
                    (history["Dog_Rank"] > opponent_rank_low)
                    & (history["Dog_Rank"] < opponent_rank_high)
                )
            )
            | (
                (history["Dog"] == player)
                & (
                    (history["Fav_Rank"] > opponent_rank_low)
                    & (history["Fav_Rank"] < opponent_rank_high)
                )
            )
        ]
        if player_history.empty == False:
            result = float(
                len(player_history[player_history["Winner"] == player])
                / len(player_history)
            )
            return result, len(player_history)
        else:
            return 0, 0

    def get_filtered_data(elo_data, elo):
        result_df = pd.DataFrame()
        for _, row in elo_data.sort_values(by="Time").iterrows():
            low_limit = 50
            high_limit = 50

            fav_percent, games = get_player_record(
                row.Fav, row.Dog_Rank, elo, low_limit, high_limit, True
            )
            count = 0
            while games < 10 and count < 200:
                count = count + 1
                low_limit = low_limit  # + 10
                high_limit = high_limit  # + 10
                fav_percent, games = get_player_record(
                    row.Fav, row.Dog_Rank, elo, low_limit, high_limit, True
                )

            low_limit = 50
            high_limit = 50
            dog_percent, games2 = get_player_record(
                row.Dog, row.Fav_Rank, elo, low_limit, high_limit, True
            )
            count = 0
            while games2 < 10 and count < 200:
                count = count + 1
                low_limit = low_limit  # + 10
                high_limit = high_limit  # + 10
                dog_percent, games2 = get_player_record(
                    row.Dog, row.Fav_Rank, elo, low_limit, high_limit, True
                )

            # New code to calculate player's record against rank 0 to 100
            fav_record, _ = get_player_record(row.Fav, 100, elo, 0, 100, False)
            dog_record, _ = get_player_record(row.Dog, 100, elo, 0, 100, False)

            if games > 0 and games2 > 0:
                temp_df = pd.DataFrame(
                    {
                        "Time": [row.Time],
                        "Fav_Odds": [row.Fav_Odds],
                        "Dog_Odds": [row.Dog_Odds],
                        "Fav": [row.Fav],
                        "Elo_Fav": [row.Elo_Fav],
                        "Fav_Record": ["{:.0%}".format(fav_percent)],
                        "Fav_Games": [games],
                        "Dog": [row.Dog],
                        "Dog_Odds": [row.Dog_Odds],
                        "Dog_Record": ["{:.0%}".format(dog_percent)],
                        "Dog_Games": [games2],
                        "fav_percent": [fav_percent],
                        "dog_percent": [dog_percent],
                        "Sex": [row.Sex],
                        "Resulted": [row.Resulted],
                        "fav_rank": [row.Fav_Rank],
                        "fav_rank_high": [row.Fav_Rank_High],
                        "dog_rank": [row.Dog_Rank],
                        "dog_rank_high": [row.Dog_Rank_High],
                        "Elo_Fav_Elo": [row.Elo_Fav_Elo],
                        "Elo_Dog_Elo": [row.Elo_Dog_Elo],
                        "Fav_Top100": [round(fav_record, 1)],  # New column
                        "Dog_Top100": [round(dog_record, 1)],  # New column
                    }
                )
                result_df = pd.concat([result_df, temp_df])
        return result_df

    results_hard = get_filtered_data(elo_data_hard, elo_hard)
    results_clay = get_filtered_data(elo_data_clay, elo_clay)

    def process_serving_data(result_df):
        try:
            # Try to read the 'servers_today.xlsx' file
            serving = pd.read_csv("mensserving.csv")

            # Try to read the 'servers_today_womens.xlsx' file
            serving_womens = pd.read_csv("womensserving.csv")

            # If 'serving_womens' dataframe is not empty, concatenate with 'serving' dataframe
            if serving_womens.empty == False:
                serving = pd.concat([serving, serving_womens])
            else:
                serving = serving

            # Drop the 'Time' column from the 'serving' dataframe
            # serving = serving.drop(columns='Time')
        except FileNotFoundError as e:
            # If either of the excel files is not found, print an error message and set serving to None
            print("The required excel file could not be found.")
            print("Error:", e)
            serving = None

        if serving is not None:
            try:
                # Try to merge the 'result_df' and 'serving' dataframes on the 'Fav' and 'Dog' columns
                result = pd.merge(
                    result_df, serving, how="left", left_on=["Fav"], right_on=["Name"]
                )
                result = pd.merge(
                    result, serving, how="left", left_on=["Dog"], right_on=["Name"]
                )
                result.rename(
                    columns={
                        "Service Games Won_x": "Fav_Serve%",
                        "Service Games Won_y": "Dog_Serve%",
                        "Return Games Won_x": "Fav_Return%",
                        "Return Games Won_y": "Dog_Return%",
                    },
                    inplace=True,
                )
                result.drop(columns=["Name_x", "Name_y"], inplace=True)

                # Set 'final_hard' to the 'result' dataframe
                result_serving = result
            except Exception as e:
                # If an error occurs during merging, print an error message and set both 'result' and 'final_hard' to None
                print("Error occured while merging the dataframes.")
                print("Error:", e)
                result = None
                result_serving = None
        else:
            result = None
            result_serving = None

        return result, result_serving

    _, serving_hard = process_serving_data(results_hard)
    _, serving_clay = process_serving_data(results_clay)

    data_concat = pd.DataFrame(columns=["Date", "Player", "Odds", "Win/Loss"])
    for dataset_type in [("Winner", "Win"), ("Loser", "Loss")]:
        df = elo_hard[["Date", dataset_type[0], f"{dataset_type[0]}_Odds"]].copy()
        df["Player"] = df[dataset_type[0]]
        df["Odds"] = df[f"{dataset_type[0]}_Odds"]
        df["Win/Loss"] = dataset_type[1]
        df.drop(columns=[dataset_type[0], f"{dataset_type[0]}_Odds"], inplace=True)
        data_concat = pd.concat([data_concat, df])
    data_concat = data_concat.sort_index()
    data_concat["Odds"] = data_concat.Odds.astype(float)

    def analyse_matchups(result_df, data_concat):
        for _, matchup in result_df.iterrows():
            player1 = matchup.Fav
            player2 = matchup.Dog
            player1_odds = float(matchup.Fav_Odds)
            player1_odds_hi = player1_odds + 0.15
            player1_odds_lo = player1_odds - 0.15
            player2_odds = float(matchup.Dog_Odds)
            player2_odds_hi = player2_odds + 0.15
            player2_odds_lo = player2_odds - 0.15
            player1 = data_concat[data_concat["Player"] == player1].copy()
            player2 = data_concat[data_concat["Player"] == player2].copy()
            player2 = player2[
                (player2["Odds"] > player2_odds_lo)
                & (player2["Odds"] < player2_odds_hi)
            ]
            if len(player2) > 0:
                winperc2 = len(player2[player2["Win/Loss"] == "Win"]) / len(player2)
            else:
                winperc2 = 0
            player1 = player1[
                (player1["Odds"] > player1_odds_lo)
                & (player1["Odds"] < player1_odds_hi)
            ]
            if len(player1) > 0:
                winperc1 = len(player1[player1["Win/Loss"] == "Win"]) / len(player1)
            else:
                winperc1 = 0
            if len(player1) > 5 and len(player2) > 3:
                print(
                    matchup.Time,
                    f"{matchup.Fav} ({round(player1_odds_lo,2)}-->{round(player1_odds_hi,2)})",
                    f"{matchup.Dog} ({round(player2_odds_lo,2)}-->{round(player2_odds_hi,2)})",
                )
                print(len(player1), winperc1, len(player2), winperc2)

    final_hard = serving_hard
    final_clay = serving_clay

    def last_five(df, pastmatches):
        for index, row in df.iterrows():
            fav = row.Fav
            dog = row.Dog
            last_five_matches_fav = pastmatches[
                (pastmatches["Winner"] == fav) | (pastmatches["Loser"] == fav)
            ].tail(5)
            if len(last_five_matches_fav) > 0:
                fav_last_five_win_perc = len(
                    last_five_matches_fav[last_five_matches_fav["Winner"] == fav]
                ) / len(last_five_matches_fav)
            else:
                fav_last_five_win_perc = 0
            last_five_matches_dog = pastmatches[
                (pastmatches["Winner"] == dog) | (pastmatches["Loser"] == dog)
            ].tail(5)
            if len(last_five_matches_dog) > 0:
                dog_last_five_win_perc = len(
                    last_five_matches_dog[last_five_matches_dog["Winner"] == dog]
                ) / len(last_five_matches_dog)
            else:
                dog_last_five_win_perc = 0
            df.at[index, "fav_last_five_win_perc"] = fav_last_five_win_perc
            df.at[index, "dog_last_five_win_perc"] = dog_last_five_win_perc
        return df

    if final_hard is not None:
        final_hard = last_five(final_hard, elo_hard)
        final_hard["Fav_Odds"] = final_hard["Fav_Odds"].astype(float)
        final_hard["Dog_Odds"] = final_hard["Dog_Odds"].astype(float)

    if final_clay is not None:
        final_clay = last_five(final_clay, elo_clay)
        final_clay["Dog_Odds"] = final_clay["Dog_Odds"].astype(float)
        final_clay["Fav_Odds"] = final_clay["Fav_Odds"].astype(float)

    for _, i in elo_data_hard.iterrows():
        check1 = elo_hard[
            ((elo_hard["Winner"] == i.Winner) & (elo_hard["Loser"] == i.Loser))
            | ((elo_hard["Loser"] == i.Winner) & (elo_hard["Winner"] == i.Loser))
        ]
        if check1.empty == False:
            for _, x in check1.iterrows():
                print(f"{x.Winner} beat {x.Loser}")

    for _, i in elo_data_clay.iterrows():
        check1 = elo_clay[
            ((elo_clay["Winner"] == i.Winner) & (elo_clay["Loser"] == i.Loser))
            | ((elo_clay["Loser"] == i.Winner) & (elo_clay["Winner"] == i.Loser))
        ]
        if check1.empty == False:
            for _, x in check1.iterrows():
                print(f"{x.Winner} beat {x.Loser}")

    if final_hard is not None:
        final_hard = final_hard

    else:
        final_hard = pd.DataFrame(
            columns=[
                "Fav_Top100",
                "Dog_Top100",
                "Sex",
                "Resulted",
                "Time",
                "Fav",
                "fav_rank",
                "fav_rank_high",
                "dog_rank",
                "dog_rank_high",
                "Fav_Odds",
                "fav_percent",
                "Fav_Serve%",
                "Fav_Return%",
                "fav_last_five_win_perc",
                "Dog",
                "Dog_Odds",
                "dog_percent",
                "Dog_Serve%",
                "Dog_Return%",
                "dog_last_five_win_perc",
            ]
        )

    final_hard[(final_hard["Resulted"] == "False")][
        [
            "Time",
            "Fav_Top100",
            "Dog_Top100",
            "Fav",
            "fav_rank",
            "fav_rank_high",
            "dog_rank",
            "dog_rank_high",
            "Fav_Odds",
            "fav_percent",
            "Fav_Serve%",
            "Fav_Return%",
            "fav_last_five_win_perc",
            "Dog",
            "Dog_Odds",
            "dog_percent",
            "Dog_Serve%",
            "Dog_Return%",
            "dog_last_five_win_perc",
        ]
    ]
    if final_clay is not None:
        final_clay = final_clay

    else:
        final_clay = pd.DataFrame(
            columns=[
                "Fav_Top100",
                "Dog_Top100",
                "Sex",
                "Resulted",
                "Time",
                "Fav",
                "fav_rank",
                "dog_rank",
                "Fav_Odds",
                "fav_percent",
                "Fav_Serve%",
                "Fav_Return%",
                "fav_last_five_win_perc",
                "Dog",
                "Dog_Odds",
                "dog_percent",
                "Dog_Serve%",
                "Dog_Return%",
                "dog_last_five_win_perc",
            ]
        )
    """

    final_clay[(final_clay["Sex"] == "Womens") & (final_clay["Resulted"] == "False")][
        [
            "Time",
            "Fav_Top100",
            "Dog_Top100",
            "Fav",
            "fav_rank",
            "fav_rank_high",
            "dog_rank",
            "dog_rank_high",
            "Fav_Odds",
            "fav_percent",
            "Fav_Serve%",
            "Fav_Return%",
            "fav_last_five_win_perc",
            "Dog",
            "Dog_Odds",
            "dog_percent",
            "Dog_Serve%",
            "Dog_Return%",
            "dog_last_five_win_perc",
        ]
    ]

    final_clay[(final_clay["Sex"] == "Mens") & (final_clay["Resulted"] == "False")][
        [
            "Time",
            "Fav_Top100",
            "Dog_Top100",
            "Fav",
            "fav_rank",
            "fav_rank_high",
            "dog_rank",
            "dog_rank_high",
            "Fav_Odds",
            "fav_percent",
            "Fav_Serve%",
            "Fav_Return%",
            "fav_last_five_win_perc",
            "Dog",
            "Dog_Odds",
            "dog_percent",
            "Dog_Serve%",
            "Dog_Return%",
            "dog_last_five_win_perc",
        ]
    ]
    """
    if final_hard is not None:
        final_hard.to_pickle("Hard_Today")
    else:
        final_clay[final_clay["Sex"] == "k"].to_pickle("Hard_Today")
    if final_clay is not None:
        final_clay.to_pickle("Clay_Today")
