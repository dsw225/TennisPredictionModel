import requests
from bs4 import BeautifulSoup
import json
import csv
from datetime import datetime

todays_date = datetime.now()
match_id = "QBgQjHHs"
url = f"https://global.flashscore.ninja/2/x/feed/df_st_1_{match_id}"

headers = {"accept": "*/*", "X-Fsign": "SW9D1eZo"}

response = requests.get(url, headers=headers)

data = response.text

def extract_stats(stats_str):
    import re
    stats = {
        "w_ace": 0, "w_df": 0, "w_svpt": 0, "w_1stIn": 0, "w_1stWon": 0, "w_2ndWon": 0, "w_SvGms": 0, "w_bpSaved": 0, "w_bpFaced": 0,
        "l_ace": 0, "l_df": 0, "l_svpt": 0, "l_1stIn": 0, "l_1stWon": 0, "l_2ndWon": 0, "l_SvGms": 0, "l_bpSaved": 0, "l_bpFaced": 0
    }

    # Regular expressions to match the needed stats
    ace_pattern = re.compile(r"Aces¬SH÷(\d+)¬SI÷(\d+)")
    df_pattern = re.compile(r"Double Faults¬SH÷(\d+)¬SI÷(\d+)")
    svpt_pattern = re.compile(r"1st Serve Points Won¬SH÷\d+% \((\d+)/(\d+)\)¬SI÷\d+% \((\d+)/(\d+)\)")
    in_pattern = re.compile(r"1st Serve Percentage¬SH÷(\d+)%¬SI÷(\d+)%")
    won_pattern = re.compile(r"1st Serve Points Won¬SH÷\d+% \((\d+)/(\d+)\)¬SI÷\d+% \((\d+)/(\d+)\)")
    second_won_pattern = re.compile(r"2nd Serve Points Won¬SH÷\d+% \((\d+)/(\d+)\)¬SI÷\d+% \((\d+)/(\d+)\)")
    bp_saved_pattern = re.compile(r"Break Points Saved¬SH÷\d+% \((\d+)/(\d+)\)¬SI÷\d+% \((\d+)/(\d+)\)")
    sv_gms_pattern = re.compile(r"Service Games Won¬SH÷\d+% \((\d+)/(\d+)\)¬SI÷\d+% \((\d+)/(\d+)\)")

    # Find all matches
    ace_match = ace_pattern.search(stats_str)
    df_match = df_pattern.search(stats_str)
    svpt_match = svpt_pattern.search(stats_str)
    in_match = in_pattern.search(stats_str)
    won_match = won_pattern.search(stats_str)
    second_won_match = second_won_pattern.search(stats_str)
    bp_saved_match = bp_saved_pattern.search(stats_str)
    sv_gms_match = sv_gms_pattern.search(stats_str)

    if ace_match:
        stats["w_ace"] = int(ace_match.group(1))
        stats["l_ace"] = int(ace_match.group(2))

    if df_match:
        stats["w_df"] = int(df_match.group(1))
        stats["l_df"] = int(df_match.group(2))

    if svpt_match:
        stats["w_svpt"] = int(svpt_match.group(2))
        stats["l_svpt"] = int(svpt_match.group(4))
        stats["w_1stWon"] = int(svpt_match.group(1))
        stats["l_1stWon"] = int(svpt_match.group(3))

    if in_match:
        stats["w_1stIn"] = int(in_match.group(1))
        stats["l_1stIn"] = int(in_match.group(2))

    if second_won_match:
        stats["w_2ndWon"] = int(second_won_match.group(1))
        stats["l_2ndWon"] = int(second_won_match.group(3))

    if bp_saved_match:
        stats["w_bpSaved"] = int(bp_saved_match.group(1))
        stats["w_bpFaced"] = int(bp_saved_match.group(2))
        stats["l_bpSaved"] = int(bp_saved_match.group(3))
        stats["l_bpFaced"] = int(bp_saved_match.group(4))

    if sv_gms_match:
        stats["w_SvGms"] = int(sv_gms_match.group(1))
        stats["l_SvGms"] = int(sv_gms_match.group(3))

    return stats


print(extract_stats(data))
