import requests
import json

with open('breakpoint/hidden/webhooks.txt', 'r') as file:
    data = file.read().rstrip()

# Replace with your webhook URL
WEBHOOK_URL = data

def send_discord_embed():

    embed = {
        # "image": {
        #     "url": 'https://i.sstatic.net/Fzh0w.png'
        # },
        "title": "August 15th ATP BREAKPOINT Bets",
        "description": "Here is todays betting slate for [directions click here](<https://discordapp.com/channels/1273303003943932038/1273405553116188744>)",
        "color": 2403043, #2403043 is ATP #7873532 is WTA  # This is a color code in decimal
        "fields": [
            {
                "name": "**Player A** - *ATP 2.* vs. **Player B** - *ATP 5.*",
                "value": "[Details](<https://www.sofascore.com/tennis/match/kostyuk-swiatek/xpQbsgbRb#id:12652642>) | **Start:** 1:30 PM EST | **Surface:** Grass | **Event:** Wimbledon \n **Win:** [Player A](<https://www.sofascore.com/team/tennis/swiatek-iga/228272>) | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275 \n **Games:** Over 22.5 | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275",
                "inline": False
            },
            {
                "name": "**Player A** - *ATP 2.* vs. **Player B** - *ATP 5.*",
                "value": "[Details](<https://www.sofascore.com/tennis/match/kostyuk-swiatek/xpQbsgbRb#id:12652642>) | **Start:** 1:30 PM EST | **Surface:** Grass | **Event:** Wimbledon \n **Win:** [Player A](<https://www.sofascore.com/team/tennis/swiatek-iga/228272>) | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275 \n **Games:** Over 22.5 | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275",
                "inline": False
            },
            {
                "name": "**Player A** - *ATP 2.* vs. **Player B** - *ATP 5.*",
                "value": "[Details](<https://www.sofascore.com/tennis/match/kostyuk-swiatek/xpQbsgbRb#id:12652642>) | **Start:** 1:30 PM EST | **Surface:** Grass | **Event:** Wimbledon \n **Win:** [Player A](<https://www.sofascore.com/team/tennis/swiatek-iga/228272>) | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275 \n **Games:** Over 22.5 | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275",
                "inline": False
            },
        ]
    }

    data = {
        "embeds": [embed]
    }
    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(WEBHOOK_URL, data=json.dumps(data), headers=headers)

    if response.status_code == 204:
        print("Message sent successfully.")
    else:
        print(f"Failed to send message. Status code: {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    send_discord_embed()
