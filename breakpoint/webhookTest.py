import requests
import json

with open('breakpoint/hidden/webhooks.txt', 'r') as file:
    data = file.read().rstrip()

# Replace with your webhook URL
WEBHOOK_URL = data

def send_discord_embed(char):
    if char == 'm':
        gender = "men"
        name = "ATP"
        icon = "https://i0.wp.com/passingshot.productions/wp-content/uploads/2023/12/vecteezy_atp-tour-logo-symbol-tournament-open-men-tennis-association_23154125.jpg?resize=1200%2C1200&ssl=1"
        color = 2403043
    else:
        gender = "women"
        name = "WTA"
        icon = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcQgpAgGbgmgnUfLSTo4spYwcHZhKynuDvgniA&s"
        color = 7873532

    embed = {
        "author": {
            "name": f"{name} BREAKPOINT Bets",  # Set to an empty string to hide the name
            "icon_url": icon  # URL to the author's icon image
        },
        "description": (
            "**August 15th, 2024** \n"
            f"Todays {gender}s betting slate for further [information click here](<https://discordapp.com/channels/1273303003943932038/1273405553116188744>)\n\n"
            f"**Player A** - *{name} 2.* vs. **Player B** - *{name} 5.*\n"
            "[Details](<https://www.sofascore.com/tennis/match/kostyuk-swiatek/xpQbsgbRb#id:12652642>) | **Start:** 1:30 PM EST | **Surface:** Grass | **Event:** Wimbledon\n"
            "**Win:** [Player A](<https://www.sofascore.com/team/tennis/swiatek-iga/228272>) | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275\n"
            "**Games:** Over 22.5 | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275\n\n"
            f"**Player A** - *{name} 2.* vs. **Player B** - *{name} 5.*\n"
            "[Details](<https://www.sofascore.com/tennis/match/kostyuk-swiatek/xpQbsgbRb#id:12652642>) | **Start:** 1:30 PM EST | **Surface:** Grass | **Event:** Wimbledon\n"
            "**Win:** [Player A](<https://www.sofascore.com/team/tennis/swiatek-iga/228272>) | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275\n"
            "**Games:** Over 22.5 | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275\n\n"
            f"**Player A** - *{name} 2.* vs. **Player B** - *{name} 5.*\n"
            "[Details](<https://www.sofascore.com/tennis/match/kostyuk-swiatek/xpQbsgbRb#id:12652642>) | **Start:** 1:30 PM EST | **Surface:** Grass | **Event:** Wimbledon\n"
            "**Win:** [Player A](<https://www.sofascore.com/team/tennis/swiatek-iga/228272>) | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275\n"
            "**Games:** Over 22.5 | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275"
        ),
        "color": color  # 2403043 is ATP, 7873532 is WTA
    }

    collapsible = {
    # embed = {
    #     # "image": {
    #     #     "url": 'https://i.sstatic.net/Fzh0w.png'
    #     # },
    #     "author": {
    #         "name": f"{name} BREAKPOINT Bets",  # Set to an empty string to hide the name
    #         "icon_url": icon  # URL to the author's icon image
    #     },
    #     "description": f"**August 15th, 2024** \nTodays {gender}s betting slate for further [information click here](<https://discordapp.com/channels/1273303003943932038/1273405553116188744>)",
    #     "color": color, #2403043 is ATP #7873532 is WTA  # This is a color code in decimal
    #     "fields": [
    #         {
    #             "name": "**Player A** - *ATP 2.* vs. **Player B** - *ATP 5.*",
    #             "value": "[Details](<https://www.sofascore.com/tennis/match/kostyuk-swiatek/xpQbsgbRb#id:12652642>) | **Start:** 1:30 PM EST | **Surface:** Grass | **Event:** Wimbledon \n**Win:** [Player A](<https://www.sofascore.com/team/tennis/swiatek-iga/228272>) | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275 \n**Games:** Over 22.5 | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275 \n\u200B",
    #             "inline": False
    #         },
    #         {
    #             "name": "**Player A** - *ATP 2.* vs. **Player B** - *ATP 5.*",
    #             "value": "[Details](<https://www.sofascore.com/tennis/match/kostyuk-swiatek/xpQbsgbRb#id:12652642>) | **Start:** 1:30 PM EST | **Surface:** Grass | **Event:** Wimbledon \n**Win:** [Player A](<https://www.sofascore.com/team/tennis/swiatek-iga/228272>) | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275 \n**Games:** Over 22.5 | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275 \n\u200B",
    #             "inline": False
    #         },
    #         {
    #             "name": "**Player A** - *ATP 2.* vs. **Player B** - *ATP 5.*",
    #             "value": "[Details](<https://www.sofascore.com/tennis/match/kostyuk-swiatek/xpQbsgbRb#id:12652642>) | **Start:** 1:30 PM EST | **Surface:** Grass | **Event:** Wimbledon \n**Win:** [Player A](<https://www.sofascore.com/team/tennis/swiatek-iga/228272>) | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275 \n**Games:** Over 22.5 | **Prob.:** 62% | **Odds:** +110 / 47.6% | **Edge:** 14.4% | **Kelly:** .275",
    #             "inline": False
    #         },
    #     ]
    # }
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
    send_discord_embed('m')
    send_discord_embed('w')
