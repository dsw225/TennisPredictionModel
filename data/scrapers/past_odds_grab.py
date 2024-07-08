import requests
import os

mw = 'm'            ## 'm' = men, 'w' = women
yr = 2024        ## year to grab

if mw == 'm':   
    url = f"http://www.tennis-data.co.uk/{yr}/{yr}.xlsx"
    directory = "csvs/ATP (Mens)/Odds"
else:
    url = f"http://www.tennis-data.co.uk/{yr}w/{yr}.xlsx"
    directory = "csvs/WTA (Womens)/Odds"

# Ensure the directory exists; if not, create it
os.makedirs(directory, exist_ok=True)

# Filename will be extracted from the URL
filename = os.path.join(directory, url.split("/")[-1])

# Send a GET request to the URL
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Open the file and write the content received to it
    with open(filename, 'wb') as f:
        f.write(response.content)
    print(f"File downloaded successfully to {filename}")
else:
    print(f"Failed to download file, status code {response.status_code}")
