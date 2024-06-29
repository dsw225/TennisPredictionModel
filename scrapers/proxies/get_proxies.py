import csv
import re
import requests


def extract_proxies(url, filename):
    """
    Extracts proxy IP addresses and ports from a given URL and writes them to a CSV file.

    Args:
        url (str): The URL to scrape for proxies.
        filename (str): The name of the CSV file to write the proxies to.
    """
    # Send a GET request to the URL
    response = requests.get(url)

    # Extract the proxies using a regular expression
    proxies = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+', response.text)

    return proxies
    # Write the proxies to a CSV file
    # with open(filename, 'w', newline='') as csvfile:
    #     writer = csv.writer(csvfile)
    #     for proxy in proxies:
    #         writer.writerow([proxy])


# extract_proxies('https://free-proxy-list.net/', 'scrapers/proxies/proxies.csv')
