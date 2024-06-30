import csv
import requests
import threading
import re

def extract_proxies(url):
    # Send a GET request to the URL
    response = requests.get(url)

    # Extract the proxies using a regular expression
    proxies = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+', response.text)

    return proxies

def test_proxies(proxies, num_threads=10):
    def test_proxy(proxy):
        try:
            response = requests.get('http://httpbin.org/get', proxies={'http': proxy, 'https': proxy}, timeout=2)
            if response.status_code == 200:
                print(f'Successful proxy: {proxy}')
                selected_proxies.append(proxy)
        except:
            pass

    # Test each proxy against httpbin.org using multiple threads
    selected_proxies = []
    threads = []
    for i in range(num_threads):
        threads.append(threading.Thread(target=lambda q: [test_proxy(proxy) for proxy in q], args=(proxies[i::num_threads],)))
        threads[-1].start()
    for thread in threads:
        thread.join()

    # Write the selected proxies to a new CSV file
    with open('scrapers/proxies/selected_proxies.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for proxy in selected_proxies:
            writer.writerow([proxy])

    # Return the number of successful proxies
    return len(selected_proxies)

test_proxies(extract_proxies('https://free-proxy-list.net/'))