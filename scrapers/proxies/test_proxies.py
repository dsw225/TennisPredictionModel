import csv
import requests
import threading
import get_proxies

def test_proxies(filename, num_threads=10):
    """
    Reads a CSV file containing proxies, tests each proxy against httpbin.org using multiple threads, and stores the successful proxies in a separate CSV file.

    Args:
        filename (str): The name of the CSV file containing proxies.
        num_threads (int): The number of threads to use for testing proxies. Default is 10.
    
    Returns:
        int: The number of successful proxies.
    """
    proxies = filename
    # # Read the proxies from the CSV file
    # with open(filename, 'r') as csvfile:
    #     reader = csv.reader(csvfile)
    #     proxies = [row[0] for row in reader]

    # Define the thread function
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


test_proxies(get_proxies.extract_proxies('https://free-proxy-list.net/', ''))