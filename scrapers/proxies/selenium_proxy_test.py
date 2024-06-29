import csv
import random
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import get_proxies


def get_working_ip(proxies):
    """
    Returns a working IP address from a list of proxies.

    Args:
        proxies (list): A list of proxies in the format 'IP:port'.

    Returns:
        str: The IP address of a working proxy.
    """
    num_attempts = 0
    while num_attempts < 4:
        proxy = random.choice(proxies)
        if test_ip(proxy):
            print(f"Using IP address: {proxy}")
            return proxy
        else:
            num_attempts += 1
            print(f"IP address {proxy} is blocked. Trying another IP address...")
    return None

def test_ip(ip_address):
    """
    Tests if a given IP address is accessible by opening a browser window and visiting https://www.whatismyip.com/

    Args:
        ip_address (str): The IP address to test.

    Returns:
        bool: True if the IP address is accessible, False otherwise.
    """
    try:
        options = webdriver.ChromeOptions()
        proxy = f"{ip_address}"
        options.add_argument(f"--proxy-server={proxy}")
        driver = webdriver.Remote(command_executor='http://localhost:4444', options=options)
        driver.get('https://www.whatismyip.com/')
        time.sleep(5)
        ip = driver.find_element(By.ID, "ipv4").text
        driver.quit()
        return ip
    except Exception as e:
        raise e

def main():
    # Read the proxies from the CSV file
    # with open('selected_proxies.csv', 'r') as csvfile:
    #     reader = csv.reader(csvfile)
    #     proxies = [row[0] for row in reader]
    proxies = get_proxies.extract_proxies('https://free-proxy-list.net/', '')
    
    # Get a working IP address
    ip_address = get_working_ip(proxies)

    if ip_address is None:
        print("Could not get current IP address. Aborting...")
    else:
        print(f"Current IP address returned by whatismyip.com: {ip_address}")

    # Close the browser
    try:
    	driver.quit()
    except:
    	...

if __name__ == '__main__':
    main()

