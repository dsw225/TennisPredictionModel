from http.client import HTTPConnection, HTTPException
import json

def read_proxies(file_path):
    try:
        with open(file_path, 'r') as csvfile:
            proxies = [line.strip() for line in csvfile.readlines()]
        return proxies
    except FileNotFoundError:
        print(f"Proxy file not found: {file_path}")
        return []

def try_proxy(proxy):
    proxy_host, proxy_port = proxy.split(':')
    print(f"Trying Proxy Host: {proxy_host} Proxy Port: {proxy_port}")

    conn = HTTPConnection(proxy_host, int(proxy_port))
    conn.set_tunnel('httpbin.org')

    payload = ''
    headers = {}

    try:
        conn.request("GET", '/get', payload, headers)
        res = conn.getresponse()
        data = res.read()
        json_data = json.loads(data.decode("utf-8"))
        conn.close()
        return json_data
    except (HTTPException, ConnectionError) as e:
        print(f"Proxy {proxy} failed: {e}")
        return None

def main():
    proxies = read_proxies("scrapers/proxies/selected_proxies.csv")
    for proxy in proxies:
        json_data = try_proxy(proxy)
        if json_data is not None:
            print("HTTP Lib: ", json_data)
            break
    else:
        print("All proxies failed.")

if __name__ == "__main__":
    main()
