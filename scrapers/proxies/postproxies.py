from http.client import HTTPConnection, HTTPException
import json

iproxy = 0

def read_proxies(file_path):
    try:
        with open(file_path, 'r') as csvfile:
            proxies = [line.strip() for line in csvfile.readlines()]
        return proxies
    except FileNotFoundError:
        print(f"Proxy file not found: {file_path}")
        return []
    
def get_new_conn(url, iproxy, proxies):
    for proxy in range(iproxy, len(proxies)):
        json_data, conn = get_connection(proxies[proxy], url)
        if json_data is not None:
            iproxy = proxy
            return json_data, conn
    else:
        print("All proxies failed.")

def get_connection(proxy, url):
    proxy_host, proxy_port = proxy.split(':')
    print(f"Trying Proxy Host: {proxy_host} Proxy Port: {proxy_port}")

    conn = HTTPConnection(proxy_host, int(proxy_port))
    conn.set_tunnel('api.sofascore.com')

    payload = ''
    headers = {}

    try:
        conn.request("GET", url, payload, headers)
        res = conn.getresponse()
        data = res.read()
        json_data = json.loads(data.decode("utf-8"))
        return json_data, conn
    except (HTTPException, ConnectionError, Exception) as e:
        print(f"Proxy {proxy} failed: {e}")
        return None, conn
    
def get_with_proxy(url, iproxy, conn, proxies):
    payload = ''
    headers = {}

    try:
        conn.request("GET", url, payload, headers)
        res = conn.getresponse()
        data = res.read()
        json_data = json.loads(data.decode("utf-8"))
        conn.close()
        return json_data, conn
    except (HTTPException, ConnectionError) as e:
        print(f"Curren proxy failed: {e}")
        return get_new_conn(url, iproxy, proxies)
