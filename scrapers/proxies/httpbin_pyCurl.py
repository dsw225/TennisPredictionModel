import pycurl
import json
from io import BytesIO

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

    buffer = BytesIO()
    c = pycurl.Curl()
    c.setopt(pycurl.URL, 'http://httpbin.org/get')
    c.setopt(pycurl.PROXY, proxy_host)
    c.setopt(pycurl.PROXYPORT, int(proxy_port))
    c.setopt(pycurl.PROXYTYPE, pycurl.PROXYTYPE_HTTP)
    c.setopt(pycurl.WRITEFUNCTION, buffer.write)

    try:
        c.perform()
        c.close()
        body = buffer.getvalue().decode('utf-8')
        json_data = json.loads(body)
        return json_data
    except pycurl.error as e:
        print(f"Proxy {proxy} failed: {e}")
        return None

def main():
    proxies = read_proxies("scrapers/proxies/selected_proxies.csv")
    for proxy in proxies:
        json_data = try_proxy(proxy)
        if json_data is not None:
            print("pycurl: ", json_data)
            break
    else:
        print("All proxies failed.")

if __name__ == "__main__":
    main()
