import aiohttp
import asyncio
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
    
async def get_new_conn(url, iproxy, proxies):
    for proxy in range(iproxy, len(proxies)):
        json_data, session = await get_connection(proxies[proxy], url)
        if json_data is not None:
            iproxy = proxy
            return json_data, session
    else:
        print("All proxies failed.")

async def get_connection(proxy, url):
    proxy_host, proxy_port = proxy.split(':')
    print(f"Trying Proxy Host: {proxy_host} Proxy Port: {proxy_port}")

    connector = aiohttp.TCPConnector()
    session = aiohttp.ClientSession(connector=connector)

    proxy_url = f'http://{proxy_host}:{proxy_port}'

    try:
        async with session.get(url, proxy=proxy_url) as response:
            if response.status == 200:
                data = await response.text()
                json_data = json.loads(data)
                return json_data, session
            else:
                print(f"Proxy {proxy} failed with status: {response.status}")
                return None, session
    except (aiohttp.ClientError, Exception) as e:
        print(f"Proxy {proxy} failed: {e}")
        await session.close()
        return None, ''

async def get_with_proxy(url, iproxy, session, proxies):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.text()
                json_data = json.loads(data)
                await session.close()
                return json_data, session
            else:
                print(f"Current proxy failed with status: {response.status}")
                return await get_new_conn(url, iproxy, proxies)
    except (aiohttp.ClientError, Exception) as e:
        print(f"Current proxy failed: {e}")
        return await get_new_conn(url, iproxy, proxies)

