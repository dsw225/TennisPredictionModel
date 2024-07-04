import aiohttp
import asyncio
import aiofiles
import json

async def read_proxies(file_path):
    try:  
        async with aiofiles.open(file_path, 'r') as csvfile:
            proxies = [line.strip() for line in await csvfile.readlines()]
        return proxies
    except FileNotFoundError:
        print(f"Proxy file not found: {file_path}")
        return []

async def get_new_conn(url, iproxy, proxies, session=None):
    for proxy in range(iproxy, len(proxies)):
        json_data, session = await get_connection(proxies[proxy], url, session)
        if json_data is not None:
            iproxy = proxy
            return json_data, session
    print("All proxies failed.")
    return None, None

async def get_connection(proxy, url, session=None):
    proxy_parts = proxy.split(':')
    if len(proxy_parts) == 4:
        proxy_host, proxy_port, proxy_user, proxy_pass = proxy_parts
        auth = aiohttp.BasicAuth(proxy_user, proxy_pass)
        proxy_url = f'http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}'
    else:  # Proxy without authentication
        proxy_host, proxy_port = proxy_parts
        auth = None
        proxy_url = f'http://{proxy_host}:{proxy_port}'

    print(f"Trying Proxy Host: {proxy_host} Proxy Port: {proxy_port}")

    connector = aiohttp.TCPConnector()
    session = aiohttp.ClientSession(connector=connector) if session is None else session

    try:
        async with session.get(url, proxy=proxy_url, proxy_auth=auth) as response:
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
                return await get_new_conn(url, iproxy, proxies, session)
    except (aiohttp.ClientError, Exception) as e:
        print(f"Current proxy failed: {e}")
        return await get_new_conn(url, iproxy, proxies, session)
