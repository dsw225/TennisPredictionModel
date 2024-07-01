import aiohttp
import asyncio

async def read_proxies(file_path):
    try:
        with open(file_path, 'r') as csvfile:
            proxies = [line.strip() for line in csvfile.readlines()]
        return proxies
    except FileNotFoundError:
        print(f"Proxy file not found: {file_path}")
        return []

async def try_proxy(proxy):
    print(f"Trying Proxy: {proxy}")
    proxy_parts = proxy.split(':')
    if len(proxy_parts) == 4:
        proxy_host, proxy_port, proxy_user, proxy_pass = proxy_parts
        auth = aiohttp.BasicAuth(proxy_user, proxy_pass)
        proxy_url = f'http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}'
    else:  # Proxy without authentication
        proxy_host, proxy_port = proxy_parts
        auth = None
        proxy_url = f'http://{proxy_host}:{proxy_port}'


    async with aiohttp.ClientSession(trust_env=True) as session:
        try:
            async with session.get("https://httpbin.org/get", proxy=proxy_url, proxy_auth=auth) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"Failed to retrieve data using proxy {proxy}")
                    return None
        except Exception as e:
            print(f"Error with proxy {proxy}: {e}")
            return None
    



async def get_connection(url, proxy):
    for i in range(5):  # retries
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

        async with aiohttp.ClientSession(trust_env=True) as session:
            try:
                async with session.get(url, proxy=proxy_url, proxy_auth=auth) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data, session
                    else:
                        print(f"Failed to retrieve data using proxy {proxy}")
            except (aiohttp.ClientError, Exception) as e:
                print(f"Proxy {proxy} failed: {e}")

    print("Retrying proxy connect")
    return None, None

async def get_with_proxy(url, proxy, session=None):
    if session is None:
        return await get_connection(url, proxy)
    
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data, session
            else:
                print(f"Current proxy failed with status: {response.status}")
                await session.close()
                return await get_connection(url, proxy)
    except (aiohttp.ClientError, Exception) as e:
        print(f"Current proxy failed: {e}")
        await session.close()
        return await get_connection(url, proxy)

async def main():
    proxies = await read_proxies("scrapers/proxy_addresses/smartproxy.csv")
    proxy = proxies[0]
    for i in range(1):
        # json_data = await try_proxy(proxy)
        json_data, _ = await get_with_proxy("https://api.sofascore.com/api/v1/category/3/scheduled-events/2024-06-28", proxy)
        unsorted_matches = json_data.get('events', [])
        print(len(unsorted_matches))
        # if json_data is not None:
        #     print(f"AIOHTTP {i}: ", json_data)
    # else:
    #     print("All proxies failed.")

if __name__ == "__main__":
    asyncio.run(main())
