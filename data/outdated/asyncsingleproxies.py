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

async def get_with_proxy(url, proxy):
    proxy_parts = proxy.split(':')
    if len(proxy_parts) == 4:
        proxy_host, proxy_port, proxy_user, proxy_pass = proxy_parts
        proxy_url = f'http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}'
        auth = aiohttp.BasicAuth(proxy_user, proxy_pass)
    else:  # Proxy without authentication
        proxy_host, proxy_port = proxy_parts
        proxy_url = f'http://{proxy_host}:{proxy_port}'
        auth = None

    async with aiohttp.ClientSession(trust_env=True) as session:
        try:
            async with session.get(url, proxy=proxy_url, proxy_auth=auth) as response:
                print("Status:", response.status)
                print("Content-type:", response.headers['content-type'])

                if response.status == 200:
                    return await response.json()
                else:
                    print(f"Proxy failed with status: {response.status}")
                    await session.close()
                    return await get_with_proxy(url, proxy)

        except (aiohttp.ClientError, Exception) as e:
            print(f"Current proxy failed: {e}")
            await session.close()
            return await get_with_proxy(url, proxy)
