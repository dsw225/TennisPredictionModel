import aiohttp
import aiofiles
import asyncio
import re

async def extract_proxies(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            text = await response.text()
            proxies = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+', text)
    return proxies

async def test_proxy(session, proxy):
    try:
        async with session.get('http://httpbin.org/get', proxy=f'http://{proxy}', timeout=2) as response:
            if response.status == 200:
                return proxy
    except:
        pass
    return None

async def test_proxies(proxies, num_threads=10):
    async with aiohttp.ClientSession() as session:
        tasks = [test_proxy(session, proxy) for proxy in proxies]
        responses = await asyncio.gather(*tasks)
        selected_proxies = [proxy for proxy in responses if proxy]

    async with aiofiles.open('scrapers/proxies/selected_proxies.csv', 'w') as csvfile:
        for proxy in selected_proxies:
            await csvfile.write(f'{proxy}\n')

    return len(selected_proxies)

async def main():
    proxies = await extract_proxies('https://free-proxy-list.net/')
    successful_proxies_count = await test_proxies(proxies)
    print(f'Successful proxies: {successful_proxies_count}')

if __name__ == '__main__':
    asyncio.run(main())
