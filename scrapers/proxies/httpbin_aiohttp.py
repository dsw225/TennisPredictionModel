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
    async with aiohttp.ClientSession(trust_env=True) as session:
        try:
            async with session.get("https://httpbin.org/get", proxy=f"http://{proxy}") as response:
                print("Status:", response.status)
                print("Content-type:", response.headers['content-type'])

                if response.status == 200:
                    return await response.json()
                else:
                    print(f"Failed to retrieve data using proxy {proxy}")
                    return None
        except Exception as e:
            print(f"Error with proxy {proxy}: {e}")
            return None

async def main():
    proxies = await read_proxies("scrapers/proxies/selected_proxies.csv")
    for proxy in proxies:
        json_data = await try_proxy(proxy)
        if json_data is not None:
            print("HTTP Lib:", json_data)
            break
    else:
        print("All proxies failed.")

if __name__ == "__main__":
    asyncio.run(main())
