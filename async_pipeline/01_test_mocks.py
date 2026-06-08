import asyncio
import aiohttp
import time

MOCKS = {
    "AWS": "http://localhost:8001/metrics",
    "AKS": "http://localhost:8002/metrics",
    "DO": "http://localhost:8003/metrics"
}

async def fetch_mock(session, name, url):
    try:
        async with session.get(url, timeout=5) as response:
            if response.status == 200:
                text = await response.text()
                print(f"[✅ {name}] Success! Received {len(text.splitlines())} lines of metrics.")
            else:
                print(f"[❌ {name}] Failed with status {response.status}")
    except Exception as e:
        print(f"[❌ {name}] Connection error: {e}")

async def main():
    print("🚀 Starting infinite mock poller. Press Ctrl+C to stop.")
    async with aiohttp.ClientSession() as session:
        while True:
            tasks = [fetch_mock(session, name, url) for name, url in MOCKS.items()]
            await asyncio.gather(*tasks)
            print("-" * 50)
            await asyncio.sleep(2)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
