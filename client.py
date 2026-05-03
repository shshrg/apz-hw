import asyncio
import aiohttp
import time
import sys

URL_BASE = "http://localhost:8080/facade_service"
DEBUG_URL = f"{URL_BASE}/debug/service_times"
ACCOUNTS_URL = f"{URL_BASE}/accounts"
NUM_CLIENTS = 10
REQUESTS_PER_CLIENT = 10000
TOTAL_REQUESTS = NUM_CLIENTS * REQUESTS_PER_CLIENT

completed_requests = 0

async def send_requests(session: aiohttp.ClientSession, user_id: int, count: int):
    global completed_requests
    payload = {"user_Id": user_id, "amount": 1}
    
    for i in range(count):
        try:
            async with session.post(URL_BASE, json=payload) as response:
                await response.release()
            
            completed_requests += 1
            
            if completed_requests % 1000 == 0:
                sys.stdout.write(f"\rProgress: {completed_requests}/{TOTAL_REQUESTS} requests processed...")
                sys.stdout.flush()
        except Exception:
            pass

async def run_test(scenario_name: str, use_same_user: bool):
    global completed_requests
    completed_requests = 0
    
    print(f"\n{'='*60}")
    print(f"Starting test: {scenario_name}")
    
    
    async with aiohttp.ClientSession() as admin_session:
        await admin_session.post(DEBUG_URL)
    
    
    connector = aiohttp.TCPConnector(limit=500, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        start_time = time.perf_counter()
        tasks = []
        for i in range(NUM_CLIENTS):
            user_id = 999 if use_same_user else i
            tasks.append(send_requests(session, user_id, REQUESTS_PER_CLIENT))
        
        await asyncio.gather(*tasks)
        end_time = time.perf_counter()
        total_duration = end_time - start_time

    
    async with aiohttp.ClientSession() as admin_session:
       
        async with admin_session.get(DEBUG_URL) as resp:
            stats = await resp.json()
        
        async with admin_session.get(ACCOUNTS_URL) as resp:
            accounts = await resp.json()
    
    log_sum = stats["logging_service"]
    count_sum = stats["counter_service"]
    rps = TOTAL_REQUESTS / total_duration

    print(f"\n\nResults for '{scenario_name}':")
    print(f"   - Total duration: {total_duration:.2f} s")
    print(f"   - Average Speed: {rps:.2f} req/s")
    print(f"   - Sum of internal Logging time: {log_sum:.2f} s")
    print(f"   - Sum of internal Counter time: {count_sum:.2f} s")

    total_internal = log_sum + count_sum
    if total_internal > 0:
        print(f"   - Time split: Log {log_sum/total_internal:.1%} | Count {count_sum/total_internal:.1%}")

    print("\nFinal Balances:")
    
    if isinstance(accounts, dict):
        for user, balance in sorted(accounts.items()):
            print(f"   User {user}: {balance}")
    else:
        print(f"   {accounts}")

async def main():
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    
    await run_test("Scenario 1: 10 Different Accounts", use_same_user=False)
    print("\nWaiting for socket cleanup...")
    await asyncio.sleep(2) 
    await run_test("Scenario 2: 1 Single Shared Account", use_same_user=True)

if __name__ == "__main__":
    asyncio.run(main())