import asyncio
from hedgebridge.listener_manager import listener_manager

async def main():
    print("🔥 SCRIPT STARTED")
    await listener_manager.start()

if __name__ == "__main__":
    asyncio.run(main())

