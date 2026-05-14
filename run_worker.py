# Standalone worker process — runs listener_manager and positions_tracker
import asyncio
from dotenv import load_dotenv
load_dotenv()

from hedgebridge.listener_manager import listener_manager
from hedgebridge.positions_tracker import positions_tracker

async def main():
    await asyncio.gather(
        listener_manager.start(),
        positions_tracker.run(),
    )

if __name__ == "__main__":
    asyncio.run(main())