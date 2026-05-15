print("🔥 TOP LEVEL STARTED")

import asyncio
from hedgebridge.positions_tracker import positions_tracker

async def main():
    print("🔥 INSIDE MAIN")
    await positions_tracker.run()

if __name__ == "__main__":
    asyncio.run(main())