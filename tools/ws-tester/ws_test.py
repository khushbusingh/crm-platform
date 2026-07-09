import asyncio
import sys
import websockets


async def main(uri: str) -> None:
    async with websockets.connect(uri) as ws:
        print(f"connected to {uri}")
        async for message in ws:
            print(f"< {message}")


if __name__ == "__main__":
    uri = sys.argv[1] if len(sys.argv) > 1 else "ws://localhost:8003/ws/agent/1"
    asyncio.run(main(uri))
