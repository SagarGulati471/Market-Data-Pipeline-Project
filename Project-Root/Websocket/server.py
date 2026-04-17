import asyncio
import websockets
import logging

logging.basicConfig(level=logging.INFO)

async def echo(websocket):
    """
    The WebSocket server handler.
    'path' is typically used for routing in more complex applications.
    """
    print(f"Websocket = {websocket}, type(websocket) = {type(websocket)}")
    logging.info("A client connected.")
    try:
        async for message in websocket:
            logging.info(f"Received message: {message}")
            await websocket.send(f"Server echo: {message}")
    except websockets.exceptions.ConnectionClosed:
        logging.info("A client disconnected.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

async def main(): # Parent Coroutine
    # Start the server on localhost, port 8765
    async with websockets.serve(echo, "localhost", 8765):
        logging.info("WebSocket server started on ws://localhost:8765")
        await asyncio.Future() # Run forever

if __name__ == "__main__":
    asyncio.run(main())
