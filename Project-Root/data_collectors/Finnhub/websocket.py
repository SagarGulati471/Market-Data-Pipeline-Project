

import asyncio
import websockets
import json




import os
import asyncio
import websockets
import logging
import time
import json

from dotenv import load_dotenv
from utils.logger import setup_logger
from config.config import Config
logger = logging.getLogger(__name__)



async def load_config():

    config = Config()
    logger.info("Configuration loaded successfully.")
    return config


async def subscribe_to_data(websocket, symbol_tickers=None):
    
    if not websocket:
        logger.error("WebSocket connection is not established. Cannot subscribe to data.")
        return
    
    logger.info(f"Sbscribing to symbols: {symbol_tickers}")
    try:
        subscribe_msg = {"type": "subscribe", "symbol": "BINANCE:BTCUSDT"}
        # subscribe_msg = {"type": "subscribe", "symbol": "BINANCE:BTCUSDT,OANDA:EUR_USD"}
        await websocket.send(json.dumps(subscribe_msg))
        logger.info(f"Subscription message sent: {subscribe_msg}")

    except Exception as e:
        logger.error(f"Exception occured while subcribing to symbols: {e}")


async def push_to_kafka(data):
    # Placeholder function to push data to Kafka
    logger.info("Pushing data to Kafka: %s", data)



def process_data(message):
    logger.info("Processing data ",message)
    try:
        socket_message = msg_pb2.SocketMessage()
        socket_message.ParseFromString(message)
        
        if socket_message.error:
            logger.info(f"Error in socket message: {socket_message.msg}")
            return None
            
        market_data = {}
        for symbol, feed in socket_message.feeds.items():
            depth_data = {
                'symbol': symbol,
                'timestamp': feed.feed_time.value,
                'total_bid_qty': feed.depth.tbq.value,
                'total_sell_qty': feed.depth.tsq.value,
                'bids': [],
                'asks': []
            }
            logger.info(f"Depth data {depth_data}")
    except Exception as e:
        logger.info("Exception", e)

async def data_receiver(websocket):
    """
    Continuously receive messages from WebSocket.
    """
    while True:
        logger.info("Data Receiver called")
        try:
            message = await websocket.recv()
            logger.debug("Received message: %s", message)
            # Here we can add code to push the received data to Kafka

            try:
                depth_data = process_data(message)
                await push_to_kafka(depth_data)
            except json.JSONDecodeError as e:
                logger.error("Failed to parse message as JSON: %s", str(e))
                continue  # Skip this message and continue receiving the next one

        except websockets.ConnectionClosed as e:
            logger.warning(f"Connection closed: code={e.code}, reason={e.reason}")
            break
        except Exception as e:
            logger.error(f"Recv error: {str(e)}")
            break


async def main():

    # Establish WebSocket connection and subscribe to data
    logger.info("Connecting to WebSocket...")

    FINNHUB_API_TOKEN = "d7eqsshr01qi33g7aps0d7eqsshr01qi33g7apsg"
    FINNHUB_WS_URI = f"wss://ws.finnhub.io?token={FINNHUB_API_TOKEN}"

    config = await load_config()
   
    retries = 0
    # Establish Websocket Connection
    while True:
        if retries > 0:
            logger.info(f"Retrying WebSocket connection... Attempt #{retries}")
        if retries >= config.WEBSOCKET_MAX_RETRIES:
            logger.error("Max retries reached. Exiting WebSocket client.")
            return
        try:
            async with websockets.connect(FINNHUB_WS_URI) as ws:
                websocket = ws

                # Subscribe to the required stock symbols to receive real-time data

                # Subscribe once after connection is established
                await subscribe_to_data(websocket)                
                # await ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:BTCUSDT"}))
                # await ws.send(json.dumps({"type": "subscribe", "symbol": "OANDA:EUR_USD"}))
                print("📡 Subscribed to BTC & EUR/USD (live 24/7)")


                # Start receiving in this same connection
                await data_receiver(websocket)

        except Exception as e:
            retries += 1
            logger.error("Error connecting to WebSocket: %s", str(e))
            await asyncio.sleep(config.WEBSOCKET_RETRY_INTERVAL)
            continue


if __name__ == "__main__":
    
    # Set up logging configuration
    setup_logger()
    
    # Run the main function in an asyncio event loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("WebSocket client stopped by user.")









async def finnhub_client():
    token = "d7eqsshr01qi33g7aps0d7eqsshr01qi33g7apsg"
    uri = f"wss://ws.finnhub.io?token={token}"

    async with websockets.connect(uri) as ws:
        print("✅ Connected!")

        # Subscribe to live 24/7 symbols so you see data immediately
        await ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:BTCUSDT"}))
        await ws.send(json.dumps({"type": "subscribe", "symbol": "OANDA:EUR_USD"}))
        print("📡 Subscribed to BTC & EUR/USD (live 24/7)")

        try:
            async for message in ws:
                data = json.loads(message)
                print("📥 Received:", json.dumps(data, indent=2))

                if data.get("type") == "trade":
                    for trade in data.get("data", []):
                        print(f"🚀 LIVE → {trade['s']} @ {trade['p']} | Vol: {trade.get('v')} | Time: {trade['t']}")

        except Exception as e:
            print(f"❌ Error: {e}")

if __name__ == "__main__":
    asyncio.run(finnhub_client())