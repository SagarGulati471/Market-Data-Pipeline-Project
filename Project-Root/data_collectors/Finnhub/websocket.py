import os
import asyncio
import websockets
import logging
import time
import json

from dotenv import load_dotenv
from utils.logger import setup_logger
from config.config import Config
from messaging.kafka_service.service import *


logger = logging.getLogger(__name__)


async def load_config():
    '''
    Load configuration settings from environment variables and return a Config object.
    '''
    config = Config()
    logger.info("Configuration loaded successfully.")
    return config


async def subscribe_to_data(websocket, config):
    '''
    Send a subscription message to the WebSocket to subscribe to the specified symbol tickers."""
    '''
    if not websocket:
        logger.error("WebSocket connection is not established. Cannot subscribe to data.")
        return
    
    symbol_tickers = config.FINNHUB_STOCK_SYMBOLS

    logger.info(f"Sbscribing to symbols: {symbol_tickers}")
    try:

        for symbol_ticker in symbol_tickers:
       
            # subscribe_msg = {"type": "subscribe", "symbol": "BINANCE:BTCUSDT"}
            subscribe_msg = {"type": "subscribe", "symbol": symbol_ticker}

            logger.debug(f"Subscribing to symbol: {subscribe_msg}")
            await websocket.send(json.dumps(subscribe_msg))
            logger.info(f"Subscription message sent: {subscribe_msg}")

    except Exception as e:
        logger.error(f"Exception occured while subcribing to symbols: {e}")


async def task_push_to_kafka(config):
    '''
    Fetches data from the queue and pushes it to Kafka.
    Any pre-processing of the data can be done here before sending to Kafka in process_data_from_queue function.
    '''

    KAFKA_TOPIC = config.FINNHUB_KAFKA_TOPIC  # Replace with your actual Kafka topic name
    KEY = f"TBT_data_{int(time.time() * 1000)}"  # Example key using current timestamp in milliseconds
    
    # Creates a Kafka producer instance
    producer = await create_kafka_producer()
    while True:
        try:
            message = await data_queue.get()  # Wait until a message is available in the queue
            await asyncio.sleep(0)
            logger.debug("Processing message from queue: %s", message)

            # Process the data and push to Kafka
            await send_message(producer, KAFKA_TOPIC, KEY, message)

        except Exception as e:
            logger.error(f"Error processing message from queue: {e}")


async def data_receiver(websocket):
    '''
    Continuously receive messages from WebSocket and add them to the processing queue.
    '''
    logger.info("data receiver called...")
    while True:
        try:
            message = await websocket.recv()
            logger.debug(f"Current Size of Queue: {data_queue.qsize()} \n")

            try:
                data_queue.put_nowait(message)  # Add raw message to the queue for processing
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
    '''
        Main function to establish WebSocket connection, subscribe to data, and start the data receiver and Kafka producer tasks.
    '''
    logger.info("Connecting to WebSocket...")
    
    config = await load_config()
    kafka_task = asyncio.create_task(task_push_to_kafka(config))  # Start the data processing task

    retries = 0
    # Establish Websocket Connection
    while True:
        if retries > 0:
            logger.info(f"Retrying WebSocket connection... Attempt #{retries}")
        if retries >= config.WEBSOCKET_MAX_RETRIES:
            logger.error("Max retries reached. Exiting WebSocket client.")
            return
        try:
            async with websockets.connect(config.FINNHUB_WEBSOCKET_URI) as websocket:

                # Subscribe once after connection is established
                await subscribe_to_data(websocket, config)                

                # Start receiving in this same connection
                await data_receiver(websocket)

        except Exception as e:
            retries += 1
            logger.error("Error connecting to WebSocket: %s", str(e))
            # kafka_task.cancel()
            await asyncio.sleep(config.WEBSOCKET_RETRY_INTERVAL)
        finally:

            kafka_task.cancel()
            await asyncio.gather(kafka_task, return_exceptions=True)
            


if __name__ == "__main__":
    
    # Set up logging configuration
    setup_logger()

    data_queue = asyncio.Queue(maxsize=100000)  # Queue to hold incoming data for processing

    # Run the main function in an asyncio event loop
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("WebSocket client stopped by user.")



# to be cleaned up and used as reference for testing websocket connection to finnhub
# async def finnhub_client():
#     token = "XXXX"
#     uri = f"wss://ws.finnhub.io?token={token}"

#     async with websockets.connect(uri) as ws:
#         print("Connected!")

#         # Subscribe to live 24/7 symbols so you see data immediately
#         await ws.send(json.dumps({"type": "subscribe", "symbol": "BINANCE:BTCUSDT"}))
#         await ws.send(json.dumps({"type": "subscribe", "symbol": "OANDA:EUR_USD"}))
#         print("Subscribed to BTC & EUR/USD (live 24/7)")

#         try:
#             async for message in ws:
#                 data = json.loads(message)
#                 print("Received:", json.dumps(data, indent=2))

#                 if data.get("type") == "trade":
#                     for trade in data.get("data", []):
#                         print(f"LIVE → {trade['s']} @ {trade['p']} | Vol: {trade.get('v')} | Time: {trade['t']}")

#         except Exception as e:
#             print(f"Error: {e}")

# if __name__ == "__main__":
#     asyncio.run(finnhub_client())