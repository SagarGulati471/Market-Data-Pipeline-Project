import os
import asyncio
import websockets
import logging
import time
import json

from dotenv import load_dotenv
from utils.logger import setup_logger
from generate_token import TokenManager
from config.config import Config
from data_collector import msg_pb2

logger = logging.getLogger(__name__)



async def setup_authentication():
    token_manager = TokenManager()
    token_manager.get_access_token()

    # After ensuring we have a valid access token, we can proceed to get the auth info required for WebSocket connection
    config = Config()
    return config


def get_auth_info():
    token_manager = TokenManager()
    auth_info = token_manager.get_auth_info()
    return auth_info


async def subscribe_to_data(websocket, symbol_tickers):
    
    if not websocket:
        logger.error("WebSocket connection is not established. Cannot subscribe to data.")
        return
    
    logger.info(f"Sbscribing to symbols: {symbol_tickers}")
    try:
        subscribe_msg = {
                            "type": 1,
                            "data": {
                                "subs": 1,
                                "symbols": list(symbol_tickers),
                                "mode": "depth",
                                "channel": "1",
                            },
                        }

        await websocket.send(json.dumps(subscribe_msg))
        logger.info(f"Subscription message sent: {subscribe_msg}")

        # Resume the channel(s)
        resume_msg = {
            "type": 2,
            "data": {
                "resumeChannels": ["1"],   # list of strings
                "pauseChannels": []        # optional
            }
        }

        await websocket.send(json.dumps(resume_msg))
        logger.info(f"Resume message sent: {resume_msg}")

    except Exception as e:
        logger.error(f"Exception occured while subcribing to symbols: {e}")


async def push_to_kafka(data):
    # Placeholder function to push data to Kafka
    logger.info("Pushing data to Kafka: %s", data)


# async def data_receiver(websocket):
#     """
#     Continuously receive messages from WebSocket.
#     """
#     while True:
#         try:
#             message = await websocket.recv()
#             logger.debug("Received message: %s", message)
#             # Here we can add code to push the received data to Kafka

#             try:
#                 parsed_data = json.loads(message)
#                 await push_to_kafka(parsed_data)
#             except json.JSONDecodeError as e:
#                 logger.error("Failed to parse message as JSON: %s", str(e))
#                 continue  # Skip this message and continue receiving the next one

#         except websockets.ConnectionClosed:
#             logger.warning("WebSocket connection closed. Attempting to reconnect...")
#             break  # Exit the loop to allow for reconnection
#         except Exception as e:
#             logger.error("Error receiving data: %s", str(e))
#             break  # Exit the loop to allow for reconnection

def process_data(message):
    # logger.info("Processing data ",message)
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
    
    # Get the auth info required for WebSocket connection
    config = await setup_authentication()

    # Create auth header from auth info obtained
    auth_header = f"{config.CLIENT_ID}:{config.ACCESS_TOKEN}"

    retries = 0
    # Establish Websocket Connection
    while True:
        if retries > 0:
            logger.info(f"Retrying WebSocket connection... Attempt #{retries}")
        if retries >= config.WEBSOCKET_MAX_RETRIES:
            logger.error("Max retries reached. Exiting WebSocket client.")
            return
        try:
            async with websockets.connect(
                        config.WEBSOCKET_URL,
                        additional_headers={
                            "Authorization": auth_header
                        }
            ) as ws:
                websocket = ws

                # Subscribe once after connection is established
                await subscribe_to_data(websocket, config.SYMBOL_TICKERS)

                # Start receiving in this same connection
                await data_receiver(websocket)

                # Subscribe to the required stock symbols to receive real-time data
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





'''

# Requirements

Stage-1 


1.) function to establish websocket connection
2.) Data receiver function to receive data from the websocket and push it to kafka
3.) From kafka - we can have different consumers for different data types - one for depth, one for trades, one for quotes etc. Each consumer will read from the respective topic and push the data to the database.



'''