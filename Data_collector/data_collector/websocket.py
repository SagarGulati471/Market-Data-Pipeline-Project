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

# Initialize the logger
setup_logger()
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
    
    subscribe_msg = {
                        "type": 1,
                        "data": {
                            "subs": 1,
                            "symbols": list(symbol_tickers),
                            "mode": "depth",
                            "channel": "1",
                        },
                    }

    logger.info(f"Sbscribing to symbols: {symbol_tickers}")
    try:
        await websocket.send(json.dumps(subscribe_msg))
    except Exception as e:
        logger.error(f"Exception occured while subcribing to symbols: {e}")


async def push_to_kafka(data):
    # Placeholder function to push data to Kafka
    logger.info("Pushing data to Kafka: %s", data)


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
                parsed_data = json.loads(message)
                await push_to_kafka(parsed_data)
            except json.JSONDecodeError as e:
                logger.error("Failed to parse message as JSON: %s", str(e))
                continue  # Skip this message and continue receiving the next one

        except websockets.ConnectionClosed:
            logger.warning("WebSocket connection closed. Attempting to reconnect...")
            break  # Exit the loop to allow for reconnection
        except Exception as e:
            logger.error("Error receiving data: %s", str(e))
            break  # Exit the loop to allow for reconnection


async def main():

    # Establish WebSocket connection and subscribe to data
    logger.info("Connecting to WebSocket...")
    
    # Get the auth info required for WebSocket connection
    config = await setup_authentication()

    # Create auth header from auth info obtained
    auth_header = f"{config.CLIENT_ID}:{config.ACCESS_TOKEN}"

    # Establish Websocket Connection
    while True:
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
            logger.error("Error connecting to WebSocket: %s", str(e))
            return
        
        

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