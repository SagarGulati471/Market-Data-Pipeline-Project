#https://pypi.org/project/websocket_client/
import websocket

# def on_message(ws, message):
#     print(message)

# def on_error(ws, error):
#     print(error)

# def on_close(ws):
#     print("### closed ###")

# def on_open(ws):
#     ws.send('{"type":"subscribe","symbol":"AAPL"}')
#     ws.send('{"type":"subscribe","symbol":"AMZN"}')
#     ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
#     ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

# if __name__ == "__main__":
#     # websocket.enableTrace(True)
#     ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=d7eqsshr01qi33g7aps0d7eqsshr01qi33g7apsg",
#                               on_message = on_message,
#                               on_error = on_error,
#                               on_close = on_close)
#     ws.on_open = on_open
#     ws.run_forever()



import asyncio
import websockets

async def client():
    async with websockets.connect("wss://ws.finnhub.io?token=d7eqsshr01qi33g7aps0d7eqsshr01qi33g7apsg") as websocket:
        await websocket.send("Hello Server!")
        print(await websocket.recv())

if __name__ == "__main__":
    asyncio.run(client())
