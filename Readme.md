## Market Data Pipeline Project

The project aims for the real time stock market data collection, cleaning it, processing it and making it ready for backtesting. As we expand the scope I plan to build different pipelines for different set of indicators, generating buy/sell signals based on different strategies.

### Architecture Design

Link - https://app.diagrams.net/#G1XzVCH1INkYbS2aGT_wJ4cgXVnr5ShD6k#%7B%22pageId%22%3A%22-xqMkZ9DFNTepwFP1atR%22%7D

<img width="815" height="793" alt="image" src="https://github.com/user-attachments/assets/87cb23d4-630b-47b6-aa6d-37e675a0351b" />

## Env variables to be set

- FYERS_CLIENT_ID
- FYERS_SECRET_KEY
- FYERS_USER_PIN
- FYERS_REDIRECT_URI
- FYERS_ACCESS_TOKEN
- FYERS_REFRESH_TOKEN
- LOG_LEVEL=DEBUG

## Procedure to gain API authentication

#### Link to the documentation - https://myapi.fyers.in/docsv3

#### Step - 1

Create an app on the fyers platform
Link for creating the app - https://myapi.fyers.in/dashboard
The app_id is the client id
Redirect URI can be - [redirect_uri](https://trade.fyers.in/api-login/redirect-uri/index.html)

#### Step - 2

Get access to the auth code
Link - https://myapi.fyers.in/docsv3#tag/Authentication-and-Login-Flow-User-Apps/paths/~1Authentication%20&%20Login%20Flow%20-%20User%20Apps/patch

#### Step - 3

Generate the authentication token

#### Step - 4

Pass the authentication token to the APIs

# About Protobuf format

### Steps to setup Protobuf

For Windows -

- 1.) Download the the archive according to your OS
- 2.) Once downloaded, extract it and place it to the correct path, like under C:// directory
- 3.) Add the path to the bin directory in the systems variables

_Link tot the archives_ - https://github.com/protocolbuffers/protobuf/releases  
_Youtube Link_ - https://www.youtube.com/watch?v=94P_0-xlZIs

**To create the protobuf files according to your programming language**

- 1.) Create a <filename>.proto file and add the schema in that, usually the organizations provide this schema info with their API documentation
- 2.) Once schemas are ready in the .proto file, then add run the command - "protoc --python_out=. --proto_path=. <filename>.proto"
- 3.) This will create a file <filename>\_pb2.py (.py for python)
- 4.) Now we can import this file in our code and extract the binary being received by the websocket according to the datatytpes

# How to execute the data collection

- go to the parent project folder - Market_Data_Pipeline_Project/Data_collector
- run - python -m data_collector.websocket

## References

- About Protbuf - https://protobuf.dev/getting-started/
- FyersAPI TBT Documentation - https://myapi.fyers.in/docsv3#tag/Tbtws/paths/~1TBTWebsocketUsageGuide/get
- Trading Bot Reference project - https://github.com/arvind10799/tradingbot/blob/main/trading_bot/bot/logging_config.py
- Fyers API sample usage (Marketcalls) - https://github.com/marketcalls/fyers-websockets
- Fyers API sample usage (Marketcalls) - https://www.marketcalls.in/python/a-simple-guide-to-using-fyers-tbt-feed-via-websocket-with-protobuf-python-tutorial.html

## Design -

Seperate Folders for seperate brokers
For each of the broker we will have an entry point file which will when started can actually start polling
The symbols to be polled will be fetched from the ENV file
The data for one broker will be pushed to one Kafka Topic

After receiving the data from the websocket we can transform it into json and try pushing it into the common format so that same consumer pipelines can be used for the brokers data. If data is quite different then we can have an ENV variable which will mention about which consumer to start, so when we will spawn the consumer pod the pod will pick the correct Processing function.

1.)
Data1 stream1 (broker1 - US stock data)
Data1 stream2 (broker2 - Indian stock data)

2.) Both will send to kafka(seperate topics)

3.) These consumers will be running as PODs or docker containers and there will be an entry point which will connect to the Kafka and starts consuming, now for the processing logic there will be a conditional check about which set of data to be processed, Indian stocks or US stocks. If we are successful in transforming the data from both the brokers to the same format then there will be just one logic, else different logics but the code base will remain the same, for consumer pods of both the brokers will contain all the code, but only some of it will be executed.

Things to consider
Will be worth transforming the data after receiving from websocket just to convert it to a common format, it can add additional latency, will just writing seperate processing logics be more

# Project Structure

#### ( April - 16)

## Data collectors from different brokers

- Fyers DataCollector
- FinnHub DataCollector
  Each of these data collectors will have a consumer file which will establish the connection to kafka and push the data
  It will use a dir called kafka service, which will have kafka_service.py file , which contains all wrappers for producer, consumer, topic creation etc

## Kafka-Deployment

This dir contains the kafka and kafka-UI service related data

- docker-compose file
- kafka-data dir - this is a volume mount

# Ingestion Engine

Fyers consumer
Finnhub Consumer
ProcessingFunctions

There are two options either we send the data to kafka in the same format, in this case we need to do some processing before sending to kafka, or second option is that we transform it after receiving the data from kafka. if we do transformation after receiving from kafka then we need to use different consumers, after pre-processing , we can just use common processing/strategy/alerting functions

Also, I am thinking from the deployment purpose also, I am thinking to keep data collectors, kafka, Ingestion Engine to be seperate (ingestion engine can have multiple consumers so we can achieve some parallel processing, and the container of data collectors can have more resources so it can easily push to kafka, additionally all logics will be written using asyncio)

```text
project-root/
├── collectors/
│ ├── fyers/
│ ├── finnhub/
│
├── messaging/
│ ├── kafka/
│ ├── internal_bus/
│
├── ingestion/
│ ├── normalizer.py
│ ├── validators.py
│
├── core/
│ ├── events.py
│ ├── models.py
│
├── strategy/
│ ├── base.py
│ ├── momentum.py
│ ├── mean_reversion.py
│
├── execution/
│ ├── engine.py
│ ├── orderbook.py
│
├── backtesting/
│ ├── engine.py
│
├── infra/
│ ├── docker/
│ ├── kafka/
│
├── configs/
├── logs/

## To start the data collector execute the following in the order -

### Windows

- Start venv - .\venv\Scripts\Activate
- python3 -m data_collectors.fyers.websocket

### MAC

- cd Market_Data_Pipeline_Project/Project-Root
- python -m data_collectors.fyers.websocket

### Contributor

Sagar Gulati
