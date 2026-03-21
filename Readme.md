## Market Data Pipeline Project
The project aims for the real time stock market data collection, cleaning it, processing it and making it ready for backtesting. As we expand the scope I plan to build different pipelines for different set of indicators, generating buy/sell signals based on different strategies.

### Architecture Design
Link - https://app.diagrams.net/#G1XzVCH1INkYbS2aGT_wJ4cgXVnr5ShD6k#%7B%22pageId%22%3A%22-xqMkZ9DFNTepwFP1atR%22%7D

<img width="815" height="793" alt="image" src="https://github.com/user-attachments/assets/87cb23d4-630b-47b6-aa6d-37e675a0351b" />



## Env variables to be set
* FYERS_CLIENT_ID
* FYERS_SECRET_KEY
* FYERS_USER_PIN
* FYERS_REDIRECT_URI
* FYERS_ACCESS_TOKEN
* FYERS_REFRESH_TOKEN
* LOG_LEVEL=DEBUG

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
* 1.) Download the the archive according to your OS
* 2.) Once downloaded, extract it and place it to the correct path, like under C:// directory
* 3.) Add the path to the bin directory in the systems variables

*Link tot the archives* - https://github.com/protocolbuffers/protobuf/releases  
*Youtube Link* - https://www.youtube.com/watch?v=94P_0-xlZIs


**To create the protobuf files according to your programming language**

* 1.) Create a <filename>.proto file and add the schema in that, usually the organizations provide this schema info with their API documentation
* 2.) Once schemas are ready in the .proto file, then add run the command - "protoc --python_out=. --proto_path=. <filename>.proto"
* 3.) This will create a file <filename>_pb2.py (.py for python)
* 4.) Now we can import this file in our code and extract the binary being received by the websocket according to the datatytpes



# How to execute the data collection
* go to the parent project folder - Market_Data_Pipeline_Project/Data_collector
* run - python -m data_collector.websocket



## References

* About Protbuf - https://protobuf.dev/getting-started/
* FyersAPI TBT Documentation - https://myapi.fyers.in/docsv3#tag/Tbtws/paths/~1TBTWebsocketUsageGuide/get
* Trading Bot Reference project - https://github.com/arvind10799/tradingbot/blob/main/trading_bot/bot/logging_config.py
* Fyers API sample usage (Marketcalls) - https://github.com/marketcalls/fyers-websockets
* Fyers API sample usage (Marketcalls) - https://www.marketcalls.in/python/a-simple-guide-to-using-fyers-tbt-feed-via-websocket-with-protobuf-python-tutorial.html

### Contributor
Sagar Gulati


