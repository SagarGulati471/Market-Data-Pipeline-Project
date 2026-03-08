## Market Data Pipeline Project
The project aims for the real time stock market data collection, cleaning it, processing it and making it ready for backtesting. As we expand the scope I plan to build different pipelines for different set of indicators, generating buy/sell signals based on different strategies.

## References

### Architecture Design
Link - https://app.diagrams.net/#G1XzVCH1INkYbS2aGT_wJ4cgXVnr5ShD6k#%7B%22pageId%22%3A%22-xqMkZ9DFNTepwFP1atR%22%7D


## Env variables to be set
FYERS_CLIENT_ID
FYERS_SECRET_KEY
FYERS_USER_PIN
FYERS_REDIRECT_URI
FYERS_ACCESS_TOKEN
FYERS_REFRESH_TOKEN
LOG_LEVEL=DEBUG

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


### Contributor
Sagar Gulati