'''
The script aims to collect the historical data of a particular stock.
Parameter - StockSymbol
if Parameter = ALL - fetch all the stocks information(heavy process)
if Parameter = sector_name = fetch all stocks of that particular sector

Parameter2 = starttime
Parameter3 = endtime


'''



# Import the required module from the fyers_apiv3 package
from fyers_apiv3 import fyersModel

# Define your Fyers API credentials

client_id = "UAJI3RNQUR-100"  # Replace with your client ID
secret_key = "89HLC4TV8T"
redirect_uri = "https://trade.fyers.in/api-login/redirect-uri/index.html"  # Replace with your redirect URI
response_type = "code" 
grant_type = "authorization_code" 

# The authorization code received from Fyers after the user grants access
auth_code = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhcHBfaWQiOiJVQUpJM1JOUVVSIiwidXVpZCI6IjJlZWRiNWI0MWNmNDQ4ZGZiM2YwZmE0MTVkZWQxY2I3IiwiaXBBZGRyIjoiIiwibm9uY2UiOiIiLCJzY29wZSI6IiIsImRpc3BsYXlfbmFtZSI6IllTMzI5NzciLCJvbXMiOiJLMSIsImhzbV9rZXkiOiJkMDY3ZDg5NDVkZWMyOTJiMTIzZDk2YmQ0NzgwYzRmOTI4NjU4MWI3NWNjMmM2ZDk3MGNiNWIxMCIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImF1ZCI6IltcImQ6MVwiLFwiZDoyXCIsXCJ4OjBcIixcIng6MVwiLFwieDoyXCJdIiwiZXhwIjoxNzcyOTI0ODY1LCJpYXQiOjE3NzI4OTQ4NjUsImlzcyI6ImFwaS5sb2dpbi5meWVycy5pbiIsIm5iZiI6MTc3Mjg5NDg2NSwic3ViIjoiYXV0aF9jb2RlIn0.vMDjSb3IObsf5n1avmWcduCfIgO1x_97XWwU-6M0RRw"


# Create a session object to handle the Fyers API authentication and token generation
session = fyersModel.SessionModel(
    client_id=client_id,
    secret_key=secret_key, 
    redirect_uri=redirect_uri, 
    response_type=response_type, 
    grant_type=grant_type
)

# Set the authorization code in the session object
session.set_token(auth_code)

# Generate the access token using the authorization code
response = session.generate_token()

# Print the response, which should contain the access token and other details
print(response)






# from fyers_apiv3 import fyersModel

# client_id = "UAJI3RNQUR"
# access_token = "eyJ0eXXXXXXXX2c5-Y3RgS8wR14g"
# secret_id = '89HLC4TV8T'

# # Initialize the FyersModel instance with your client_id, access_token, and enable async mode
# fyers = fyersModel.FyersModel(client_id=client_id, is_async=False, token=access_token, log_path="")

# data = {
#     "symbol":"NSE:SBIN-EQ",
#     "resolution":"D",
#     "date_format":"0",
#     "range_from":"1690895316",
#     "range_to":"1691068173",
#     "cont_flag":"1"
# }


# response = fyers.history(data=data)
# print(response)
