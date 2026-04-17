# Import the required module from the fyers_apiv3 package
from fyers_apiv3 import fyersModel
import requests
import os
import logging
from utils.logger import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

class FyersAuth:
    def __init__(self, config):

        self.client_id = config.CLIENT_ID
        self.secret_key = config.SECRET_KEY
        self.redirect_uri = config.REDIRECT_URI

    # Method to generate the authorization code
    # Returns the redirect URL for the user to authenticate and grant access
    def generate_auth_code(self): 

        session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            response_type="code"
        )
        response = session.generate_authcode()
        return response
    

    def generate_auth_token(self, auth_code):
        
        session = fyersModel.SessionModel(
            client_id=self.client_id,
            secret_key=self.secret_key,
            redirect_uri=self.redirect_uri,
            response_type="code",
            grant_type="authorization_code"
        )
        session.set_token(auth_code)
        token_response = session.generate_token()
        return token_response

    def refresh_access_token(self, refresh_token, user_pin):
       
        payload = {
            "grant_type": "refresh_token",
            "appIdHash": "",
            "refresh_token":refresh_token,
            "pin": user_pin 
        }

        headers = {
            "Content-Type": "application/json",
        }

        url = 'https://api-t1.fyers.in/api/v3/validate-refresh-token'
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()  # Return the JSON response from the API
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred: {e}")
            return None  # Return None or handle the error as needed

    def  is_access_token_valid(self, access_token):
        # This can involve making a get profile API call or checking the token's expiry time
        # For demonstration, we'll assume the token is valid for a certain period and return True

        fyers = fyersModel.FyersModel(client_id=self.client_id, is_async=False, token=access_token, log_path="")

        # Make a request to get the user profile information
        response = fyers.get_profile()
        logger.debug(f"Token validation response: {response}")

        if 'data' in response and response['code'] == 200:
            return True
    
        return False



if __name__ == "__main__":
    
    # ****** Placeholder for testing the FyersAuth class *******
    # import sys
    # sys.path.append("..")
    # from config.config import Config
    # config = Config()
    # fyers_auth = FyersAuth(config)    
    # access_token='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsiZDoxIiwiZDoyIiwieDowIiwieDoxIiwieDoyIl0sImF0X2hhc2giOiJnQUFBQUFCcHJSUDV2a0REa1pHa19kVXhacFZCYVV4OWVYSFhQN1BvaTFRc09PWUdJLVZPWFlIWkxRU2lYbHhNeFY4QU84dmhZSDZwU3JJM1V3N2d6MDNtX1NUdjBmZVZiWWJfUnV1bUdQdVVpdm02dFhtMWtaaz0iLCJkaXNwbGF5X25hbWUiOiIiLCJvbXMiOiJLMSIsImhzbV9rZXkiOiJiZGEyMzhmYjFhOTE5ZDRhYzdmNzVhMGM3ZWI2NDQ3YzlhZTg0MDYxYWI1YjllNTEyMTdkNTBhZSIsImlzRGRwaUVuYWJsZWQiOiJOIiwiaXNNdGZFbmFibGVkIjoiTiIsImZ5X2lkIjoiWVMzMjk3NyIsImFwcFR5cGUiOjEwMCwiZXhwIjoxNzczMDE2MjAwLCJpYXQiOjE3NzI5NTA1MjEsImlzcyI6ImFwaS5meWVycy5pbiIsIm5iZiI6MTc3Mjk1MDUyMSwic3ViIjoiYWNjZXNzX3Rva2VuIn0.7DrB_sUR77cR8j4pKLyF1I6VZ_LlLnYzsXSrMadwd40'
    # fyers_auth.is_access_token_valid(access_token)
    # # auth_code_response = fyers_auth.generate_auth_code()
    # print(auth_code_response)
    pass