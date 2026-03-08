from fyers_auth.fyers_auth import FyersAuth
import os
from config.config import Config
import logging
from utils.logger import setup_logger

setup_logger()
logger = logging.getLogger(__name__)

class TokenManager:
    
    config = Config()
    def __init__(self):
        self.fyers_auth = FyersAuth(config=self.config)
    
    def get_access_token(self):

        access_token = self.config.ACCESS_TOKEN
        refresh_token = self.config.REFRESH_TOKEN

        if access_token:
            if self.fyers_auth.is_access_token_valid(access_token):
                logger.info("Access token already exists and is valid.")
                return access_token
        elif refresh_token:
                new_token_data = self.fyers_auth.refresh_access_token(refresh_token, user_pin=os.getenv("FYERS_USER_PIN"))
                if new_token_data:
                    new_access_token = new_token_data.get("access_token")
                    new_refresh_token = new_token_data.get("refresh_token")
                    if new_access_token and new_refresh_token:
                        self.config.setEnvVariable("FYERS_ACCESS_TOKEN", new_access_token, overwrite=True, add_to_file=True)
                        self.config.setEnvVariable("FYERS_REFRESH_TOKEN", new_refresh_token, overwrite=True, add_to_file=True)
                        logger.info("Access token already exists but is invalid. Refreshed access token using refresh token.")
                        return new_access_token
                    else:
                        logger.error("Access token refresh failed: Missing access or refresh token in response.")
                else:
                    logger.error("Failed to refresh access token.")
        else:

            logger.info("No valid access token or refresh token found. Please authenticate.")
            login_url = self.fyers_auth.generate_auth_code()
            logger.info("Open this URL and login:\n\n%s", login_url)
            auth_code = input("Paste auth code: ")

            token_data = self.fyers_auth.generate_auth_token(auth_code)
            if token_data:
                logger.info("Access Token: %s", token_data['access_token'])
                self.config.setEnvVariable("FYERS_ACCESS_TOKEN", token_data['access_token'], add_to_file=True)
                self.config.setEnvVariable("FYERS_REFRESH_TOKEN", token_data['refresh_token'], add_to_file=True)
                logger.info("Generated new access token using auth code.")
                return token_data['access_token']
            else:
                logger.error("Failed to generate new access token.")
                return None
            
if __name__ == "__main__":
    token_manager = TokenManager()
    access_token = token_manager.get_access_token()