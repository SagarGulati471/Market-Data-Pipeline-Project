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

        try:
            # 1. Check valid access token
            if access_token and self.fyers_auth.is_access_token_valid(access_token):
                logger.info("Access token already exists and is valid.")
                return access_token

            # 2. Try refresh token
            if refresh_token:
                logger.info("Refreshing access token...")
                try:
                    new_token_data = self.fyers_auth.refresh_access_token(
                        refresh_token,
                        user_pin=os.getenv("FYERS_USER_PIN")
                    )
                except Exception:
                    logger.exception("Error while refreshing token.")
                    new_token_data = None

                if new_token_data:
                    new_access_token = new_token_data.get("access_token")
                    new_refresh_token = new_token_data.get("refresh_token")

                    if new_access_token and new_refresh_token:
                        self.config.setEnvVariable("FYERS_ACCESS_TOKEN", new_access_token, overwrite=True, add_to_file=True)
                        self.config.setEnvVariable("FYERS_REFRESH_TOKEN", new_refresh_token, overwrite=True, add_to_file=True)

                        logger.info("Access token refreshed successfully.")
                        return new_access_token

                logger.warning("Refresh token failed, generating new token.")

            # 3. Generate new token (manual step)
            logger.info("Generating new token via auth flow.")
            login_url = self.fyers_auth.generate_auth_code()
            logger.info("Login URL: %s", login_url)

            # ❗ Avoid input in production
            auth_code = input("Paste auth code: ")

            token_data = self.fyers_auth.generate_auth_token(auth_code)

            if token_data:
                self.config.setEnvVariable("FYERS_ACCESS_TOKEN", token_data['access_token'], add_to_file=True)
                self.config.setEnvVariable("FYERS_REFRESH_TOKEN", token_data['refresh_token'], add_to_file=True)

                logger.info("New access token generated.")
                return token_data['access_token']

            logger.error("Failed to generate access token.")
            return None

        except Exception:
            logger.exception("Unexpected error in get_access_token")
            return None
    
            
if __name__ == "__main__":
    token_manager = TokenManager()
    access_token = token_manager.get_access_token()