import logging
import os


def setup_logger():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True
    )




# Process starts
#       ↓
# First logging configuration wins
#       ↓
# All later basicConfig() calls ignored
#       ↓
# Unless force=True is used


# Logging levels: DEBUG < INFO < WARNING < ERROR < CRITICAL
# If LOG_LEVEL is set to INFO, then DEBUG messages will not be shown, but INFO and above will be shown.


# Important Note:
# if we print print(logging.getLogger().handlers) in setup_logger() then we see which other modules like urllib3 or requests have already configured logging and added handlers. This means that if we call logging.basicConfig() without force=True, it will be ignored because handlers are already present. By using force=True, we ensure that our logging configuration takes precedence and is applied regardless of any previous configurations made by other libraries.
# The logging.basicConfig() is called once only in the current process, and the first call to it will set up the logging configuration. Any subsequent calls to logging.basicConfig() will be ignored unless the force=True parameter is used, which forces the reconfiguration of the logging system. This means that if you have multiple calls to logging.basicConfig() in your code, only the first one will take effect unless you explicitly use force=True in subsequent calls.


# A handler is an object responsible for sending logs to some destination.

# Common destinations:

# Handler	Where logs go
# StreamHandler	console / terminal [default if no handlers are configured]
# FileHandler	file
# RotatingFileHandler	rotating log files
# HTTPHandler	send logs to API
# SysLogHandler	system logs


# force = True
# remove existing handlers
# add new handler
# set level