import logging
import os


PROJECT_LOGGERS = (
    "config",
    "consumers",
    "data_collectors",
    "messaging",
    "storage",
    "utils",
)

THIRD_PARTY_LOGGERS = (
    "aiokafka",
    "asyncio",
    "asyncpg",
    "fyers_apiv3",
    "urllib3",
    "websockets",
)


def _get_log_level(env_var: str, default: str) -> int:
    level_name = os.getenv(env_var, default).upper()
    level = getattr(logging, level_name, None)
    if isinstance(level, int):
        return level

    fallback = getattr(logging, default.upper(), logging.INFO)
    logging.getLogger(__name__).warning(
        "Invalid log level %r for %s. Falling back to %s.",
        level_name,
        env_var,
        default.upper(),
    )
    return fallback


def setup_logger():
    app_log_level = _get_log_level("LOG_LEVEL", "INFO")
    third_party_log_level = _get_log_level("THIRD_PARTY_LOG_LEVEL", "WARNING")
    kafka_log_level = _get_log_level("KAFKA_LOG_LEVEL", "WARNING")

    logging.basicConfig(
        level=third_party_log_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        force=True
    )

    for logger_name in PROJECT_LOGGERS:
        logging.getLogger(logger_name).setLevel(app_log_level)

    for logger_name in THIRD_PARTY_LOGGERS:
        logging.getLogger(logger_name).setLevel(third_party_log_level)

    logging.getLogger("aiokafka").setLevel(kafka_log_level)




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
