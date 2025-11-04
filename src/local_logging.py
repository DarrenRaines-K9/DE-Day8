# Logging Configuration Module - Sets up a standardized logger for the application

import logging


def get_logger():
    # Creates and configures logger with INFO level and timestamp formatting
    logging.basicConfig(
        level=logging.INFO,  # Log INFO and above (INFO, WARNING, ERROR, CRITICAL)
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Output logs to console
        ],
    )
    logger = logging.getLogger()
    return logger
