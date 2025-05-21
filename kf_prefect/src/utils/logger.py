"""A general purpose logger for the project."""

import logging


def get_logger(name: str = __name__) -> logging.Logger:
    """
    Returns a logger with the specified name and a basic configuration.
    The logger will log messages to the console with a level of INFO or higher.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Create a console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Create a formatter and set it for the handler
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch.setFormatter(formatter)

    # Add the handler to the logger
    logger.addHandler(ch)

    return logger
