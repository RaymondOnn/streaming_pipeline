from loguru import logger as lg
import loguru
import sys
import os


logger_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)

def get_logger():
    """
    Create and configure a Logger instance from loguru.

    Returns:
        Logger: A Logger instance with the configured settings.
    """
    lg.remove()
    # Add a new handler that logs to sys.stderr
    lg.add(
        sys.stderr,
        # Logging level
        level=os.getenv('LOG_LEVEL', 'INFO'),
        # Logging format
        format=logger_format,
        # Colorize the output
        colorize=True,
        # Do not serialize the log records
        serialize=False,
    )
    print(lg)
    return lg

logger = get_logger()

if __name__ == "__main__":
    logger.trace("A trace message.")
    logger.debug("A debug message.")
    logger.info("An info message.")
    logger.success("A success message.")
    logger.warning("A warning message.")
    logger.error("An error message.")
    logger.critical("A critical message.")