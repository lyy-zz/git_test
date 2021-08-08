
import os
import logging


def get_logger(name=None):
    """
    Util function to set up a simple logger.
    The loglevel is fetched from the LOGLEVEL
    env. variable or INFO as default.
    Args:
        name: Name of the logger
    """
    logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log = logging.getLogger(__name__)
    return log


logger = get_logger()