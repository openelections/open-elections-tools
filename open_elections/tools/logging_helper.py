import logging

LOG_LEVEL = logging.INFO
HANDLERS = []

logging.basicConfig(level=LOG_LEVEL,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M:%S')


def get_logger(name: str):
    """
    Returns a logger with the given name
    :param name:
    :return:
    """
    logger = logging.getLogger(name)

    for handler in HANDLERS:
        logger.addHandler(handler)

    logger.setLevel(LOG_LEVEL)
    return logger
