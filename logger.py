import logging


def with_logger(cls):
    if not hasattr(cls, 'logger') or cls.__name__ != cls.logger.logger_name:
        logger = logging.getLogger(cls.__name__)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        setattr(cls, 'logger_name', cls.__name__)
        setattr(cls, 'logger', logger)
    return cls
