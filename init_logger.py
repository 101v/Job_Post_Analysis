import logging
import logging.config
import yaml


def init_logger() -> None:
    with open('log_config.yaml', 'r') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)

    logger = logging.getLogger(__name__)
    logger.info('Logger initiated')
