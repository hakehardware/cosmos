from src.utils.logger import logger
from src.utils.helpers import Helpers

if __name__ == "__main__":
    logger.info('starting')
    config = Helpers.read_yaml_file('example.config.yml')
    logger.info(config)