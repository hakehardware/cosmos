import argparse
import sys
from src.utils.helpers import Helpers
from src.utils.logger import logger
from src.cosmos import Cosmos


def main():
    # To read the docker logs the user must run the script with sudo
    is_root = Helpers.check_root()
    if not is_root:
        logger.error('To read docker logs you need to run this script with sudo')
        sys.exit(1)

    # Get arguments from user
    parser = argparse.ArgumentParser(description='Load and print YAML configuration.')
    parser.add_argument('config_file', metavar='config_file.yml', type=str,
                        help='path to the YAML configuration file')
    args = parser.parse_args()

    # Parse Config
    config = Helpers.read_yaml_file(args.config_file)

    # If no config file is found, throw error and exit
    if not config:
        logger.error(f'Error loading config from {args.config}. Are you sure you put in the right location?')
        sys.exit(1)

    logger.info(f'Configuration loaded successfully: {config}')

    # Start Cosmos app
    cosmos = Cosmos(config)
    cosmos.run()


if __name__ == "__main__":
    main()