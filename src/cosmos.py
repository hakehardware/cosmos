from src.utils.logger import logger
from src.utils.helpers import Helpers
from src.utils.parser import Parser
from src.apis import DiscordAPI

from prometheus_client.parser import text_string_to_metric_families
import src.utils.constants as constants
import requests
import json

class Cosmos:
    def __init__(self, config) -> None:
        self.config = config

        self.containers = {
            'Node Log Location': None,
            'Node Container ID': None,
            'Node Version': None,
            'Farmer Log Location': None,
            'Farmer Container ID': None,
            'Farmer Version': None
        }

        self.farmer_state = {
            'Farms': [],
            'Farm Status': None
        }

    def _backfill_logs(self) -> None:
        # Backfill Node Logs
        if self.containers["Node Log Location"]:
            logger.info('Backfilling Node Logs')
            node_logs = Helpers.get_docker_logs(self.containers["Node Log Location"])
            logger.info(f'Node Log Count: {len(node_logs)}')
            
            for log in node_logs:
                event = Parser.get_log_event(log)
                logger.info(event)
        
        # Backfill Farmer Logs
        if self.containers["Farmer Log Location"]:
            logger.info('Backfilling Farmer Logs')
            farmer_logs = Helpers.get_docker_logs(self.containers["Farmer Log Location"])
            logger.info(f'Farmer Log Count: {len(farmer_logs)}')

            for log in farmer_logs:
                event = Parser.get_log_event(log)

    def _set_container_info(self) -> None:
        # Get the Node information if applicable
        if 'node_container_name' in self.config:
            container_id = Helpers.get_full_container_id(self.config["node_container_name"])
            if container_id:
                self.containers["Node Container ID"] = container_id
                self.containers["Node Log Location"] = f'/var/lib/docker/containers/{self.containers["Node Container ID"]}/{self.containers["Node Container ID"]}-json.log'
                logger.info(f'Node Container ID: {self.containers["Node Container ID"]}')
                logger.info(f'Node Log Location: {self.containers["Node Log Location"]}')

                # Get the docker image version
                container_version = Helpers.get_container_image_version(self.config["node_container_name"])
                if container_version:
                    self.containers["Node Version"] = container_version
                    logger.info(f'Node Version: {self.containers["Node Version"]}')
            else:
                logger.info(f'Node will not be monitored.')
        else:
            logger.info(f'No Node info in config. Node will not be monitored.')

        # Get the Farmer information if applicable
        if 'farmer_container_name' in self.config:
            container_id = Helpers.get_full_container_id(self.config["farmer_container_name"])
            if container_id:
                self.containers["Farmer Container ID"] = container_id
                self.containers["Farmer Log Location"] = f'/var/lib/docker/containers/{self.containers["Farmer Container ID"]}/{self.containers["Farmer Container ID"]}-json.log'
                logger.info(f'Farmer Container ID: {self.containers["Farmer Container ID"]}')
                logger.info(f'Farmer Log Location: {self.containers["Farmer Log Location"]}')

                # Get the docker image version
                container_version = Helpers.get_container_image_version(self.config["farmer_container_name"])
                if container_version:
                    self.containers["Farmer Version"] = container_version
                    logger.info(f'Farmer Version: {self.containers["Farmer Version"]}')

            else:
                logger.info(f'Farmer will not be monitored.')
        else:
            logger.info(f'No Farmer info in config. Farmer will not be monitored.')

    def _check_version(self) -> None:
        # Each version of Cosmos is built for a specific image version - warn users if they are not using the same
        logger.info('Checking version compatibility')

        if self.containers["Node Version"]:
            if self.containers["Node Version"] != constants.VERSIONS["Node Version"]:
                logger.warn(f'You are running {self.containers["Node Version"]}. For the best experience use {constants.VERSIONS["Node Version"]}')
        
        if self.containers["Farmer Version"]:
            if self.containers["Farmer Version"] != constants.VERSIONS["Farmer Version"]:
                logger.warn(f'You are running {self.containers["Farmer Version"]}. For the best experience use {constants.VERSIONS["Farmer Version"]}')

    def _monitor_metrics(self) -> None:
        try:
            response = requests.get(f'{self.config["farmer_metrics"]}')
            response.raise_for_status()  # Raises an exception for 4XX/5XX errors

            metrics = []
            for family in text_string_to_metric_families(response.text):
                for sample in family.samples:
                    if 'subspace' in sample.name:
                        metrics.append(sample)
            
            for metric in metrics:
                logger.info(metric)

        except requests.RequestException as e:
            logger.error(f'Error fetching metrics from {self.config["farmer_metrics"]}: {e}')
            return None
        
    def send_discord_message(self, message_type, message) -> None:
        if message_type == 'Reward':
            DiscordAPI.send_discord_message(self.config["reward_webhook"], message)


    def run(self) -> None:
        logger.info(f'Initializing Cosmos {constants.VERSIONS["Cosmos"]}')

        # Set IDs and Log Locations
        self._set_container_info()

        # Check Version Compatibility
        self._check_version()

        # Backfill Logs
        #self._backfill_logs()

        # Pull in Prometheus Metrics
        self._monitor_metrics()



