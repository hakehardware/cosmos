from src.utils.logger import logger
from src.utils.helpers import Helpers
from src.utils.parser import Parser
from src.apis import DiscordAPI, DatabaseAPI
from prometheus_client.parser import text_string_to_metric_families
import src.utils.constants as constants
import requests
import os
import json
import sys
import asyncio
import signal
from aiofiles import open as async_open

from dotenv import load_dotenv

load_dotenv()

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

        self.reward_webhook = os.getenv('REWARD_WEBHOOK')

        self.database_api = DatabaseAPI()

    async def _evaluate_log(self, log) -> None:
        event = Parser.get_log_event(log, self.config['farmer_name'])

        # Add Event to Database if it isn't already in there
        if event['Event Type'] != 'Unknown':
            new_event = self.database_api.insert_event(event)

            if new_event:
                if event['Event Type'] == "Farm ID":
                    # Update Farm ID
                    self.database_api.update_farm_id(event)

                elif event['Event Type'] == "Farm Public Key":
                    # Update Farm Public Key                           
                    self.database_api.update_farm_pub_key(event)

                elif event['Event Type'] == "Farm Allocated Space":
                    # Update Farm Allocated Space                            
                    self.database_api.update_farm_alloc_space(event)

                elif event['Event Type'] == 'Farm Directory':
                    # Update Farm Directory
                    self.database_api.update_farm_directory(event)

                elif event['Event Type'] == 'Starting Workers':
                    # Update Farm Workers
                    self.database_api.update_farm_workers(event)

                elif event['Event Type'] == 'Failed to Send Solution':
                    # Update Rewards for Failed Result
                    self.database_api.update_rewards(event)

                elif event['Event Type'] == 'Replotting Complete':
                    # Update Farm Status
                    self.database_api.update_farm_status(event)

                elif event['Event Type'] == 'Replotting Sector':
                    # Update Plots for Replotted Sector
                    self.database_api.update_plotting(event, 1)

                elif event['Event Type'] == 'Synchronizing Piece Cache':
                    # Update Farmer Status
                    self.database_api.update_piece_cache_status(event)

                elif event['Event Type'] == 'Reward':
                    # Update Rewards for Success Result
                    self.database_api.update_rewards(event)
                    await self.send_discord_message('Reward', f'{self.config["farmer_name"]} Received a Reward')

                elif event['Event Type'] == 'Finished Piece Cache Sync':
                    # Update Farmer Status
                    self.database_api.update_piece_cache_status(event)

                elif event['Event Type'] == 'Plotting Resumed':
                    # Update Farm Status
                    self.database_api.update_farmer_status(event)

                elif event['Event Type'] == 'Plotting Paused':
                    # Update Farm Status
                    self.database_api.update_farmer_status(event)

                elif event['Event Type'] == 'Piece Cache Sync':
                    # Update Farmer Status
                    self.database_api.update_piece_cache_sync(event)

                elif event['Event Type'] == 'Plotting Sector':
                    # Update Plots for Plotted Sector
                    self.database_api.update_plotting(event, 0)

    async def _backfill_logs(self) -> None:
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
                await self._evaluate_log(log)

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

    def _monitor_metrics(self):
        try:
            logger.info(f'Getting Farmer Metrics from {self.config["farmer_metrics"]}')
            response = requests.get(f'{self.config["farmer_metrics"]}')
            response.raise_for_status()  # Raises an exception for 4XX/5XX errors

            metrics = []
            for family in text_string_to_metric_families(response.text):
                for sample in family.samples:
                    if 'subspace' in sample.name:
                        metrics.append(sample)
            
            logger.info('Parsing Metrics')
            parsed_metrics = Parser.parse_prometheus_metrics(metrics)

            for farm in parsed_metrics['Farms']:
                logger.info(f'Writing metrics for farm {farm} to db')
                self.database_api.insert_farm_metrics(farm, parsed_metrics['Farms'][farm])

            logger.info(f'Writing metrics for farmer {self.config["farmer_name"]} to db')
            self.database_api.update_farmer_metrics(self.config["farmer_name"], parsed_metrics['Farmer'])

            return parsed_metrics

        except requests.RequestException as e:
            logger.error(f'Error fetching metrics from {self.config["farmer_metrics"]}: {e}')
            return None

    def _add_farm_ids(self) -> None:
        logger.info('Checking metrics endpoint for farms.')
        parsed_metrics = self._monitor_metrics()
        farm_ids = list(parsed_metrics["Farms"].keys())

        logger.info(f'Found {len(farm_ids)} farms.')

        # Add farms if needed
        for farm_id in farm_ids:
            self.database_api.insert_farm(farm_id)

        # Clean out old farms where there is no match in the metrics
        self.database_api.clean_farms(farm_ids)

    async def send_discord_message(self, message_type, message) -> None:
        if message_type == 'Reward':
            await DiscordAPI.send_message(self.reward_webhook, message)

    async def _metrics_loop(self):
        try:
            while True:
                self._monitor_metrics()
                await asyncio.sleep(60)
                
        except asyncio.CancelledError:
            logger.info("Closing Metrics Loop.")
        except Exception as e:
            logger.error("Error in fetch_data task:", exc_info=e)

    async def _logs_loop(self):
        try:
            with open(self.containers["Farmer Log Location"], "r") as f:
                f.seek(0, 2)
                while True:
                    line = f.readline()
                    if line:
                        await self._evaluate_log(json.loads(line.strip()))
                    else:
                        await asyncio.sleep(0.1)
        except FileNotFoundError:
            print("Log file not found.")

        except asyncio.CancelledError:
            logger.info("Closing Log Loop.")
        except Exception as e:
            logger.error("Error in fetch_data task:", exc_info=e)

    def signal_handler(self):
        asyncio.create_task(self.shutdown())

    async def shutdown(self):
        print("Shutting down...")
        tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()

    async def launch(self):
        # Run loops
        loop = asyncio.get_running_loop()

        for signame in {'SIGINT', 'SIGTERM'}:
            loop.add_signal_handler(getattr(signal, signame), self.signal_handler)

        try:
            log_task = asyncio.create_task(self._logs_loop())
            metrics_task = asyncio.create_task(self._metrics_loop())

            await asyncio.gather(log_task, metrics_task)

        except Exception as e:
            logger.error("Error in task:", exc_info=e)

        except asyncio.CancelledError:
            logger.info("Thanks for using Cosmos, have a great day!")

    async def run(self) -> None:
        logger.info(f'Initializing Cosmos {constants.VERSIONS["Cosmos"]}')

        logger.info('Initializing Cosmos DB')
        self.database_api.initialize()

        # Set IDs and Log Locations
        self._set_container_info()

        # Check Version Compatibility
        self._check_version()

        # Insert Farmer Details
        result = self.database_api.insert_farmer_details(self.config['farmer_name'])

        if not result:
            logger.error('Could not create farmer. Exiting')
            sys.exit(1)

        # Perform initial metrics query to add farm_id and farm_index to farms table
        self._add_farm_ids()

        # Backfill Logs
        await self._backfill_logs()

        # Launch Asyncio Loops
        await self.launch()

