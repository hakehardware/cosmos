from src.utils.logger import logger
from src.utils.helpers import Helpers
import src.utils.constants as constants
from src.utils.parser import Parser
import json
import re
import time
import argparse
import sys
import datetime
import threading
import requests
from src.utils.publisher import Publisher
from prometheus_client.parser import text_string_to_metric_families

class Cosmos:
    def __init__(self, config) -> None:
        self.config = config

        self.node_container_id = None
        self.node_log_location = None
        self.farmer_container_id = None
        self.farmer_log_location = None

        self.farmer_state = {
            'farms': [],
            'farm_status': None
        }
        self.node_state = None

    def _fetch_metrics(self, url):
        try:
            print(f'{url}/metrics')
            response = requests.get(f'{url}/metrics')
            response.raise_for_status()  # Raises an exception for 4XX/5XX errors
            return response.text
        except requests.RequestException as e:
            print(f"Error fetching metrics from {url}: {e}")
            return None
        
    def _parse_metrics(self, metrics_data):
        metrics = []
        for family in text_string_to_metric_families(metrics_data):
            for sample in family.samples:
                if 'subspace' in sample.name:
                    metrics.append(sample)
        return metrics

    def _set_container_info(self):

        # Set Node Container ID & Log Location
        if config['node_container_name']:
            self.node_container_id = Helpers.get_full_container_id(config['node_container_name'])
            self.node_log_location = f'/var/lib/docker/containers/{self.node_container_id}/{self.node_container_id}-json.log'
            logger.info(f'Node Container ID: {self.node_container_id}')
            logger.info(f'Node Log Location: {self.node_log_location}')

        if config['farmer_container_name']:
            self.farmer_container_id = Helpers.get_full_container_id(config['farmer_container_name'])
            self.farmer_log_location = f'/var/lib/docker/containers/{self.farmer_container_id}/{self.farmer_container_id}-json.log'
            logger.info(f'Farmer Container ID: {self.farmer_container_id}')
            logger.info(f'Farmer Log Location: {self.farmer_log_location}')

    # def _get_logs(self, log_file_path):
    #     logs = []
    #     with open(log_file_path, 'r') as file:
    #         lines = file.readlines()
    #         # Start reading from the end of the file
            
    #         for line in lines:
    #             try:
    #                 log_entry = json.loads(line)
    #                 logs.append(log_entry)
    #             except json.JSONDecodeError as e:
    #                 print(f"Error decoding JSON: {e}")

    #     return logs
    
    def _parse_existing_logs(self):
        if self.node_log_location:
            node_logs = self._get_logs(self.node_log_location)
            logger.info(f'Node Log Count: {len(node_logs)}')

        if self.farmer_log_location:
            farmer_logs = self._get_logs(self.farmer_log_location)
            logger.info(f'Farmer Log Count: {len(farmer_logs)}')
            for index, log in enumerate(farmer_logs):
                analyzed_log = Parser.analyze_log(log)

                # if analyzed_log['event_type'] == 'Unknown':
                #     print(analyzed_log)
                
                if analyzed_log['event_type'] == 'New Farm Identified':
                    farm_index = analyzed_log['data']['farm_index']
                    id = Parser.get_farm_id(farmer_logs[index+1])
                    allocated_space = Parser.get_allocated_space(farmer_logs[index+4])
                    self.farmer_state['farms'].append({
                        'allocated_space': allocated_space, 
                        'id': id,
                        'percentage_complete': 0,
                        'current_sector': 0
                    })
                    logger.info(f'Added new disk to farm state: {self.farmer_state["farms"][farm_index]}')

                if analyzed_log['event_type'] == 'Synchronizing Piece Cache':
                    self.farmer_state['farm_status'] = analyzed_log['event_type']
                    logger.info(f'New Farm State: {self.farmer_state["farm_status"]}')

                if analyzed_log['event_type'] == 'Plotting Sector' and self.farmer_state['farm_status'] != 'Plotting Sector':
                    self.farmer_state['farm_status'] = analyzed_log['event_type']
                    logger.info(f'New Farm State: {self.farmer_state["farm_status"]}')

                if analyzed_log['event_type'] == 'Plotting Paused' and self.farmer_state['farm_status'] != 'Plotting Paused':
                    self.farmer_state['farm_status'] = analyzed_log['event_type']
                    logger.info(f'New Farm State: {self.farmer_state["farm_status"]}')

                if analyzed_log['event_type'] == 'Plotting Sector':
                    farm_index = analyzed_log['data']['farm_index']
                    percentage_complete = analyzed_log['data']['percentage_complete']
                    current_sector = analyzed_log['data']['current_sector']
                    self.farmer_state['farms'][farm_index]['percentage_complete'] = percentage_complete
                    self.farmer_state['farms'][farm_index]['current_sector'] = current_sector

        logger.info(f'Loaded Farmer State: {json.dumps(self.farmer_state, indent=4)}')

    def run(self):
        logger.info('Initializing Cosmos')

        # Set IDs and Log Locations
        self._set_container_info()

        # Backfill Logs
        self._parse_existing_logs()

        # Monitor Logs

        # Monitor Metrics

        # for farm in config['farmer_endpoints']:
        #     response = self._fetch_metrics(farm['endpoint'])
        #     metrics = self._parse_metrics(response)
        #     print(metrics)
















KEY_EVENTS = [
    'Plotting sector',
    'Piece cache sync',
    'pausing plotting',
    'resuming plotting',
    'Finished piece cache synchronization',
]

def get_plot_times(logs):
    if not logs:
        return 0  # Return 0 if there are no logs

    # Filter logs with event_type "Plotting Sector"
    plotting_logs = [log for log in logs if log['event_type'] == 'Plotting Sector']
    
    if not plotting_logs:
        return 0  # Return 0 if there are no "Plotting Sector" logs

    # Sort the filtered logs by timestamp
    plotting_logs.sort(key=lambda x: x['time'])
    
    # Extract the timestamps of the earliest and latest logs
    earliest_time = plotting_logs[0]['time']  # Remove the 'Z' character
    latest_time = plotting_logs[-1]['time']

    # Calculate the time difference in minutes
    time_difference_minutes = (latest_time - earliest_time).total_seconds() / 60

    # Calculate the rate of "Plotting Sector" logs per minute
    sectors_per_minute = round(time_difference_minutes / len(plotting_logs), 3)

    return sectors_per_minute

def analyze_log(log):
    event = None
    log_time = log['time'].split('.')[0]

    if KEY_EVENTS[0] in log['log']:
        pattern = r'disk_farm_index=(\d+).*?(\d+\.\d+)% complete.*?sector_index=(\d+)'        
        match = re.search(pattern, log['log'])
        if match:
            event = {
                'event_type': 'Plotting Sector',
                'time': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
                'data': {
                    'disk_farm_index': match.group(1),
                    'percentage_complete': match.group(2),
                    'sector_index': match.group(3)
                }
            }
    elif KEY_EVENTS[1] in log['log']:
        # Define a regex pattern to match the percentage complete
        pattern = r'Piece cache sync (\d+\.\d+)% complete'

        # Use re.search() to find the match
        match = re.search(pattern, log['log'])

        if match:
            event = {
                'event_type': 'Syncing Piece Cache',
                'time': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
                'data': {
                    'percentage_complete': float(match.group(1)),
                }
            }
    elif KEY_EVENTS[2] in log['log']:
        event = {
                'event_type': 'Plotting Paused',
                'time': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
        }
    elif KEY_EVENTS[3] in log['log']:
        event = {
            'event_type': 'Plotting Resumed',
            'time': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
        }
    elif KEY_EVENTS[4] in log['log']:
        event = {
            'event_type': 'Finished Piece Cache Sync',
            'time': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
        }
    else:
        event = {
            'event_type': 'Unknown',
            'time': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S")
        }
        logger.info(log)
    return event   

def tail_logs(log_file_path, tail):
    logs = []
    with open(log_file_path, 'r') as file:
        lines = file.readlines()
        # Start reading from the end of the file
        
        for line in lines:
            try:
                log_entry = json.loads(line)
                logs.append(log_entry)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

    return logs

def parse_existing_log(logs):
    events = []
    for log in logs:
        event = analyze_log(log)
        events.append(event)

    return events

def watch_logs(log_file_path):
    logger.info(f'Watching logs at {log_file_path}')

    publisher = Publisher()


    with open(log_file_path, 'r') as file:
        # Move the pointer to the end of the file
        file.seek(0,2)
        
        while True:
            line = file.readline()
            if not line:
                time.sleep(0.1)
                continue
            else:
                event = analyze_log(json.loads(line))

                if event['event_type'] == 'Plotting Sector':
                    print(event)
                    publisher.publish_sector_data(event['data'])
                elif event['event_type'] == 'Syncing Piece Cache':
                    print(event)
                    publisher.publish_piece_cache_data(event['data'])

            time.sleep(1)
            # try:
            #     line = file.readline()
            #     if not line:
            #         time.sleep(0.1)  # Sleep briefly to avoid busy waiting
            #         continue
                
            #     else:
            #         logger.info(analyze_log(line))
            #     yield line
            # except Exception as e:
            #     print(e)

def run(config):
    logger.info(f'Starting Prometheus Client')
    start_http_server(config["prometheus_client_port"])

    logger.info('Starting Parser')
    farmer_log_path, node_log_path, tail = config['farmer_log_path'], config['node_log_path'], config['tail']

    logger.info(f'Tailing the last {tail} logs of the log file.')
    tailed_logs = tail_logs(farmer_log_path, tail)

    logger.info('Analyzing tailed logs')
    events = parse_existing_log(tailed_logs)

    logger.info('Tailed logs complete, watching log file')
    watch_logs(farmer_log_path)




if __name__ == "__main__":
    # Must be run as root in order to access docker logs
    if not Helpers.check_root():
        logger.error('You must run as root')
        sys.exit(1)

    # Get args in order to load the config
    parser = argparse.ArgumentParser(description="Process command line arguments.")
    parser.add_argument("-c", "--config", help="Path to a config file", type=str, required=True)
    args = parser.parse_args()

    # Load the config
    config = Helpers.read_yaml_file(args.config)
    logger.info(f'Loaded Config: {config}')
    
    if not config:
        logger.error(f'Error loading config from {args.config}. Are you sure you put in the right location?')
        sys.exit(1)

    cosmos = Cosmos(config)
    cosmos.run()



