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
from prometheus_client import start_http_server
from prometheus_client.parser import text_string_to_metric_families

class Cosmos:
    def __init__(self, config) -> None:
        self.config = config
        self.node_state = {}
        self.farm_state = {
            'peers': 0,
            'piece_cache_status': 0,
            'plotting_status': [],
            'replotting_status': []
            # 'plotting_speed_mib': [],
            # 'current_sector': 0,
            # 'plotting_sectors_per_min': 0
        }
        self.publisher = Publisher()

    def _load_old_logs(self, log_file_path):
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

    def _parse_old_logs(self):
        logger.info('Loading Node Logs')
        # node_logs = self._load_old_logs(self.config['node_log_path'])

        logger.info('Loading Farmer Logs')
        farmer_logs = self._load_old_logs(self.config['farmer_log_path'])

        logger.info('Analyzing Logs')
        for log in farmer_logs:
            event = Parser.analyze_log(log)
            self._evaluate_event(event)

    def _evaluate_event(self, event):
        if event['event_type'] == 'Plotting Sector':
            if len(self.farm_state['plotting_status']) > int(event['data']['disk_farm_index']):
                self.farm_state['plotting_status'][int(event['data']['disk_farm_index'])] = event['data']
            else:
                self.farm_state['plotting_status'].append(event['data'])
            
            print(self.farm_state)

        elif event['event_type'] == 'Syncing Piece Cache':
            self.farm_state['piece_cache_status'] = event['data']['percentage_complete']

            print(self.farm_state)

        elif event['event_type'] == 'Finished Piece Cache Sync':
            self.farm_state['piece_cache_status'] = 100.00
            print(self.farm_state)

    def _fetch_metrics(self, url):
        try:
            response = requests.get(url)
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

    def _publish(self):
        self.publisher.publish_farmer(self.farm_state)

    def _monitor_logs(self, log_file_path):
        logger.info(f'Watching logs at {log_file_path}')
        self._publish()

        with open(log_file_path, 'r') as file:
            # Move the pointer to the end of the file
            file.seek(0,2)

            while True:
                line = file.readline()
                if not line:
                    time.sleep(0.1)
                    continue
                else:
                    event = Parser.analyze_log(json.loads(line))
                    self._evaluate_event(event)
                    self._publish()

    def run(self):
        logger.info('Initializing Cosmos')
        logger.info('Loading existing logs...')
        start_http_server(config["prometheus_client_port"])

        self._parse_old_logs()

        self._monitor_logs(self.config['farmer_log_path'])

        # thread1 = threading.Thread(target=monitor_log_file, args=(log_file1,))

        # metrics = self.fetch_metrics('http://172.19.0.202:9615/metrics')
        # metrics_parsed = self.parse_metrics(metrics)
        # print(metrics_parsed)
















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

    parser = argparse.ArgumentParser(description="Process command line arguments.")
    parser.add_argument("-c", "--config", help="Path to a config file", type=str, required=True)
    args = parser.parse_args()

    config = Helpers.read_yaml_file(args.config)

    if not config:
        logger.error(f'Error loading config from {args.config}. Are you sure you put in the right location?')
        sys.exit(1)

    logger.info(f'Loaded Config: {config}')

    cosmos = Cosmos(config)
    cosmos.run()



