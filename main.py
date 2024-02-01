from src.utils.logger import logger
from src.utils.helpers import Helpers
import json
import re
import time
import argparse
import sys
import datetime

from src.utils.publisher import Publisher
from prometheus_client import start_http_server
# from collections import deque
# import dateutil.parser
# import numpy as np

# def handle_line(line):
#     data = json.loads(line)
#     log_message = data.get("log", "")
#     log_time = data.get("time", "")

#     # Regular expression patterns for different log types
#     pattern1 = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*\[\w+\] 💤 Idle \((\d+) peers\), best: #(\d+)')
#     pattern2 = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*\[\w+\] ✨ Imported #(\d+)')
#     pattern3 = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*\[\w+\] 🎁 Prepared block for proposing at (\d+) \((\d+) ms\)')

#     match1 = pattern1.search(log_message)
#     match2 = pattern2.search(log_message)
#     match3 = pattern3.search(log_message)

#     result = {"time": log_time}

#     # Check which pattern matches and extract data
#     if match1:
#         result["peers"] = int(match1.group(1))
#         result["best"] = int(match1.group(2))
#     elif match2:
#         result["imported"] = int(match2.group(1))
#     elif match3:
#         result["block"] = int(match3.group(1))
#         result["latency"] = match3.group(2) + " ms"

#     return result

# def tail_file(log_path, lines):
#     with open(log_path, 'r') as file:
#         return deque(file, maxlen=lines)
    
# def get_average_sector_time():


#     # Sample log entries
#     log_entries = [
#         # ... (your log entries here)
#     ]

#     # Function to extract time from a log entry
#     def extract_time(entry):
#         data = json.loads(entry)
#         return dateutil.parser.isoparse(data['time'])

#     # Extract times from log entries
#     times = [extract_time(entry) for entry in log_entries]

#     # Calculate time differences between consecutive entries (in seconds)
#     time_diffs = [(times[i] - times[i-1]).total_seconds() for i in range(1, len(times))]

#     # Calculate the average time difference
#     average_time_diff = np.mean(time_diffs)

#     print(f"Average time between plots: {average_time_diff} seconds")


# def follow_log(file_name):
#     with open(file_name, 'r') as file:
#         # Move the pointer to the end of the file
#         file.seek(0,2)
        
#         while True:
#             line = file.readline()
#             if not line:
#                 time.sleep(0.1)  # Sleep briefly to avoid busy waiting
#                 continue
            
#             # else:
#             #     logger.info(handle_line(line))
#             yield line
# 2024-01-31T14:33:50.798806Z [Consensus] 🗳️ Claimed vote at slot slot=7935042
# 2024-01-31T14:26:18.826774Z [Consensus] ✨ Imported #1342113 (0x0703…e6c8)    
# 2024-01-31T14:26:12.702283Z [Consensus] 💤 Idle (6 peers), best: #1342110 (0x965f…530e), finalized #1180565 (0xa5a2…e408), ⬇ 23.3kiB/s ⬆ 13.1kiB/s    



# if __name__ == "__main__":
#     logger.info('starting')
#     config = Helpers.read_yaml_file('example.config.yml')
#     logger.info(config)
#     logger.info('test')
#     tail = tail_file(config['farmer_logs'], 100)
#     print(tail)

    # for line in follow_log(config['farmer_logs']):
    #     print(line, end='')

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
        start_index = max(0, len(lines) - tail)
        
        for line in lines[start_index:]:
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

    run(config)



