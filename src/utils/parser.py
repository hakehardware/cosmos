from src.utils.logger import logger
import src.utils.constants as constants
import re
import datetime

class Parser:
    def get_log_event(log):
        event = None
        log_time = log['time'].split('.')[0]

        if constants.KEY_EVENTS[0] in log['log']:
            pattern = r'farm_index=(\d+).*?(\d+\.\d+)% complete.*?sector_index=(\d+)'        
            match = re.search(pattern, log['log'])
            if match:
                event = {
                    'Event Type': 'Plotting Sector',
                    'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Percentage Complete': match.group(2),
                        'Current Sector': match.group(3)
                    }
                }
        elif constants.KEY_EVENTS[1] in log['log']:
            pattern = r'Piece cache sync (\d+\.\d+)% complete'
            match = re.search(pattern, log['log'])

            if match:
                event = {
                    'Event Type': 'Piece Cache Sync',
                    'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
                    'Data': {
                        'Percentage Complete': float(match.group(1)),
                    }
                }
        elif constants.KEY_EVENTS[2] in log['log']:
            event = {
                    'Event Type': 'Plotting Paused',
                    'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
            }
        elif constants.KEY_EVENTS[3] in log['log']:
            event = {
                'Event Type': 'Plotting Resumed',
                'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
            }
        elif constants.KEY_EVENTS[4] in log['log']:
            event = {
                'Event Type': 'Finished Piece Cache Sync',
                'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
            }
        elif constants.KEY_EVENTS[5] in log['log']:
            pattern = r'{farm_index=(\d+)}'
            match = re.search(pattern, log['log'])

            event = {
                'Event Type': 'Reward',
                'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
                'Data': {
                    'Farm Index': match.group(1)
                }
            }
        elif constants.KEY_EVENTS[6] in log['log']:
            pattern = r"Single disk farm (\d+):"

            # Use re.search() to find the match
            match = re.search(pattern, log['log'])

            event = {
                'Event Type': 'New Farm Identified',
                'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
                'Data': {
                    'Farm Index': int(match.group(1))
                }
            }
        elif constants.KEY_EVENTS[7] in log['log']:
            event = {
                'Event Type': 'Synchronizing Piece Cache',
                'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
            }
        elif constants.KEY_EVENTS[8] in log['log']:
            pattern = r'farm_index=(\d+).*?(\d+\.\d+)% complete.*?sector_index=(\d+)'        
            match = re.search(pattern, log['log'])
            if match:
                event = {
                    'Event Type': 'Replotting Sector',
                    'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Percentage Complete': match.group(2),
                        'Current Sector': match.group(3)
                    }
                }
        elif constants.KEY_EVENTS[9] in log['log']:
            pattern = r"farm_index=(\d+)"
            match = re.search(pattern, log['log'])
            if match:
                event = {
                    'Event Type': 'Replotting Complete',
                    'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                    }
                }
        elif constants.KEY_EVENTS[10] in log['log']:
            pattern = r"farm_index=(\d+)"
            match = re.search(pattern, log['log'])
            if match:
                event = {
                    'Event Type': 'Failed to Send Solution',
                    'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                    }
                }
            logger.info(event)
        else:
            event = {
                'Event Type': 'Unknown',
                'Datetime': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
                'data': {
                    'log': log['log']
                }
            }

            
        return event
    
    def get_farm_id(log):
        pattern = r"ID:\s+(\S+)"

        # Use re.search() to find the match
        match = re.search(pattern, log['log'])

        return match.group(1)
    
    def get_allocated_space(log):
        pattern = r"Allocated space: ([\d\.]+) (TiB|GiB|TB|GB)"
        # Use re.search() to find the match
        match = re.search(pattern, log['log'])

        return match.group(1) + " " + match.group(2)
    

    def parse_prometheus_metrics(metrics):
        parsed_metrics = {}
        for metric in metrics:
            pass