from src.utils.logger import logger
import src.utils.constants as constants
import re
import datetime

class Parser:
    def analyze_log(log):
        event = None
        log_time = log['time'].split('.')[0]

        if constants.KEY_EVENTS[0] in log['log']:
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
        elif constants.KEY_EVENTS[1] in log['log']:
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
        elif constants.KEY_EVENTS[2] in log['log']:
            event = {
                    'event_type': 'Plotting Paused',
                    'time': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
            }
        elif constants.KEY_EVENTS[3] in log['log']:
            event = {
                'event_type': 'Plotting Resumed',
                'time': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
            }
        elif constants.KEY_EVENTS[4] in log['log']:
            event = {
                'event_type': 'Finished Piece Cache Sync',
                'time': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S"),
            }
        else:
            event = {
                'event_type': 'Unknown',
                'time': datetime.datetime.strptime(log_time, "%Y-%m-%dT%H:%M:%S")
            }
            #logger.info(event)
        return event   
