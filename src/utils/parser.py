from src.utils.logger import logger
import src.utils.constants as constants
import re
import datetime
from typing import Dict
import sys
import json

class Parser:
    def get_log_event(log, farmer_name) -> Dict:
        event = None
        log_time = log['time'].split('.')[0] + '.' + log['time'].split('.')[1][0:4]

        if constants.KEY_EVENTS[0] in log['log']:
            pattern = r'farm_index=(\d+).*?(\d+\.\d+)% complete.*?sector_index=(\d+)'        
            match = re.search(pattern, log['log'])
            if match:
                event = {
                    'Event Type': 'Plotting Sector',
                    'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
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
                    'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Percentage Complete': float(match.group(1)),
                        'Farmer Name': farmer_name
                    }
                }
        elif constants.KEY_EVENTS[2] in log['log']:
            event = {
                    'Event Type': 'Plotting Paused',
                    'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farmer Name': farmer_name
                    }
            }
        elif constants.KEY_EVENTS[3] in log['log']:
            event = {
                'Event Type': 'Plotting Resumed',
                'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                'Data': {
                    'Farmer Name': farmer_name
                }
            }
        elif constants.KEY_EVENTS[4] in log['log']:
            event = {
                'Event Type': 'Finished Piece Cache Sync',
                'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                'Data': {
                    'Farmer Name': farmer_name
                }
            }
        elif constants.KEY_EVENTS[5] in log['log']:
            pattern = r'farm_index=(\d+).*hash\s(0x[0-9a-fA-F]+)'
            match = re.search(pattern, log['log'])

            event = {
                'Event Type': 'Reward',
                'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                'Data': {
                    'Farm Index': match.group(1),
                    'Hash': match.group(2)
                }
            }
        elif constants.KEY_EVENTS[6] in log['log']:
            pattern = r"Single disk farm (\d+):"

            # Use re.search() to find the match
            match = re.search(pattern, log['log'])

            event = {
                'Event Type': 'New Farm Identified',
                'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                'Data': {
                    'Farm Index': int(match.group(1))
                }
            }
        elif constants.KEY_EVENTS[7] in log['log']:
            event = {
                'Event Type': 'Synchronizing Piece Cache',
                'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                'Data': {
                    'Farmer Name': farmer_name
                }
            }
        elif constants.KEY_EVENTS[8] in log['log']:
            pattern = r'farm_index=(\d+).*?(\d+\.\d+)% complete.*?sector_index=(\d+)'        
            match = re.search(pattern, log['log'])
            if match:
                event = {
                    'Event Type': 'Replotting Sector',
                    'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Percentage Complete': float(match.group(2)),
                        'Current Sector': int(match.group(3))
                    }
                }
        elif constants.KEY_EVENTS[9] in log['log']:
            pattern = r"farm_index=(\d+)"
            match = re.search(pattern, log['log'])
            if match:
                event = {
                    'Event Type': 'Replotting Complete',
                    'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
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
                    'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Hash': None
                    }
                }
        elif constants.KEY_EVENTS[11] in log['log']:
            pattern = r'starting (\d+) workers'
            match = re.search(pattern, log['log'])
            if match:
                event = {
                    'Event Type': 'Starting Workers',
                    'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Number of Workers': int(match.group(1)),
                        'Farmer Name': farmer_name
                    }
                }
        elif constants.KEY_EVENTS[12] in log['log']:
            pattern = r'farm_index=(\d+).*ID:\s+([A-Z0-9]+)'
            match = re.search(pattern, log['log'])
            if match:
                event = {
                    'Event Type': 'Farm ID',
                    'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Farm ID': match.group(2),
                    }
                }
        elif constants.KEY_EVENTS[13] in log['log']:
            pattern = r'farm_index=(\d+).*Public key:\s+(0x[a-fA-F0-9]+)'
            match = re.search(pattern, log['log'])
            if match:
                event = {
                    'Event Type': 'Farm Public Key',
                    'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Farm Public Key': match.group(2),
                    }
                }
        elif constants.KEY_EVENTS[14] in log['log']:
            pattern = r'farm_index=(\d+).*Allocated space:\s+([\d.]+)\s+(GiB|TiB|GB|TB)\s+\(([\d.]+)\s+(GiB|TiB|GB|TB)\)'
            match = re.search(pattern, log['log'])

            if match:
                if match.group(3) == 'TiB':
                    allocated_gib = float(match.group(2)) * 1024
                else:
                    allocated_gib = float(match.group(2))

                event = {
                    'Event Type': 'Farm Allocated Space',
                    'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Allocated Space Primary': allocated_gib                    
                    }
                }
        elif constants.KEY_EVENTS[15] in log['log']:
            pattern = r'farm_index=(\d+).*Directory:\s+(.+)'
            match = re.search(pattern, log['log'])
            if match:
                event = {
                    'Event Type': 'Farm Directory',
                    'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                    'Data': {
                        'Farm Index': int(match.group(1)),
                        'Directory': match.group(2)
                    }
                }
        else:
            event = {
                'Event Type': 'Unknown',
                'Datetime': datetime.datetime.strptime(log_time, '%Y-%m-%dT%H:%M:%S.%f'),
                'data': {
                    'log': log['log']
                }
            }

            
        return event
    
    def parse_prometheus_metrics(metrics) -> Dict:
        parsed_metrics = {
            "Farmer": {
                "Established Connections": None,
                "Downloading Sectors": None,
                "Downloaded Sectors": None,
                "Encoding Sectors": None,
                "Encoded Sectors": None,
                "Writing Sectors": None,
                "Written Sectors": None,
                "Plotting Sectors": None,
                "Plotted Sectors": None
            },
            "Farms": {}
        }

        for metric in metrics:
            # First we make sure we are tracking the farm_id
            # If it doesn't exist, add it
            if 'subspace_farmer' in metric.name:
                farm_id = metric.labels.get("farm_id", None)
                if farm_id:
                    if farm_id not in parsed_metrics['Farms']:
                        parsed_metrics["Farms"][farm_id] = {
                            "Plotted": 0,
                            "Not Plotted": 0,
                            "Expired": 0,
                            "About to Expire": 0,
                            "Plotting Time Seconds Count": None,
                            "Writing Time Seconds Count": None,
                            "Encoding Time Seconds Count": None,
                            "Downloading Time Seconds Count": None,
                            "Proving Time Seconds Count": None,
                            "Auditing Time Seconds Count": None,
                            "Plotting Time Seconds Sum": None,
                            "Writing Time Seconds Sum": None,
                            "Encoding Time Seconds Sum": None,
                            "Downloading Time Seconds Sum": None,
                            "Proving Time Seconds Sum": None,
                            "Auditing Time Seconds Sum": None,
                        }
            

            if metric.name == 'subspace_farmer_sectors_total_sectors':
                if metric.labels["state"] == 'Plotted':
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Plotted"] = metric.value
                elif metric.labels["state"] == 'NotPlotted':
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Not Plotted"] = metric.value
                elif metric.labels["state"] == 'Expired':
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Expired"] = metric.value
                elif metric.labels["state"] == 'AboutToExpire':
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["About to Expire"] = metric.value


            # Downloading Time
            elif metric.name == 'subspace_farmer_sector_downloading_time_seconds_count':
                parsed_metrics["Farms"][metric.labels["farm_id"]]["Downloading Time Seconds Count"] = metric.value

            elif metric.name == 'subspace_farmer_sector_downloading_time_seconds_sum':
                if metric.value is not None:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Downloading Time Seconds Sum"] = round(metric.value, 2)
                else:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Downloading Time Seconds Sum"] = metric.value = None

            # Encoding Time
            elif metric.name == 'subspace_farmer_sector_encoding_time_seconds_count':
                parsed_metrics["Farms"][metric.labels["farm_id"]]["Encoding Time Seconds Count"] = metric.value

            elif metric.name == 'subspace_farmer_sector_encoding_time_seconds_sum':
                if metric.value is not None:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Encoding Time Seconds Sum"] = round(metric.value, 2)
                else:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Encoding Time Seconds Sum"] = None

            # Writing Time
            elif metric.name == 'subspace_farmer_sector_writing_time_seconds_count':
                parsed_metrics["Farms"][metric.labels["farm_id"]]["Writing Time Seconds Count"] = metric.value

            elif metric.name == 'subspace_farmer_sector_writing_time_seconds_sum':
                if metric.value is not None:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Writing Time Seconds Sum"] = round(metric.value, 2)
                else:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Writing Time Seconds Sum"] = None

            # Plotting Time
            elif metric.name == 'subspace_farmer_sector_plotting_time_seconds_count':
                parsed_metrics["Farms"][metric.labels["farm_id"]]["Plotting Time Seconds Count"] = metric.value

            elif metric.name == 'subspace_farmer_sector_plotting_time_seconds_sum':
                if metric.value is not None:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Plotting Time Seconds Sum"] = round(metric.value, 2)
                else:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Plotting Time Seconds Sum"] = None

            # Proving Time
            elif metric.name == 'subspace_farmer_proving_time_seconds_count':
                parsed_metrics["Farms"][metric.labels["farm_id"]]["Proving Time Seconds Count"] = metric.value

            elif metric.name == 'subspace_farmer_proving_time_seconds_sum':
                if metric.value is not None:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Proving Time Seconds Sum"] = round(metric.value, 2)
                else:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Proving Time Seconds Sum"] = None

            # Auditing Time
            elif metric.name == 'subspace_farmer_auditing_time_seconds_count':
                parsed_metrics["Farms"][metric.labels["farm_id"]]["Auditing Time Seconds Count"] = metric.value

            elif metric.name == 'subspace_farmer_auditing_time_seconds_sum':
                if metric.value is not None:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Auditing Time Seconds Sum"] = round(metric.value, 2)
                else:
                    parsed_metrics["Farms"][metric.labels["farm_id"]]["Auditing Time Seconds Sum"] = None

            # =============== FARMER METRICS
            # Download
            elif metric.name == 'subspace_farmer_sector_downloading_counter_sectors_total':
                parsed_metrics["Farmer"]["Downloading Sectors"] = metric.value

            elif metric.name == 'subspace_farmer_sector_downloaded_counter_sectors_total':
                parsed_metrics["Farmer"]["Downloaded Sectors"] = metric.value

            # Encoding
            elif metric.name == 'subspace_farmer_sector_encoding_counter_sectors_total':
                parsed_metrics["Farmer"]["Encoding Sectors"] = metric.value

            elif metric.name == 'subspace_farmer_sector_encoded_counter_sectors_total':
                parsed_metrics["Farmer"]["Encoded Sectors"] = metric.value

            # Writing
            elif metric.name == 'subspace_farmer_sector_writing_counter_sectors_total':
                parsed_metrics["Farmer"]["Writing Sectors"] = metric.value

            elif metric.name == 'subspace_farmer_sector_written_counter_sectors_total':
                parsed_metrics["Farmer"]["Written Sectors"] = metric.value

            # Plotting
            elif metric.name == 'subspace_farmer_sector_plotting_counter_sectors_total':
                parsed_metrics["Farmer"]["Plotting Sectors"] = metric.value

            elif metric.name == 'subspace_farmer_sector_plotted_counter_sectors_total':
                parsed_metrics["Farmer"]["Plotted Sectors"] = metric.value

            # Established Connections
            elif metric.name == 'subspace_established_connections':
                parsed_metrics["Farmer"]["Established Connections"] = metric.value

        return parsed_metrics