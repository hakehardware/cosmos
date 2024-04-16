import asyncio
import discord
from discord import Webhook
import aiohttp
import sqlite3
from src.utils.logger import logger
from src.utils.helpers import Helpers
import json
import datetime
import sys

class PromAPI:
    """
    Returns data from the metrics endpoints
    """
    pass

class LogsAPI:
    """
    Returns data contained in the logs so users can monitor events
    """
    pass

class SystemAPI:
    """
    Returns data about the system so users can monitor usage
    """
    pass

class DiscordAPI:

    @staticmethod
    async def send_message(url, message):
        async with aiohttp.ClientSession() as session:
            webhook = Webhook.from_url(url, session=session)
            embed = discord.Embed(
                title="New Reward!",
                description=message,
                color=discord.Color.green()
            )
            await webhook.send(embed=embed)

    def send_discord_message(url, message):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(
            DiscordAPI.send_message(url, message)
        )
        loop.close()

class DatabaseAPI:
    def __init__(self, db_name='cosmos.db'):
        self.db_name = db_name
        self.conn = None

    def connect(self):
        # logger.info('Connecting to DB')
        self.conn = sqlite3.connect(self.db_name)

    def disconnect(self):
        # logger.info('Disconnecting from DB')
        if self.conn:
            self.conn.close()

    def create_tables(self):
        
        self.connect()
        cursor = self.conn.cursor()

        logger.info('Initializing "events" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS events (
                            event_datetime TEXT,
                            event_type TEXT,
                            event_data TEXT
                        )''')
        
        logger.info('Initializing "farmer" Table')
        # piece_cache_status can be: SYNCRONIZING or COMPLETE
        cursor.execute('''CREATE TABLE IF NOT EXISTS farmer (
                    farmer_name TEXT,
                    piece_cache_status TEXT,
                    piece_cache_percent REAL,
                    workers INTEGER,
                    creation_datetime TEXT
                )''')
        
        logger.info('Initializing "farms" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS farms (
                            farm_id TEXT,
                            farm_index INTEGER,
                            public_key TEXT,
                            allocated_space_gib REAL,
                            directory TEXT,
                            status TEXT,
                            creation_datetime TEXT
                        )''')
        
        logger.info('Initializing "rewards" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS rewards (
                            farm_index TEXT,
                            reward_hash TEXT,
                            reward_result TEXT,
                            reward_datetime TEXT
                        )''')
        
        logger.info('Initializing "plots" Table')
        # is_replot can be 1 (yes) or 0 (no)
        cursor.execute('''CREATE TABLE IF NOT EXISTS plots (
                            farm_index TEXT,
                            percentage REAL,
                            current_sector INTEGER,
                            is_replot INTEGER,
                            plot_datetime TEXT
                        )''')
        
        logger.info('Initializing "errors" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS errors (
                            error_text TEXT,
                            error_datetime TEXT
                        )''')
        
        logger.info('Initializing "farm_metrics" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS farm_metrics (
                            farm_id TEXT,
                            plotted INTEGER,
                            not_plotted INTEGER,
                            expired INTEGER,
                            about_to_expire INTEGER,
                            plotting_time_seconds_cnt INTEGER,
                            writing_time_seconds_cnt INTEGER,
                            encoding_time_seconds_cnt INTEGER,
                            downloading_time_seconds_cnt INTEGER,
                            proving_time_seconds_cnt INTEGER,
                            auditing_time_seconds_count INTEGER,
                            plotting_time_seconds_sum REAL,
                            writing_time_seconds_sum REAL,
                            encoding_time_seconds_sum REAL,
                            downloading_time_seconds_sum REAL,
                            proving_time_seconds_sum REAL,
                            auditing_time_seconds_sum REAL,
                            metric_datetime TEXT
                        )''')
        
        logger.info('Initializing "farmer_metrics" Table')
        cursor.execute('''CREATE TABLE IF NOT EXISTS farmer_metrics (
                            farmer_name TEXT,
                            established_connections INTEGER,
                            downloading_sectors INTEGER,
                            downloaded_sectors INTEGER,
                            encoding_sectors INTEGER,
                            encoded_sectors INTEGER,
                            writing_sectors INTEGER,
                            written_sectors INTEGER,
                            plotting_sectors INTEGER,
                            plotted_sectors INTEGER,
                            metric_datetime TEXT
                        )''')
        self.conn.commit()
        self.disconnect()

    def insert_event(self, event):
        try:
            self.connect()

            event_datetime = event["Datetime"]
            event_type = event["Event Type"]
            event_data = event["Data"]

            cursor = self.conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM events WHERE event_datetime = ? AND event_type = ?', (event_datetime, event_type))
            exists = cursor.fetchone()[0] > 0

            if not exists:
                logger.info(f'Found new event: {event_type}')
                cursor.execute('INSERT INTO Events (event_datetime, event_type, event_data) VALUES (?, ?, ?)', (event_datetime, event_type, json.dumps(event_data)))
                self.conn.commit()

                return True
            else:
                return False

        finally:
            self.disconnect()

    def insert_farmer_details(self, farmer_name) -> bool:
        logger.info(f'Checking to see if {farmer_name} exists in farmer table.')

        try:
            self.connect()
            cursor = self.conn.cursor()

            self.conn.execute("BEGIN TRANSACTION")

            # Check if a row with the given farmer_name already exists
            cursor.execute("SELECT COUNT(*) FROM farmer WHERE farmer_name = ?", (farmer_name,))
            row_count = cursor.fetchone()[0]

            if row_count == 0:
                logger.info(f'{farmer_name} does not exist. Removing any previous farmers and adding {farmer_name}.')

                # If no row with the given farmer_name exists, delete all rows from the table
                cursor.execute("DELETE FROM farmer")

                # Add a new row with the farmer_name
                cursor.execute("INSERT INTO farmer (farmer_name) VALUES (?)", (farmer_name,))

            else:
                logger.info(f'{farmer_name} exists, no changes needed.')

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False

        finally:
            self.disconnect()

        return True

    def insert_farm(self, farm_id) -> bool:
        
        try:
            logger.info(f'Checking to see if {farm_id} exists')

            self.connect()
            cursor = self.conn.cursor()

            self.conn.execute("BEGIN TRANSACTION")

            # Check if a row with the given farm_id already exists
            cursor.execute("SELECT COUNT(*) FROM farms WHERE farm_id = ?", (farm_id,))
            row_count = cursor.fetchone()[0]

            if row_count == 0:
                logger.info(f'{farm_id} does not exist. Adding.')

                # Add a new row with the farm_id
                current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                cursor.execute("INSERT INTO farms (farm_id, creation_datetime) VALUES (?, ?)", (farm_id, current_datetime))

            else:
                logger.info(f'{farm_id} exists, no changes needed.')

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False

        finally:
            self.disconnect()

        return True

    def clean_farms(self, farm_ids) -> bool:
        try:
            self.connect()
            cursor = self.conn.cursor()
            
            logger.info(f'Cleaning farms table by removing any farm_ids not reported by metrics.')

            cursor.execute("SELECT COUNT(*) FROM farms WHERE farm_id NOT IN ({})".format(
                ','.join(['?'] * len(farm_ids))
            ), farm_ids)

            # Fetch the count of rows to be deleted
            rows_to_delete_count = cursor.fetchone()[0]
            logger.info(f'Deleting {rows_to_delete_count} old farms.')

            cursor.execute("DELETE FROM farms WHERE farm_id NOT IN ({})".format(
                ','.join(['?'] * len(farm_ids))
            ), farm_ids)

            self.conn.commit()


        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False

        finally:
            self.disconnect()

        return True

    def update_farm_id(self, event) -> bool:
        try:
            self.connect()
            cursor = self.conn.cursor()

            farm_index = event['Data']['Farm Index']
            farm_id = event['Data']['Farm ID']

            logger.info(f'Found index {farm_index} for Farm ID: {farm_id}')
            cursor.execute("UPDATE farms SET farm_index = ? WHERE farm_id = ?", (farm_index, farm_id))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True

    def update_farm_pub_key(self, event):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farm_index = event['Data']['Farm Index']
            farm_public_key = event['Data']['Farm Public Key']

            logger.info(f'Farm Index {farm_index} Public Key: {farm_public_key}')
            cursor.execute("UPDATE farms SET public_key = ? WHERE farm_index = ?", (farm_public_key, farm_index))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True

    def update_farm_alloc_space(self, event):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farm_index = event['Data']['Farm Index']
            allocated_primary = event['Data']['Allocated Space Primary']

            logger.info(f'Farm Index {farm_index} Allocated Space: {allocated_primary}')
            cursor.execute("UPDATE farms SET allocated_space_gib = ? WHERE farm_index = ?", (allocated_primary, farm_index))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True

    def update_farm_directory(self, event):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farm_index = event['Data']['Farm Index']
            directory = event['Data']['Directory']

            logger.info(f'Farm Index {farm_index} Directory: {directory}')
            cursor.execute("UPDATE farms SET directory = ? WHERE farm_index = ?", (directory, farm_index))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True

    def update_farm_workers(self, event):
        try:
            self.connect()
            cursor = self.conn.cursor()

            num_of_workers = event['Data']['Number of Workers']
            farmer_name = event['Data']['Farmer Name']
            logger.info(f'Number of Workers: {num_of_workers}')

            cursor.execute("UPDATE farmer SET workers = ? WHERE farmer_name = ?", (num_of_workers, farmer_name))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True

    def update_rewards(self, event):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farm_index = event['Data']['Farm Index']
            hash = event['Data']['Hash']
            result = event['Event Type']
            reward_datetime = event['Datetime']

            logger.info(f'Farm Index {farm_index}: {result}')
            cursor.execute("""
                INSERT INTO rewards (farm_index, reward_hash, reward_result, reward_datetime)
                VALUES (?, ?, ?, ?)
            """, (farm_index, hash, result, reward_datetime))
            
            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True

    def update_farm_status(self, event):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farm_index = event['Data']['Farm Index']
            status = event['Event Type']

            logger.info(f'Farm Index {farm_index}: {event["Event Type"]}')
            cursor.execute("UPDATE farms SET status = ? WHERE farm_index = ?", (status, farm_index))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True

    def update_plotting(self, event, is_replot):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farm_index = event['Data']['Farm Index']
            percentage_complete = event['Data']['Percentage Complete']
            current_sector = event['Data']['Current Sector']
            plot_datetime = event['Datetime']

            logger.info(f'Farm Index {farm_index}:  {event["Event Type"]} {current_sector} @ {percentage_complete}% Complete')
            cursor.execute("""
                INSERT INTO plots (farm_index, percentage, current_sector, is_replot, plot_datetime)
                VALUES (?, ?, ?, ?, ?)
                """, (
                    farm_index,
                    percentage_complete, 
                    current_sector,
                    is_replot,
                    plot_datetime                
                ))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True

    def update_farmer_status(self, event):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farmer_name = event['Data']['Farmer Name']
            status = event['Event Type']

            logger.info(f'{farmer_name}: {status}')
            cursor.execute("UPDATE farmer SET status = ? WHERE farmer_name = ?", (status, farmer_name))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        finally:
            self.disconnect()

        return True
    
    def update_piece_cache_sync(self, event):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farmer_name = event['Data']['Farmer Name']
            percentage = event['Data']['Percentage Complete']
            piece_cache_status = 'Syncronizing Piece Cache'

            logger.info(f'{farmer_name}: Piece Cache @ {percentage}')
            cursor.execute("UPDATE farmer SET piece_cache_percent = ?, piece_cache_status = ? WHERE farmer_name = ?", (percentage, piece_cache_status, farmer_name))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        finally:
            self.disconnect()

        return True
    
    def update_piece_cache_status(self, event):
        try:
            self.connect()
            cursor = self.conn.cursor()

            farmer_name = event['Data']['Farmer Name']
            piece_cache_status = event['Event Type']
            logger.info(f'{farmer_name}: {piece_cache_status}')

            if piece_cache_status == 'Finished Piece Cache Sync':

                piece_cache_percent = 100.00
                cursor.execute("UPDATE farmer SET piece_cache_percent = ?, piece_cache_status = ? WHERE farmer_name = ?", (piece_cache_percent, piece_cache_status, farmer_name))
            else:
                cursor.execute("UPDATE farmer SET piece_cache_status = ? WHERE farmer_name = ?", (piece_cache_status, farmer_name))

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            sys.exit(1)
            self.conn.rollback()
            return False
        
        finally:
            self.disconnect()

        return True

    def update_farm_metrics(self, farm_id, farm_metrics):
        try:
            self.connect()
            cursor = self.conn.cursor()
            
            current_datetime = Helpers.get_current_datetime()

            cursor.execute('''
                INSERT INTO farms (
                    farm_id TEXT,
                    plotted INTEGER,
                    not_plotted INTEGER,
                    about_to_expire INTEGER,
                    plotting_time_seconds_cnt INTEGER,
                    writing_time_seconds_cnt INTEGER,
                    encoding_time_seconds_cnt INTEGER,
                    downloading_time_seconds_cnt INTEGER,
                    proving_time_seconds_cnt INTEGER,
                    auditing_time_seconds_cnt INTEGER,
                    plotting_time_seconds_sum REAL,
                    writing_time_seconds_sum REAL,
                    encoding_time_seconds_sum REAL,
                    downloading_time_seconds_sum REAL,
                    proving_time_seconds_sum REAL,
                    auditing_time_seconds_sum REAL,
                    metrics_datetime TEXT
                ) VALUES (?, ?)''', (
                    farm_id,
                    farm_metrics['Plotted'],
                    farm_metrics['Not Plotted'],
                    farm_metrics['Expired'],
                    farm_metrics['About to Expire'],
                    farm_metrics['Plotting Time Seconds Count'],
                    farm_metrics['Writing Time Seconds Count'],
                    farm_metrics['Encoding Time Seconds Count'],
                    farm_metrics['Downloading Time Seconds Count'],
                    farm_metrics['Proving Time Seconds Count'],
                    farm_metrics['Auditing Time Seconds Count'],
                    farm_metrics['Plotting Time Seconds Sum'],
                    farm_metrics['Writing Time Seconds Sum'],
                    farm_metrics['Encoding Time Seconds Sum'],
                    farm_metrics['Downloading Time Seconds Sum'],
                    farm_metrics['Proving Time Seconds Sum'],
                    farm_metrics['Auditing Time Seconds Sum'],
                    current_datetime
                    )
                )

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True

    def update_farmer_metrics(self, farm_id, farmer_metrics):
        pass

    def db_update_template(self) -> bool:
        try:
            self.connect()
            cursor = self.conn.cursor()

            # INSERT HERE

            self.conn.commit()

        except Exception as e:
            # Rollback the transaction if an error occurs
            logger.error(f'Error: {e}')
            self.conn.rollback()
            return False
        
        
        finally:
            self.disconnect()

        return True

    # def get_and_update_farm_indexes(self, farm_ids):
    #     try:
    #         logger.info('Iterating through farms to get the farm_indexes')
    #         self.connect()
    #         cursor = self.conn.cursor()

    #         farm_indexes = {}
    #         for farm_id in farm_ids:
    #             # Execute the SELECT statement to retrieve event_data containing the specified farm_id
    #             cursor.execute("""
    #                 SELECT event_data 
    #                 FROM events 
    #                 WHERE event_data LIKE ? 
    #                 AND event_datetime = (
    #                     SELECT MAX(event_datetime) 
    #                     FROM events 
    #                     WHERE event_data LIKE ?
    #                 )""", 
    #                 ('%' + farm_id + '%', '%' + farm_id + '%'))
    #             row = cursor.fetchone()

    #             # Parse the event_data as JSON and extract the "Farm Index" value
    #             if row:
    #                 event_data_json = json.loads(row[0])
    #                 farm_index = event_data_json.get("Farm Index")
    #                 logger.info(f'{farm_id} has an index of {event_data_json.get("Farm Index")}')

    #                 farm_indexes[farm_id] = farm_index


    #         for farm_id, farm_index in farm_indexes.items():
    #             cursor.execute("UPDATE farms SET farm_index = ? WHERE farm_id = ?", (farm_index, farm_id))
    #             logger.info(f'Updating database for {farm_id} with an index of {farm_index}')

    #         self.conn.commit()

        
    #     except Exception as e:
    #         # Rollback the transaction if an error occurs
    #         logger.error(f'Error: {e}')
    #         self.conn.rollback()
    #         return False

    #     finally:
    #         self.disconnect()

    
    def initialize(self):
        self.create_tables()