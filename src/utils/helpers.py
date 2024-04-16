from src.utils.logger import logger
import os
import yaml
import subprocess
import json
import datetime

class Helpers:
    @staticmethod
    def read_yaml_file(file_path):
        logger.info(f'Opening config from {file_path}')
        if not os.path.exists(file_path):
            return None
        
        with open(file_path, 'r') as file:
            try:
                data = yaml.safe_load(file)
                return data
            except yaml.YAMLError as e:
                print(f"Error reading YAML file: {e}")
                return None
    
    @staticmethod
    def get_full_container_id(container_name):
        try:
            logger.info(f'Getting Container ID for {container_name}')

            # List all containers (including non-running ones) that match the container name
            cmd_list = ['docker', 'ps', '-a', '--filter', f'name={container_name}', '--format', '{{.ID}}']
            container_ids = subprocess.check_output(cmd_list).decode('utf-8').strip().split('\n')
            
            # Assuming the first container ID is the one we're interested in
            if container_ids:
                container_id = container_ids[0]
                # Use docker inspect to get the full container ID
                cmd_inspect = ['docker', 'inspect', container_id, '--format', '{{.Id}}']
                full_container_id = subprocess.check_output(cmd_inspect).decode('utf-8').strip()
                return full_container_id
            else:
                logger.error(f'{container_name} not found. Did you enter it in the config correctly?')
                return False
        except subprocess.CalledProcessError as e:
            logger.error(f"Error executing Docker command. You may need to add the current user to the Docker group : {e}")
            return False
        
    @staticmethod
    def get_container_image_version(container_name):
        try:
            logger.info(f'Getting image version for {container_name}')
            cmd_list = ['docker', 'ps', '-a', '--filter', f'name={container_name}', '--format', '{{.ID}}\t{{.Image}}']
            result = subprocess.run(cmd_list, capture_output=True, text=True)
            if result.returncode == 0:
                repo_tags = result.stdout.strip("[]\n").split(":")
                if len(repo_tags) > 1:
                    return repo_tags[1]
            
            logger.error('Unable to get the image version.')
            return None
        
        except subprocess.CalledProcessError as e:
            logger.error(f"Error executing Docker command. You may need to add the current user to the Docker group : {e}")
            return False

    
    @staticmethod
    def get_docker_logs(log_file_path):
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
        
    @staticmethod
    def check_root():
        if os.geteuid() == 0:
            return True

        return False
    
    @staticmethod
    def get_current_datetime():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')