from src.utils.logger import logger
import os
import yaml
import subprocess
import json

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
            logger.info('Getting Container ID')
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
                return "Container not found."
        except subprocess.CalledProcessError as e:
            return f"Error executing Docker command: {e}"