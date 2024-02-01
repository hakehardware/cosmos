from prometheus_client import Gauge
from src.utils.logger import logger

class Publisher:
    def __init__(self) -> None:
        self.sector_plotted = Gauge('sector_plotted', 'Sector Plotted', labelnames=['disk_farm_index'])
        self.plotting_progress = Gauge('plotting_progress', 'Plotting Progress', labelnames=['sector_index','disk_farm_index'])
        self.piece_cache = Gauge('piece_cache', 'Piece Cache Progress')

    def publish_sector_data(self, data):
        logger.info('Publishing Sector Data')

        sector_index, disk_farm_index, progress = data['sector_index'], data['disk_farm_index'], data['percentage_complete']
        self.sector_plotted.labels(disk_farm_index=disk_farm_index).set(sector_index)
        self.plotting_progress.labels(sector_index=sector_index, disk_farm_index=disk_farm_index).set(progress)

    def publish_piece_cache_data(self, data):
        logger.info('Publishing Piece Cache Data')
        self.piece_cache.set(data['percentage_complete'])


        
