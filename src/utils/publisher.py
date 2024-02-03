from prometheus_client import Gauge
from src.utils.logger import logger

class Publisher:
    def __init__(self) -> None:
        self.sector_plotted = Gauge('sector_plotted', 'Sector Plotted', labelnames=['disk_farm_index'])
        self.plotting_progress = Gauge('plotting_progress', 'Plotting Progress', labelnames=['sector_index','disk_farm_index'])
        self.piece_cache = Gauge('piece_cache', 'Piece Cache Progress')

    def publish_farmer(self, data):
        logger.info(f'Publishing Farmer Data: {data}')
        self.piece_cache.set(data['piece_cache_status'])

        for farm in data['plotting_status']:
            disk_farm_index, percentage_complete, sector_index = farm['disk_farm_index'], farm['percentage_complete'], farm['sector_index']
            self.sector_plotted.labels(disk_farm_index=disk_farm_index).set(sector_index)
            self.plotting_progress.labels(sector_index=sector_index, disk_farm_index=disk_farm_index).set(percentage_complete)