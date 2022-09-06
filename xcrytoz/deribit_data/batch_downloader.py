import os
from typing import Callable, List, Dict, Tuple, Any
from datetime import datetime, timedelta
from abc import abstractclassmethod, ABC
from collections import namedtuple
import itertools
import time
import numpy as np
import pandas as pd

from ..common_utils import get_logger, Converter
from .deribit_fields import DeribitFields
from .downloader import DeribitDownloader_Simple
from .file_zip_io import FileZipWriter

_LOGGER = get_logger(__name__)


# this is to make the variable name shorter
_cst = DeribitFields()


class BatchDownloaderZip(ABC):

    dt_format = '%Y%m%d%H%M%S'

    @staticmethod
    def get_month_str(dt: datetime):
        return dt.strftime('%Y%m')

    def __init__(self, root_folder: str, save_subfolder: str, file_header: str, currencies: List[str], kinds: List[str]):
        self.root_folder = root_folder
        if not os.path.exists(root_folder):
            # making a save folder
            os.makedirs(root_folder)

        self.downloader = DeribitDownloader_Simple()
        self.writer: FileZipWriter = FileZipWriter(root_folder, save_subfolder, file_header) # to be defined

        self.execute_download: Callable[[str,str], Any] # to be defined

        self.currencies = currencies #['BTC', 'ETH', 'SOL']
        self.kinds = kinds  # ['future', 'option']

    def download(self):

        for currency, kind in itertools.product(self.currencies, self.kinds):
            start_timestamp = int(time.time())

            _LOGGER.info('downloading ' + currency + ' ' + kind)
            data = self.execute_download(currency, kind)
            end_timestamp = int(time.time())
            attribs = {
                'batch_id': self.writer.file_header,
                'save_folder': self.writer.save_folder,
                'time_start': start_timestamp,
                'time_end': end_timestamp
            }

            file_path = self.writer.write(data, attribs, currency, kind)        

            _LOGGER.info('wrote to json ' + file_path)

        _LOGGER.info('done')

class BatchDownloaderZip_TickerInfo(BatchDownloaderZip):

    def __init__(self, root_folder: str, timestamp: int, 
            currencies = ['BTC', 'ETH', 'SOL'], kinds = ['future', 'option']):
        
        self.currencies = currencies
        self.kinds = kinds

        dt = Converter.ms2dt(timestamp)
        save_subfolder = self.get_month_str(dt)
        file_header = dt.strftime(self.dt_format)

        super().__init__(root_folder, save_subfolder, file_header, currencies, kinds)

        self.execute_download = lambda c, k: self.downloader.download_tickers(c, k)

class BatchDownloaderZip_LastTradeInfo(BatchDownloaderZip):

    def __init__(self, root_folder: str, start_timestamp: int, end_timestamp: int, 
                currencies = ['BTC', 'ETH', 'SOL'], kinds = ['option'], ts_split: int = 20):

        dt_s, dt_e = Converter.ms2dt(start_timestamp), Converter.ms2dt(end_timestamp)
        save_subfolder = self.get_month_str(dt_e)
        file_header = dt_s.strftime(self.dt_format) + '-' + dt_e.strftime(self.dt_format)

        super().__init__(root_folder, save_subfolder, file_header, currencies, kinds)

        self.execute_download = lambda c, k: self.downloader.download_last_trades(c, k, start_timestamp, end_timestamp, ts_split=ts_split)

