import itertools
import json
import os
import time
import zipfile
from abc import abstractclassmethod
from datetime import datetime
from typing import List

import numpy as np

from ..common_utils import Converter, get_logger
from .downloader import DeribitDownloader_Simple
from .shared_structures import DeribitConstants, TickerBatchInfo

_LOGGER = get_logger(__name__)

# this is to make the variable name shorter
_dcs = DeribitConstants()


class BatchDownloader:

    def __init__(self, root_folder, save_folder_name, batch_id):

        self.root_folder = root_folder
        self.batch_id = batch_id
        self.save_folder = os.path.join(root_folder, save_folder_name)
        self.downloader = DeribitDownloader_Simple()

    def download_batches(self, currencies: List[str], kinds: List[str]):

        if not os.path.exists(self.save_folder):
            os.makedirs(self.save_folder)

        start_timestamp = int(time.time())
        for currency, kind in itertools.product(currencies, kinds):
            try:
                _LOGGER.info('downloading ' + currency + ' ' + kind)
                data = self._execute_download(currency, kind)
                end_timestamp = int(time.time())
                attribs = {
                    'batch_id': self.batch_id,
                    'save_folder': self.save_folder,
                    'time_start': start_timestamp,
                    'time_end': end_timestamp
                }

                file_path = self.__write_zip(data, attribs, currency, kind)
                _LOGGER.info('wrote to json ' + file_path)

            except Exception as ex:
                _LOGGER.error("FAILED: " + currency + '/' + kind + '. Error: ' + str(ex))

    def __write_zip(self, data: dict, attributes: dict, currency: str, kind: str) -> str:

        # create a data structure with data and attrbutes
        to_save = {_dcs.attributes_file_name: attributes, _dcs.data_file_name: data}

        zip_file_path = os.path.join(self.save_folder, '_'.join([self.batch_id, currency, kind]) + '.zip')

        with zipfile.ZipFile(zip_file_path, mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:

            for fn, content in to_save.items():
                dumped_json = json.dumps(content, ensure_ascii=False, indent=4)
                zip_file.writestr(fn, data=dumped_json)

            zip_file.testzip()

        return zip_file_path

    @abstractclassmethod
    def _execute_download(self, currency, kind):
        pass


class TickerBatchDownloader(BatchDownloader):

    def __init__(self, root_folder, timestamp):
        dt = Converter.ms2dt(timestamp)
        save_folder_name = dt.strftime(_dcs.YYYYMM)
        batch_id = dt.strftime(_dcs.YYYYMMDDhhmmss)
        super().__init__(root_folder, save_folder_name, batch_id)

    def _execute_download(self, currency, kind):
        return self.downloader.download_tickers(currency, kind)


class LastTradeBatchDownloader(BatchDownloader):

    def __init__(self, root_folder: str, start_timestamp: int, end_timestamp: int):

        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp

        dt_s, dt_e = Converter.ms2dt(start_timestamp), Converter.ms2dt(end_timestamp)
        save_folder_name = dt_e.strftime(_dcs.YYYYMM)
        batch_id = dt_s.strftime(_dcs.YYYYMMDDhhmmss) + '-' + dt_e.strftime(_dcs.YYYYMMDDhhmmss)

        super().__init__(root_folder, save_folder_name, batch_id)

    def _execute_download(self, currency, kind):
        return self.downloader.download_last_trades(currency, kind, self.start_timestamp, self.end_timestamp)


class BatchFileManager:

    def __init__(self, root_folder):
        self.root_folder = root_folder

    def read(self, file_path_without_root_folder: str):

        file_path = os.path.join(self.root_folder, file_path_without_root_folder)

        with zipfile.ZipFile(file_path, 'r') as zip_file:
            data = json.loads(zip_file.read(_dcs.data_file_name))
            attribs = json.loads(zip_file.read(_dcs.attributes_file_name))

        return {_dcs.data: data, _dcs.attributes: attribs}

    def get_ticker_batch_file_infos(self, from_timestamp: int = None, to_timestamp: int = None)\
            -> List[TickerBatchInfo]:

        if from_timestamp is None:
            from_timestamp = -np.inf
        if to_timestamp is None:
            to_timestamp = np.inf

        # find all subfolders with digits
        data_folder_names = sorted([path.name for path in os.scandir(self.root_folder)
                                    if (path.is_dir() and os.path.basename(path.path).isdigit())])

        # name, folder pairs
        file_infos = []
        for folder_name in data_folder_names:
            for file_path in os.scandir(os.path.join(self.root_folder, folder_name)):

                file_name_w_ext = file_path.name
                file_path_wo_root = os.path.join(folder_name, file_name_w_ext)

                # parse
                batch_dt_str, currency, kind = os.path.splitext(file_name_w_ext)[0].split('_')
                batch_ts = Converter.dt2ms_int(datetime.strptime(batch_dt_str, _dcs.YYYYMMDDhhmmss))

                if (batch_ts >= from_timestamp) and (batch_ts <= to_timestamp):
                    file_infos.append(TickerBatchInfo(batch_ts, currency, kind, file_path_wo_root))

        return file_infos
