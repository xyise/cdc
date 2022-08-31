from turtle import Turtle
from typing import List, Dict, Tuple
from datetime import date, datetime, timedelta
import os
from abc import abstractclassmethod, ABC
from collections import namedtuple
import itertools
import time
import json, zipfile
import sqlite3
from sqlite3 import Error as DBError
from contextlib import contextmanager
import numpy as np

import glob

from ..common_utils import get_logger, Converter
from .downloader import DeribitDownloader_Simple

_LOGGER = get_logger(__name__)


def batch_download_and_zip(ts_ms: int, root_folder: str):

    currencies = ['BTC', 'ETH', 'SOL']
    kinds = ['future', 'option']

    if not os.path.exists(root_folder):
        # making a save folder
        os.makedirs(root_folder)

    downloader = DeribitDownloader_Simple()
    writer = FileZipDaoWriter(root_folder, ts_ms)

    for currency, kind in itertools.product(currencies, kinds):
        start_timestamp = int(time.time())

        _LOGGER.info('downloading ' + currency + ' ' + kind)
        data = downloader.download_tickers(currency, kind)
        end_timestamp = int(time.time())
        attribs = {
            'batch_id': writer.batch_dt_id,
            'save_folder': writer.save_folder,
            'time_start': start_timestamp,
            'time_end': end_timestamp
        }

        file_path = writer.write(data, attribs, currency, kind)        

        _LOGGER.info('wrote to json ' + file_path)

    _LOGGER.info('done')

class FileDaoBase(ABC):

    batch_dt_id_format = '%Y%m%d%H%M%S'
    dt_sub_folder_format = '%Y%m'
    data_file_name = 'data.json'
    attrib_file_name = 'attributes.json'

    def __init__(self, root_folder):

        self.root_folder = root_folder

    def _get_save_folder_path(self, dt: datetime) -> str:

        return os.path.join(self.root_folder, dt.strftime(self.dt_sub_folder_format))

    def _get_zip_file_path(self, save_folder, batch_dt_id: str, currency: str, kind: str) -> str:

        return os.path.join(save_folder, '_'.join([batch_dt_id, currency, kind]) + '.zip')

    def _get_batch_dt_id(self, dt: datetime):

        return dt.strftime(self.batch_dt_id_format)

    def _get_timestamp_from_file_path(self, file_path):

        file_name: str = os.path.basename(file_path)
        return datetime.strptime(file_name.split('_')[0], self.batch_dt_id_format)

class FileZipDaoWriter(FileDaoBase):

    def __init__(self, root_folder, ts_ms: int):

        super().__init__(root_folder)
        self.ts_ms = ts_ms
        self.dt = Converter.ms2dt(ts_ms)

        self.batch_dt_id = self._get_batch_dt_id(self.dt)
        self.save_folder = self._get_save_folder_path(self.dt)
        if not os.path.exists(self.save_folder):
            os.makedirs(self.save_folder)

    def write(self, data: dict, attributes: dict, currency: str, kind: str) -> str:

        # create a data structure with data and attrbutes
        to_save = {self.attrib_file_name: attributes, self.data_file_name:data}

        zip_file_path = self._get_zip_file_path(self.save_folder, self.batch_dt_id, currency, kind)

        with zipfile.ZipFile(zip_file_path, mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:

            for fn, content in to_save.items():
                dumped_json = json.dumps(content, ensure_ascii=False, indent=4)
                zip_file.writestr(fn, data=dumped_json)
            
            zip_file.testzip()

        return zip_file_path

DataWithAttribs = namedtuple("DataWithAttrib", ["attributes", "data"])

class FileZipDaoReader(FileDaoBase):

    # def __init__(self, root_folder):
    # this trivial constructor is not required. 
    #     super().__init__(root_folder)

    def read(self, ts_ms: int, currency: str, kind: str, attrib_only = False, dt_tol_in_sec: int = 60) -> DataWithAttribs:

        # find file
        dt = Converter.ms2dt(ts_ms)
        dt_s, dt_e = [dt + timedelta(seconds=d*dt_tol_in_sec) for d in [-1, 1]]
        batch_id_s, batch_id_e = [self._get_batch_dt_id(t) for t in [dt_s, dt_e]]
        save_folder_s, save_folder_e = [self._get_save_folder_path(t) for t in [dt_s, dt_e]]

        batch_id_pattern = os.path.commonprefix([batch_id_s, batch_id_e]) + '*'

        candidates_files = glob.glob(self._get_zip_file_path(save_folder_s, batch_id_pattern, currency, kind))
        if save_folder_e != save_folder_s:
            candidates_files.extend(glob.glob(self._get_zip_file_path(save_folder_e, batch_id_pattern, currency, kind)))

        file_path = self.__find_file(candidates_files, dt)
        if file_path is None:
            _LOGGER.warn('no data found for ' + str(dt))
            return None, None

        return self.__read_zip(file_path, attrib_only)


    def __read_zip(self, file_path, attrib_only) -> DataWithAttribs:

        with zipfile.ZipFile(file_path, 'r') as zip_file:
            data = json.loads(zip_file.read(self.data_file_name)) if not attrib_only else None
            attribs = json.loads(zip_file.read(self.attrib_file_name))

        return DataWithAttribs(attribs, data)
        

    def __find_file(self, candidates_files: List[str], dt: datetime) -> str:
        
        if len(candidates_files) == 0:
            return None

        if len(candidates_files) == 1:
            return candidates_files[0]

        # find the one with the closest time stamp
        return sorted(candidates_files, key = lambda fn: abs(self._get_timestamp_from_file_path(fn) - dt))[0]


class QueryDB:

    def __init__(self, db_path: str):
        self.db_path = db_path

    def __conn(self):
        return sqlite3.connect(self.db_path)

    def execute_queries(self, queries: List[str] | str) ->  None:

        if type(queries) is str:
            queries = [queries] 
        conn = self.__conn()
        for q in queries:
            self._execute_query(conn, q)
        conn.close()

    def execute_read_query(self, query: str) -> List[tuple]:
        
        conn = self.__conn()
        res = self._execute_read_query(conn, query)
        conn.close()
        return res

    # DB queries
    @staticmethod
    def _execute_query(connection, query: str) -> None:
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        _LOGGER.info('executed successfully: ' + query)

    @staticmethod
    def _execute_read_query(connection, query: str) -> List[tuple]:
        cursor = connection.cursor()
        result = None
        cursor.execute(query)
        result = cursor.fetchall()
        _LOGGER.info('executed successfully: ' + query)
        return result

# taken from here: https://stackoverflow.com/questions/67436362/decorator-for-sqlite3/67436763#67436763
# this looks cool. but, I don't understand this well. 
# @contextmanager
# def db_ops(db_name):
#     conn = sqlite3.connect(db_name)
#     cur = conn.cursor()
#     yield cur
#     conn.commit()
#     conn.close()

class FilePathMappingDB:

    s_timestamp = 'timestamp_ms'
    s_datetime = 'datetime'
    s_currency = 'currency'
    s_kind = 'kind'
    s_path = 'path'
    s_db_name = 'ts2fp.sqlite3'
    s_mapping_table_name = 'timestamp2filepath'
    
    dt_format = '%Y%m%d%H%M%S'

    def __init__(self, root_folder):

        self.root_folder = root_folder
        self.db_path = os.path.join(root_folder, self.s_db_name)
        self.qdb = QueryDB(self.db_path)


    def mapping_table_exists(self):

        query = f'SELECT name FROM sqlite_master WHERE type="table" AND name="{self.s_mapping_table_name}";'
        res = self.qdb.execute_read_query(query)

        return len(res) > 0
        

    def create_mapping_table(self) -> None:

        if self.mapping_table_exists():
            return None

        # create table
        query_create_table = '\n'.join([
            f'CREATE TABLE IF NOT EXISTS "{self.s_mapping_table_name}" (',
            f'{self.s_timestamp} INT,'
            f'{self.s_datetime} TEXT,',
            f'{self.s_currency} TEXT,',
            f'{self.s_kind} TEXT,',
            f'{self.s_path} TEXT',
            ');'])

        # create index
        query_create_index = '\n'.join([
            f'CREATE INDEX IF NOT EXISTS "f{self.s_timestamp}_index" ON "{self.s_mapping_table_name}" (',
            f'"{self.s_timestamp}"',
            ');'
            ])
        self.qdb.execute_queries([query_create_table, query_create_index])

        return None
    
    def drop_mapping_table(self) -> None: 

        query = f'DROP TABLE {self.s_mapping_table_name}'
        self.qdb.execute_queries(query)
        return None

    def insert_mapping(self, timestamp_ms: int, currency: str, kind: str, path: str) -> None:

        dt_str = Converter.ms2dt(timestamp_ms).strftime(self.dt_format)

        query = '\n'.join([
            'INSERT INTO',
            f'   {self.s_mapping_table_name} ({self.s_timestamp}, {self.s_datetime}, {self.s_currency}, {self.s_kind}, {self.s_path})',
            'VALUES',
            f'  ({str(timestamp_ms)}, "{dt_str}", "{currency}", "{kind}", "{path}");'
        ])

        self.qdb.execute_queries(query)    


    def get_count(self) -> int:

        query = f'SELECT count(*) from {self.s_mapping_table_name}'
        res = self.qdb.execute_read_query(query)
        return res[0][0]

    def get_min_max_timestamp(self) -> Tuple[float]:

        query = f'SELECT min({self.s_timestamp}), max({self.s_timestamp}) FROM {self.s_mapping_table_name}'
        res = self.qdb.execute_read_query(query)
        return res[0]


    def remove(self, from_timestamp: int = None, to_timestamp: int = None) -> None:

        if not self.mapping_table_exists():
            return None
        
        if self.get_count() == 0: 
            return None

        ts_min, ts_max = self.get_min_max_timestamp()
        if from_timestamp is not None:
            from_timestamp_str = str(np.maximum(ts_min, from_timestamp))
        if to_timestamp is not None:
            to_timestamp_str = str(np.minimum(ts_max, to_timestamp))

        query = f'DELETE FROM {self.s_mapping_table_name} ' \
              + f'WHERE {self.s_timestamp} >= {from_timestamp_str} and {self.s_timestamp} <= {to_timestamp_str}'
        self.qdb.execute_queries(query)

        return None

