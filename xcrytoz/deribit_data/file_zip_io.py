
import os
from typing import Callable, List, Dict, Tuple, Any
from datetime import datetime, timedelta
from abc import abstractclassmethod, ABC
from collections import namedtuple
import itertools
import time
import json, zipfile
import sqlite3
import numpy as np
import pandas as pd

from ..common_utils import get_logger, Converter
from .deribit_fields import DeribitFields
from .downloader import DeribitDownloader_Simple

_LOGGER = get_logger(__name__)

_cst = DeribitFields()

class FileZipBase(ABC):

    data_file_name = 'data.json'
    attrib_file_name = 'attributes.json'

    def __init__(self, root_folder):

        self.root_folder = root_folder

    def _get_zip_file_path(self, save_folder, dt_id: str, currency: str, kind: str) -> str:

        return os.path.join(save_folder, '_'.join([dt_id, currency, kind]) + '.zip')


class FileZipWriter(FileZipBase):

    def __init__(self, root_folder, save_subfolder, file_header: str):

        super().__init__(root_folder)

        self.file_header = file_header
        self.save_folder = os.path.join(self.root_folder, save_subfolder)
        if not os.path.exists(self.save_folder):
            os.makedirs(self.save_folder)

    def write(self, data: dict, attributes: dict, currency: str, kind: str) -> str:

        # create a data structure with data and attrbutes
        to_save = {self.attrib_file_name: attributes, self.data_file_name:data}

        zip_file_path = self._get_zip_file_path(self.save_folder, self.file_header, currency, kind)

        with zipfile.ZipFile(zip_file_path, mode='w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file:

            for fn, content in to_save.items():
                dumped_json = json.dumps(content, ensure_ascii=False, indent=4)
                zip_file.writestr(fn, data=dumped_json)
            
            zip_file.testzip()

        return zip_file_path

DataWithAttribs = namedtuple('DataWithAttrib', ['timestamp', 'attributes', 'data'])
ZipFileNameInfo = namedtuple('ZipFileNameInfo', ['timestamp', 'currency', 'kind'])

# NOTE: alternative way would be https://stackoverflow.com/questions/67436362/decorator-for-sqlite3/67436763#67436763
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

class FileZipReader_TickerInfo(FileZipBase):

    dt_id_format = '%Y%m%d%H%M%S'

    s_timestamp = 'timestamp'
    s_datetime = 'datetime'
    s_currency = 'currency'
    s_kind = 'kind'
    s_path = 'path'
    s_db_name = 'ts2fp.sqlite3'
    s_mapping_table = 'timestamp2filepath'
    mapping_table_columns = [s_timestamp, s_datetime, s_currency, s_kind, s_path]

    def __init__(self, root_folder):
        super().__init__(root_folder)

        self.db_path = os.path.join(root_folder, self.s_db_name)
        self.qdb = QueryDB(self.db_path)

    def mapping_table_exists(self):

        query = f'SELECT name FROM sqlite_master WHERE type="table" AND name="{self.s_mapping_table}";'
        res = self.qdb.execute_read_query(query)

        return len(res) > 0
        
    def prepare_mapping_table(self, drop_existing_table) -> None:

        if self.mapping_table_exists():
            if drop_existing_table:
                _LOGGER.info('dropping the existing DB')
                self.drop_mapping_table()
            else:
                _LOGGER.info('mapping exists, and doing nothing..')
                return None

        # create table
        query_create_table = '\n'.join([
            f'CREATE TABLE IF NOT EXISTS "{self.s_mapping_table}" (',
            f'{self.s_timestamp} INT,'
            f'{self.s_datetime} TEXT,',
            f'{self.s_currency} TEXT,',
            f'{self.s_kind} TEXT,',
            f'{self.s_path} TEXT',
            ');'])

        # create index
        query_create_index = '\n'.join([
            f'CREATE INDEX IF NOT EXISTS "f{self.s_timestamp}_index" ON "{self.s_mapping_table}" (',
            f'"{self.s_timestamp}"',
            ');'
            ])
        self.qdb.execute_queries([query_create_table, query_create_index])

        return None
    
    def drop_mapping_table(self) -> None: 
    
        query = f'DROP TABLE {self.s_mapping_table}'
        self.qdb.execute_queries(query)
        return None

    def insert_mapping(self, timestamp: int, currency: str, kind: str, path: str) -> None:

        dt_str = Converter.ms2dt(timestamp).strftime(self.dt_id_format)

        query = '\n'.join([
            'INSERT INTO',
            f'   {self.s_mapping_table} ({self.s_timestamp}, {self.s_datetime}, {self.s_currency}, {self.s_kind}, {self.s_path})',
            'VALUES',
            f'  ({str(timestamp)}, "{dt_str}", "{currency}", "{kind}", "{path}");'
        ])

        self.qdb.execute_queries(query)    


    def get_mapping_table_count(self) -> int:

        query = f'SELECT count(*) from {self.s_mapping_table}'
        res = self.qdb.execute_read_query(query)
        return res[0][0]

    def get_min_max_timestamp(self) -> Tuple[float]:

        query = f'SELECT min({self.s_timestamp}), max({self.s_timestamp}) FROM {self.s_mapping_table}'
        res = self.qdb.execute_read_query(query)
        return res[0]

    def get_valid_ts_from_to(self, from_timestamp: int = None, to_timestamp: int = None) -> Tuple[int]:

        ts_min, ts_max = self.get_min_max_timestamp()
        if from_timestamp is not None:
            ts_min = np.maximum(ts_min, from_timestamp)
        if to_timestamp is not None:
            ts_max = np.minimum(ts_max, to_timestamp)
        
        return ts_min, ts_max

    def remove_mappings(self, from_timestamp: int = None, to_timestamp: int = None) -> None:

        if not self.mapping_table_exists():
            return None
        
        if self.get_mapping_table_count() == 0: 
            return None

        ts_min, ts_max = self.get_valid_ts_from_to(from_timestamp, to_timestamp)

        query = f'DELETE FROM {self.s_mapping_table} ' \
              + f'WHERE {self.s_timestamp} >= {str(ts_min)} AND {self.s_timestamp} <= {str(ts_max)}'
        self.qdb.execute_queries(query)

        return None

    def get_mapping_in_range(self, currency: str, kind: str, 
                    from_timestamp: int = None, to_timestamp: int = None, 
                    columns: list = None) -> pd.DataFrame:

        if columns is None:
            columns = self.mapping_table_columns
            columns_str = '*'
        else:
            if type(columns) is str:
                columns = [columns]
            columns_str = ','.join(columns)
        
        ts_min, ts_max = self.get_valid_ts_from_to(from_timestamp, to_timestamp)

        query = f'SELECT {columns_str} FROM {self.s_mapping_table}\n'\
              + f'WHERE {self.s_timestamp} >= {str(ts_min)} AND {self.s_timestamp} <= {str(ts_max)}' \
              + f' AND {self.s_currency} = "{currency}" AND {self.s_kind} = "{kind}"'
        
        res = self.qdb.execute_read_query(query)

        # convert to dataframe and sort by timestamp (if it is there)
        df_res = pd.DataFrame(data=res, columns=columns)
        if self.s_timestamp in columns:
            df_res.sort_values(by=self.s_timestamp, inplace=True)

        return df_res

    def get_one_mapping(self, currency: str, kind: str, timestamp: int) -> pd.Series:

        query = f'SELECT * FROM {self.s_mapping_table}\n'\
              + f'WHERE {self.s_timestamp} = {str(timestamp)} AND {self.s_currency} = "{currency}" AND {self.s_kind} = "{kind}"'

        res = self.qdb.execute_read_query(query)
        df_res = pd.DataFrame(data=res, columns=self.mapping_table_columns)
        if df_res.shape[0] == 0:
            _LOGGER.warning('No entry is found. Returning None.')
            return None
        
        if df_res.shape[0] > 1:
            _LOGGER.warning('There are more than one items. DB is compromised. Returning an arbitrary item.') 
        return df_res.iloc[0]

    def get_closet_mapping(self, currency: str, kind: str, timestamp: int) -> pd.Series:
        
        tss = self.get_mapping_in_range(currency, kind, columns=self.s_timestamp)[self.s_timestamp].to_numpy()

        idx_min = np.argmin(np.abs(tss - timestamp))
        ts_closet = tss[idx_min]

        return self.get_one_mapping(currency, kind, ts_closet)

    def collect_saved_file_info_path_pairs(self, from_timestamp: int = None, to_timestamp: int = None) -> List[tuple]:

        if from_timestamp is None:
            from_timestamp = -np.inf
        if to_timestamp is None:
            to_timestamp = np.inf

        # find all subfolders with digits
        data_folder_names = sorted([path.name for path in os.scandir(self.root_folder) 
                            if (path.is_dir() and os.path.basename(path.path).isdigit())])

        # name, folder pairs
        info_path_pairs = []
        for folder_name in data_folder_names:
            for file_path in os.scandir(os.path.join(self.root_folder, folder_name)):

                filename_w_ext = file_path.name
                fn_path_wo_root = os.path.join(folder_name, filename_w_ext)
                fn_info = self.__filename2info(filename_w_ext)

                if (fn_info.timestamp >= from_timestamp) and (fn_info.timestamp <= to_timestamp):             
                    info_path_pairs.append((fn_info, fn_path_wo_root))

        return info_path_pairs

    def bulk_mapping_update(self, from_ts: int = None, to_ts: int = None) -> None:

        info_path_pairs  = self.collect_saved_file_info_path_pairs(from_ts, to_ts)
        self.remove_mappings(from_ts, to_ts)

        # this kind of type hinting is a bit annoying
        info: ZipFileNameInfo
        path: str
        for info, path in info_path_pairs:
            self.insert_mapping(info.timestamp, info.currency, info.kind, path)

    def __read_zip(self, file_path_wo_root, attrib_only=False) -> DataWithAttribs:
        file_path = os.path.join(self.root_folder, file_path_wo_root)

        with zipfile.ZipFile(file_path, 'r') as zip_file:
            data = json.loads(zip_file.read(self.data_file_name)) if not attrib_only else None
            attribs = json.loads(zip_file.read(self.attrib_file_name))

        return DataWithAttribs(None, attribs, data)

    def read(self, currency:str , kind: str, timestamp: int, exact_timestamp = True) -> DataWithAttribs:

        if exact_timestamp:
            ds = self.get_one_mapping(currency, kind, timestamp)
        else:
            ds = self.get_closet_mapping(currency, kind, timestamp)

        res = self.__read_zip(ds[self.s_path])

        # append timestamp
        return DataWithAttribs(ds[self.s_timestamp], res.attributes, res.data)


    def __filename2info(self, filename_w_ext: str) -> ZipFileNameInfo:

        filename_wo_ext, _ = os.path.splitext(filename_w_ext)

        dt_str, currency, kind = filename_wo_ext.split('_')
        dt = datetime.strptime(dt_str, self.dt_id_format)
        ts = Converter.dt2ms_int(dt)
        return ZipFileNameInfo(ts, currency, kind)

