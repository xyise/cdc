import os
from collections import namedtuple
from tabnanny import check
from typing import List
from datetime import datetime
import numpy as np
import pandas as pd
import pymongo as mdb

from ..common_utils import Converter, get_logger
from .shared_structures import TickerBatchInfo

_LOGGER = get_logger(__name__)

class DBManager:

    dt_id_format = '%Y%m%d%H%M%S'

    s_batch_timestamp = 'batch_timestamp'
    s_datetime = 'datetime'
    s_currency = 'currency'
    s_kind = 'kind'
    s_path = 'path'
    s_attributes = 'attributes',
    s_data = 'data'
    s_batch = 'batch'
    s_ticker_batch = 'ticker_batch'
    s_last_trade = 'last_trade'


    def __init__(self, db_name='CDC', host='mongodb://localhost', port=27017):

        self.db_client = mdb.MongoClient(host=host, port=port)
        self.db_name = db_name
        self.db = self.db_client[db_name] # this is Mongo DB way connecting to a database

        self.ticker_batch_col_name = self.s_ticker_batch
        self.last_trade_col_name = self.s_last_trade

        #### collections:
        # ticker batches
        self.col_ticker_batch = self.db[self.ticker_batch_col_name]
        self.col_ticker_batch.create_index([(self.s_batch_timestamp, mdb.ASCENDING)])
        self.col_ticker_batch.create_index([(self.s_currency, mdb.ASCENDING)])
        self.col_ticker_batch.create_index([(self.s_kind, mdb.ASCENDING)])

    def __get_ticker_batch_keys(self, batch_timestamp: int, currency, kind):

        return {
            self.s_batch_timestamp: batch_timestamp,
            self.s_currency: currency,
            self.s_kind: kind
            }

    def insert_ticker_batch(self, batch_timestamp: int, currency, kind, batch, check_exists = True) -> None:

        ticker_batch = self.__get_ticker_batch_keys(batch_timestamp, currency, kind)

        b_insert = True

        if check_exists:        
            b_insert = self.col_ticker_batch.count_documents(ticker_batch) == 0

        if b_insert:
            _LOGGER.info(f'inserting a ticker batch {ticker_batch}')

            # add batch and insert
            ticker_batch[self.s_batch] = batch
            self.col_ticker_batch.insert_one(ticker_batch)
        else:
            _LOGGER.info(f'skipping. already exists: {ticker_batch}')

    def find_ticker_batch(self, batch_timestamp: int, currency, kind) -> dict:

        res = self.col_ticker_batch.find_one(
                self.__get_ticker_batch_keys(batch_timestamp, currency, kind))

        return res
    
    def exist_ticker_batch(self, batch_timestamp, currency, kind) -> bool:

        return self.col_ticker_batch.count_documents(self.__get_ticker_batch_keys(batch_timestamp, currency, kind)) > 0

