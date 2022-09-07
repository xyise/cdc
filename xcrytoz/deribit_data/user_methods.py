
from typing import List
import pandas as pd

from xcrytoz.deribit_data.db_manager import DBManager

from ..common_utils import get_logger
from .shared_structures import DeribitFields, TickerBatchInfo
from .batch_managers import BatchFileManager

_LOGGER = get_logger(__name__)

_cst = DeribitFields()

class ConverterToDF:

    @staticmethod
    def tick_info_to_df(deribit_option_ticker_info: dict):

            df_instruments = pd.DataFrame(deribit_option_ticker_info[_cst.instruments])
            df_tickers = pd.DataFrame(deribit_option_ticker_info[_cst.tickers])

            # merge instruments & tickers
            df_md = pd.merge(df_tickers, df_instruments, how='left', on=_cst.instrument_name)
            # expand greeks
            df_md = pd.concat([
                df_md, 
                pd.DataFrame({i:r[_cst.greeks] for i, r in df_md.iterrows()}).T,
                pd.DataFrame({i:r[_cst.stats] for i, r in df_md.iterrows()}).T
                ], axis=1)
            # let's order
            df_md.sort_values([_cst.expiration_timestamp, _cst.strike], inplace=True)

            return df_md

    @staticmethod
    def last_trade_info_to_df(deribit_last_trades: list):
        return pd.concat([pd.DataFrame(b['result']['trades']) for b in deribit_last_trades])


def bulk_upload_ticker_batches(batch_data_root_folder, from_timestamp: int = None, to_timestamp: int = None,
                                db_name='CDC', host='mongodb://localhost', port=27017):
    
    bdm = BatchFileManager(batch_data_root_folder)
    ticker_batch_infos = bdm.get_ticker_batch_file_infos()
    _LOGGER.info(f'There are {len(ticker_batch_infos)} batch files.')

    # database manager
    dbm = DBManager(db_name, host, port)

    ticker_batch_infos_to_add: List[TickerBatchInfo] = []
    for bi in ticker_batch_infos:
        if not dbm.exist_ticker_batch(bi.batch_timestamp, bi.currency, bi.kind):
            ticker_batch_infos_to_add.append(bi)
    _LOGGER.info(f'New {len(ticker_batch_infos_to_add)} batch files to add')

    # upload them
    for bi in ticker_batch_infos_to_add:
        this_batch = bdm.read_batch(bi.path)
        # since the items are already checked, we can insert without checking whether the batch
        # exists or not. 
        dbm.insert_ticker_batch(bi.batch_timestamp, bi.currency, bi.kind, this_batch, False)
    _LOGGER.info(f'done')


