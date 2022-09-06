from collections import namedtuple
import pandas as pd

from ..common_utils import get_logger
from .deribit_fields import DeribitFields



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
