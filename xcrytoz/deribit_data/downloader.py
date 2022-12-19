import json
import time
from time import sleep

import numpy as np
import websocket

from ..common_utils import get_logger
# import within package
from .shared_structures import DeribitFields

_LOGGER = get_logger(__name__)


# this is to make the variable name shorter
_cst = DeribitFields()


class DeribitDownloader_Simple:
    ''' simple downloader using websocket. needs to be improved. '''

    deribit_ws_live = "wss://www.deribit.com/ws/api/v2"
    jsonrpc_version = "2.0"

    @staticmethod
    def __make_now_timestamp_if_none(id: int = None) -> int:
        if id is None:
            return int(time.time())
        return id

    def __init__(self):

        # initialise websocket
        self.ws = websocket.WebSocket()

    def download_tickers(self, currency='BTC', kind='option', sleep_in_sec=0.05):

        # go to live server
        self.ws.connect(self.deribit_ws_live)

        msg = self.__make_msg_get_instruments(currency, kind)
        received_instruments = self.__download_no_check(msg)

        if _cst.result not in received_instruments:
            self.ws.close()
            raise Exception('failed to receive a list of instruments')

        # sort instruments by expiration timestamp so that the options with the same expiry are
        # downloaded about the same timestamp
        instruments = sorted(received_instruments[_cst.result], key=lambda inst: inst[_cst.expiration_timestamp])

        # collect tickers
        tickers = []
        missing_ticker_instruments = []
        for inst in instruments:
            inst_name = inst[_cst.instrument_name]
            msg = self.__make_msg_ticker(inst_name)
            received_ticker = self.__download_no_check(msg)
            if _cst.result in received_ticker:
                tickers.append(received_ticker[_cst.result])
            else:
                missing_ticker_instruments.append(inst_name)
            sleep(sleep_in_sec)

        self.ws.close()
        return {
            'instruments': instruments,
            'tickers': tickers,
            'missing': missing_ticker_instruments
        }

    def download_last_trades(
            self, currency, kind,
            start_timestamp_exclusive, end_timestamp_inclusive,
            max_count=1000, ts_split=20):

        download_iter = self.get_iter_download_last_trades(
            currency, kind, start_timestamp_exclusive, end_timestamp_inclusive, max_count, ts_split)
        return [r for r in download_iter]

    def get_iter_download_last_trades(
            self, currency, kind,
            start_timestamp_exclusive, end_timestamp_inclusive,
            max_count=1000, ts_split=20, level=0):

        indent = '*   ' * level

        _LOGGER.info(indent + 'requesting from ' + str(start_timestamp_exclusive) +
                     ' to ' + str(end_timestamp_inclusive))
        recvd = self.get_last_trades_by_currency_and_time(
            currency, kind, start_timestamp_exclusive + 1, end_timestamp_inclusive, max_count)
        if recvd['result']['has_more']:
            _LOGGER.info(indent + 'Splitting into ' + str(ts_split) + ' sub-tasks')
            ts_se = np.linspace(start_timestamp_exclusive, end_timestamp_inclusive, ts_split + 1)
            for i in range(ts_split):
                time.sleep(0.05)
                ts_s = int(ts_se[i])
                ts_e = int(ts_se[i+1])
                yield from self.get_iter_download_last_trades(
                    currency, kind, ts_s+1, ts_e, max_count, ts_split, level + 1)
        else:
            _LOGGER.info(indent + 'returning data')
            yield recvd

    def get_last_trades_by_instrument_and_time(self, instrument_name,
                                               start_timestamp=None, end_timestamp=None, count=10):

        self.ws.connect(self.deribit_ws_live)
        msg = self.__make_get_last_trades_by_instrument_and_time(
            instrument_name, start_timestamp, end_timestamp, count)
        received = self.__download_no_check(msg)
        self.ws.close()

        return received

    def get_last_trades_by_currency_and_time(self, currency, kind,
                                             start_timestamp=None, end_timestamp=None, count=10):

        self.ws.connect(self.deribit_ws_live)
        msg = self.__make_get_last_trades_by_currency_and_time(
            currency, kind, start_timestamp, end_timestamp, count)
        received = self.__download_no_check(msg)
        self.ws.close()

        return received

    def __download_no_check(self, msg: dict):

        # assumes the socket is open
        # ideally, we should do async programming - that is for next time.
        self.ws.send(json.dumps(msg))
        received = json.loads(self.ws.recv())

        # todo: probably needs to check whether the received has 'result' key.
        return received

    def __make_msg_get_instruments(self, currency: str, kind: str, expired=False, id: int = None) -> dict:
        msg = {
            "jsonrpc": self.jsonrpc_version,
            "id": self.__make_now_timestamp_if_none(id),
            "method": "public/get_instruments",
            "params": {
                "currency": currency,
                "kind": kind,
                "expired": expired
            }
        }
        return msg

    def __make_msg_ticker(self, instrument_name: str, id: int = None) -> dict:

        msg = {
            "jsonrpc": self.jsonrpc_version,
            "id": self.__make_now_timestamp_if_none(id),
            "method": "public/ticker",
            "params": {
                "instrument_name": instrument_name
            }
        }
        return msg

    def __make_get_last_trades_by_instrument_and_time(
            self,
            instrument_name,
            start_timestamp=None, end_timestamp=None, count=10, id: int = None):

        msg = {
            "jsonrpc": self.jsonrpc_version,
            "id": self.__make_now_timestamp_if_none(id),
            "method": "public/get_last_trades_by_instrument_and_time",
            "params": {
                "instrument_name": instrument_name,
                "count": count
            }
        }
        if start_timestamp is not None:
            msg["params"]["start_timestamp"] = start_timestamp
        if end_timestamp is not None:
            msg["params"]["end_timestamp"] = end_timestamp

        return msg

    def __make_get_last_trades_by_currency_and_time(
            self,
            currency, kind,
            start_timestamp=None, end_timestamp=None, count=10, id: int = None):

        msg = {
            "jsonrpc": self.jsonrpc_version,
            "id": self.__make_now_timestamp_if_none(id),
            "method": "public/get_last_trades_by_currency_and_time",
            "params": {
                "currency": currency,
                "kind": kind,
                "count": count
            }
        }
        if start_timestamp is not None:
            msg["params"]["start_timestamp"] = start_timestamp
        if end_timestamp is not None:
            msg["params"]["end_timestamp"] = end_timestamp

        return msg
