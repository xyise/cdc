import time
from time import sleep
import json
import websocket


# import within package
from ..common_utils import Col2Str, get_logger

_LOGGER = get_logger(__name__)

# TODOs:
# logging
# async programming for websocket

# this is to make the variable name shorter
_cst = Col2Str()

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

    def download_tickers(self, currency='BTC', kind='option', sleep_in_sec = 0.05):
        
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
            'instruments':instruments, 
            'tickers':tickers, 
            'missing': missing_ticker_instruments
        }



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
            "params":{
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
            "params":{
                "instrument_name": instrument_name
            }
        }
        return msg



