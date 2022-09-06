from importlib.resources import path
import os, sys, pathlib
from datetime import datetime

# to add path required (required before packageds)
for pp in [str(pathlib.Path(__file__).resolve().parent.parent)]:
    if pp not in sys.path:
        sys.path.append(pp)

from xcrytoz.common_utils import Converter
from xcrytoz.deribit_data import BatchDownloaderZip_TickerInfo, BatchDownloaderZip_LastTradeInfo

if __name__ == '__main__':
    
    args = sys.argv[1:]

    if len(args) == 1:
        run_type = args[0]
    else:
        run_type = '' # 'last_trade' # 'last_trade'

    home_path = str(pathlib.Path.home())

    dt_utc_now = datetime.utcnow()

    if run_type == 'ticker':

        # we work everything using utc time (no local times)
        ts_utcnow_in_msec = Converter.dt2ms_int(dt_utc_now)
        root_folder = os.path.join(home_path, 'data', 'deribit')
        # now run. 
        BatchDownloaderZip_TickerInfo(root_folder, ts_utcnow_in_msec).download()

    elif run_type == 'last_trade':
        kinds = ['future', 'option']
        root_folder = os.path.join(home_path, 'data', 'deribit_last_trade')

        # this is expected to run on hourly basis
        end_datetime = datetime(dt_utc_now.year, dt_utc_now.month, dt_utc_now.day, dt_utc_now.hour, 0, 0)
        end_timestamp = Converter.dt2ms_int(end_datetime)
        start_timestamp = end_timestamp - 60 * 60 * 1000

        BatchDownloaderZip_LastTradeInfo(root_folder, start_timestamp, end_timestamp, kinds = kinds).download()

    else:
        raise Exception('unknown run type: ' + run_type + '. either "ticker", or "last_trade"')