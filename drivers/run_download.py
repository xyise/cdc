import argparse
import os
import pathlib
import sys
from datetime import datetime

from xcrytoz.common_utils import Converter, get_logger
from xcrytoz.deribit_data import (LastTradeBatchDownloader,
                                  TickerBatchDownloader)

# to add path required (required before packageds)
for pp in [str(pathlib.Path(__file__).resolve().parent.parent)]:
    if pp not in sys.path:
        sys.path.append(pp)


_LOGGER = get_logger(__name__)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('run_type', help='which run to execute', choices=['ticker', 'last_trade'])
    parser.add_argument('--live', help='run in the live mode.', action="store_true")
    args = parser.parse_args()
    run_type = args.run_type

    target_folder = 'deribit' if args.live else 'test_deribit'

    home_path = str(pathlib.Path.home())

    dt_utc_now = datetime.utcnow()

    currencies = ['BTC', 'ETH', 'SOL']
    kinds = ['future', 'option']

    if run_type == 'ticker':

        # we work everything using utc time (no local times)
        ts_utcnow_in_msec = Converter.dt2ms_int(dt_utc_now)
        root_folder = os.path.join(home_path, 'data', target_folder)
        # now run.

        TickerBatchDownloader(root_folder, ts_utcnow_in_msec).download_batches(currencies, kinds)

    elif run_type == 'last_trade':
        root_folder = os.path.join(home_path, 'data', target_folder + '_trade')

        # this is expected to run on hourly basis
        end_datetime = datetime(dt_utc_now.year, dt_utc_now.month, dt_utc_now.day, dt_utc_now.hour, 0, 0)
        end_timestamp = Converter.dt2ms_int(end_datetime)
        start_timestamp = end_timestamp - 60 * 60 * 1000

        LastTradeBatchDownloader(root_folder, start_timestamp, end_timestamp).download_batches(currencies, kinds)

    else:
        raise Exception('unknown run type: ' + run_type + '. either "ticker", or "last_trade"')

    _LOGGER.info('done')
