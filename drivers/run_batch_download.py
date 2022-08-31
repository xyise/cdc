from importlib.resources import path
import os, sys, pathlib
from datetime import datetime

# to add path required (required before packageds)
for pp in [str(pathlib.Path(__file__).resolve().parent.parent)]:
    if pp not in sys.path:
        sys.path.append(pp)

from xcrytoz import batch_download_and_zip, Converter

if __name__ == '__main__':

    home_path = str(pathlib.Path.home())

    # we work everything using utc time (no local times)
    tsms_utcnow = Converter.dt2ms_int(datetime.utcnow())
    root_save_folder = os.path.join(home_path, 'data', 'deribit')
    # now run. 
    batch_download_and_zip(tsms_utcnow, root_save_folder)

