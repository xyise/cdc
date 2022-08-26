import os, pathlib

from datetime import datetime
from deribit_downloader import batch_download_and_zip



if __name__ == '__main__':

    home_path = str(pathlib.Path.home())

    dt_now = datetime.now()
    batch_id = dt_now.strftime('%Y%m%d%H%M%S')

    root_save_folder = os.path.join(home_path, 'data', 'deribit')
    save_folder = os.path.join(root_save_folder, dt_now.strftime('%Y%m'))
    if not os.path.exists(save_folder):
        os.makedirs(save_folder)

    # now run. 
    batch_download_and_zip(batch_id, save_folder)

