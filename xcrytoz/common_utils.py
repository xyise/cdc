import sys
from datetime import datetime
import logging


class Converter:

    @staticmethod
    def sec2ms_int(sec: float) -> int:
        return int(sec * 1000.0)

    @staticmethod
    def dt2ms_int(dt: datetime) -> int:

        return Converter.sec2ms_int(dt.timestamp())

    @staticmethod
    def ms2dt(millisec: [float, int]) -> datetime:

        return datetime.fromtimestamp(millisec / 1000.0)




def get_logger(name: str, level = logging.INFO):

    my_logger = logging.getLogger(name)
    my_logger.setLevel(level) # level should be set here and at handler as well

    # if logger has a handler, it is already created. so, return it as. 
    if my_logger.hasHandlers():
        return my_logger

    c_handler = logging.StreamHandler(sys.stdout)
    c_handler.setLevel(level)
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    my_logger.addHandler(c_handler)

    return my_logger

