from collections import namedtuple

TickerBatchInfo = namedtuple('TickerBatchInfo', ['batch_timestamp', 'currency', 'kind', 'path'])


class ConstantsBase():

    def __repr__(self):
        return ','.join([x for x in self.__dir__() if not x.startswith('__')])


class DeribitConstants(ConstantsBase):

    data = 'data'
    attributes = 'attributes'
    data_file_name = 'data.json'
    attributes_file_name = 'attributes.json'
    YYYYMMDDhhmmss = '%Y%m%d%H%M%S'
    YYYYMM = '%Y%m'


class DeribitFields(ConstantsBase):
    # keys

    delta = 'delta'
    expiration_timestamp = 'expiration_timestamp'
    greeks = 'greeks'
    instruments = 'instruments'
    instrument_name = 'instrument_name'
    mark_iv = 'mark_iv'
    mark_price = 'mark_price'
    option_type = 'option_type'
    result = 'result'
    stats = 'stats'
    strike = 'strike'
    tickers = 'tickers'
    underlying_price = 'underlying_price'

    # values
    future = 'future'
    option = 'option'
    call = 'call'
    put = 'put'
