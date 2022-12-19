from abc import ABC, abstractclassmethod
from typing import Callable, Union

import numpy as np
import pandas as pd

from xcrytoz.deribit_data import ConverterToDF

from ..deribit_data.shared_structures import DeribitFields
from .utils import linear_interp_flat_extrap

# from ..common_utils import get_logger, Converter
# from .downloader import DeribitDownloader_Simple

# NOTE:
# all time stamps integers, measured in milliseconds.


class VolatilitySurface(ABC):

    s_ATMF = 'ATMF'
    s_RR = 'RR'
    s_FLY = 'FLY'
    s_P = 'P'
    s_C = 'C'

    s_expiration = 'expiration'
    s_expiration_timestamp = 'expiration_timestamp'
    s_forward = 'forward'
    s_strike = 'strike'
    s_volatility = 'volatility'
    s_volatility_pac = 'volatility_pac'
    s_neg_put_delta_pac = 'neg_put_delta_pac'
    s_volatility_arf = 'volatility_arf'
    s_extrapolated = 'extrapolated'
    s_extrapolated_pac = 'extrapolated_pac'
    s_extrapolated_arf = 'extrapolated_arf'

    @staticmethod
    def f2str100(x):
        return str(round(x*100))

    def __init__(self, name: str, timestamp: int):
        if not np.issubdtype(type(timestamp), np.integer):
            raise TypeError('timestamp ' + str(timestamp) + ' is not an integer.')
        self.name: str = name
        self.as_of_timestamp: int = timestamp

    def get_as_of_timestamp(self) -> int:
        return self.as_of_timestamp

    # @abstractclassmethod
    def get_black_volatility(self, expiration_timestamp: int, strike: np.ndarray) -> np.ndarray:
        pass

    # @abstractclassmethod
    def get_strike_at_npdelta(self, expiration_timestamp: int, neg_put_delta: np.ndarray) -> np.ndarray:
        pass

    # @abstractclassmethod
    def get_npdelta_at_strike(self, expiration_timestamp: int, strike: np.ndarray) -> np.ndarray:
        pass

    @abstractclassmethod
    def build(self, *args, **kwargs):
        pass

    def get_negputdel_label(self, npd: float, tol=1e-12) -> str:

        if np.abs(npd - 0.5) < tol:
            return self.s_ATMF
        if npd < 0.5:
            return self.f2str100(npd) + 'P'
        if npd > 0.5:
            return self.f2str100(1.0 - npd) + 'C'

    def extend_to_full_negputdeltas(self, npdeltas_half: Union[list, np.ndarray]) -> np.ndarray:

        # first sort
        npdeltas_s = np.sort(npdeltas_half)
        # then, append 0.5 and 1 - reverse(npdeltas_s)
        npdelta_full = np.hstack((npdeltas_s, 0.5, 1.0 - npdeltas_s[::-1]))

        return npdelta_full


# constants from deribit data
_cst = DeribitFields()


class VolatilitySurfaceDeribit(VolatilitySurface):

    def __init__(self, name: str, timestamp: int, deribit_option_data: dict):

        super().__init__(name, timestamp)

        df_md = ConverterToDF.tick_info_to_df(deribit_option_data)

        # option data, organise this put expiration and option type
        self.kkw_md = {ex: {ot: df_ot for ot, df_ot in df_ex.groupby(_cst.option_type)}
                       for ex, df_ex in df_md.groupby(_cst.expiration_timestamp)}

        self.missing_instruments: list = []
        if 'missing' in deribit_option_data:
            self.missing_instruments: list = deribit_option_data['missing']

        # to be defined
        self.ds_fwd: pd.Series
        self.target_npdeltas: np.ndarray
        self.interp_extrap: Callable
        self.df_md_pac: pd.DataFrame
        self.df_md_arf: pd.DataFrame
        self.df_md_combined: pd.DataFrame

    def build(self, target_neg_put_deltas_half=[0.1, 0.25], *args, **kwargs):

        # set forwards: keep the forward price by taking average for each expiry
        kw_fwd = {}
        for ex, kw_md in self.kkw_md.items():
            kw_fwd[ex] = np.mean(np.hstack([df[_cst.underlying_price].to_numpy() for df in kw_md.values()]))
        self.ds_fwd: pd.Series = pd.Series(kw_fwd)
        self.ds_fwd.index.name = self.s_expiration_timestamp

        # target neg put deltas
        self.target_npdeltas = self.extend_to_full_negputdeltas(target_neg_put_deltas_half)

        # As a starter, use linear interp & flat extrapolator.
        self.interp_extrap = linear_interp_flat_extrap

        # run each expiry and collect them
        kw_md_pac_ex = {ex_ts: self.__get_md_at_npdeltas(ex_ts) for ex_ts in self.kkw_md}

        # index: expiration_timestamp, columns: (fields, label) where fields = (strike, volatility, extrapolated)
        self.df_md_pac = pd.concat(kw_md_pac_ex).unstack()
        self.df_md_pac.index.name = self.s_expiration_timestamp

        # set md by atmf, rr, fly
        self.df_md_arf = self.__get_md_atmf_rr_fly()
        self.df_md_arf.index.name = self.s_expiration_timestamp

        df_fwd = pd.DataFrame(self.ds_fwd, columns=pd.MultiIndex.from_arrays([[self.s_forward], [self.s_forward]]))
        self.df_md_combined = pd.concat([df_fwd, self.df_md_pac, self.df_md_arf], axis=1)

    def get_surface_summary_in_npdelta(self) -> pd.DataFrame:

        return self.df_md_combined

    def __get_md_at_npdeltas(self, expiration_timestamp: int) -> pd.DataFrame:

        # market data: use put (deribit has the same vol info for put & call)
        df_p = self.kkw_md[expiration_timestamp][_cst.put]
        md_npdeltas = - df_p[_cst.delta].to_numpy()
        md_strikes = df_p[_cst.strike].to_numpy()
        md_vols = df_p[_cst.mark_iv].to_numpy()

        # interpolates at 'deltaP, ATM, deltaC' (PAC)
        npd_min, npd_max = md_npdeltas.min(), md_npdeltas.max()
        pac_extrapolated = (self.target_npdeltas < npd_min) | (self.target_npdeltas > npd_max)
        pac_npdeltas = np.clip(self.target_npdeltas, npd_min, npd_max)
        pac_strikes = self.interp_extrap(md_npdeltas, md_strikes)(pac_npdeltas)
        pac_vols = self.interp_extrap(md_npdeltas, md_vols)(pac_npdeltas)

        return pd.DataFrame(
            index=np.array([self.get_negputdel_label(npd) for npd in self.target_npdeltas]),
            data={self.s_neg_put_delta_pac: pac_npdeltas,
                  self.s_strike: pac_strikes,
                  self.s_volatility_pac: pac_vols,
                  self.s_extrapolated_pac: pac_extrapolated})

    def __get_md_atmf_rr_fly(self) -> pd.DataFrame:

        # for ATM, RR, FLY: RR & FLY are ordered in descending order of negative put deltas
        i_atm = self.target_npdeltas.size // 2  # 3 -> 1, 5 -> 2

        vol = self.df_md_pac[self.s_volatility_pac].to_numpy()
        ext = self.df_md_pac[self.s_extrapolated_pac].to_numpy()

        vol_atm = vol[:, i_atm]
        vol_rr = vol - vol[:, ::-1]
        vol_fly = 0.5*(vol + vol[:, ::-1]) - vol_atm[:, np.newaxis]  # newaxis to broadcast

        ext_atm = ext[:, i_atm]
        ext_rr = ext | ext[:, ::-1]
        ext_fly = ext_rr | ext_atm[:, np.newaxis]

        col_arf, vol_arf, ext_arf = [self.s_ATMF], [vol_atm], [ext_atm]
        for i in range(i_atm):
            idx = i_atm + i + 1
            vol_arf.extend([vol_rr[:, idx], vol_fly[:, idx]])
            ext_arf.extend([ext_rr[:, idx], ext_fly[:, idx]])
            delta_str = self.f2str100(1.0 - self.target_npdeltas[idx])
            col_arf.extend([delta_str + self.s_RR, delta_str + self.s_FLY])

        df_vol_arf = pd.DataFrame(data=np.array(vol_arf).T, index=self.df_md_pac.index, columns=col_arf)
        df_ext_arf = pd.DataFrame(data=np.array(ext_arf).T, index=self.df_md_pac.index, columns=col_arf)

        df_md_arf = pd.concat({self.s_volatility_arf: df_vol_arf, self.s_extrapolated_arf: df_ext_arf}, axis=1)

        return df_md_arf
