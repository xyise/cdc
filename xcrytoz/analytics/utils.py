import numpy as np
from scipy import interpolate


def linear_interp_flat_extrap(x: np.ndarray, y: np.ndarray, extrapolate=True):

    i_s = np.argsort(x)
    x_s, y_s = x[i_s], y[i_s]

    fill_value = (y_s[0], y_s[-1]) if extrapolate else (np.nan, np.nan) 

    return interpolate.interp1d(x_s, y_s, bounds_error=False, 
        fill_value=fill_value, assume_sorted=True)

