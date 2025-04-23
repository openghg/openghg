from math import gcd
from typing import cast

import numpy as np
import pandas as pd
import xarray as xr

from openghg.analyse._alignment import time_of_day_offset
from openghg.analyse._utils import match_dataset_dims, reindex_on_dims


def fp_x_flux_integrated(footprint: xr.Dataset, flux: xr.Dataset) -> xr.DataArray:
    """Calculate footprint times flux.

    Args:
        footprint: footprint data; should have `fp` data variable.
        flux: flux data; should have `flux` data variable.

    Returns:
        DataArray containing footprint times flux.

    """
    footprint, flux = match_dataset_dims([footprint, flux], dims=["lat", "lon"])

    # align separately on time
    # TODO: if method="nearest" was acceptable, then we could align all coordinates at once with reindex_like
    flux = flux.reindex_like(footprint, method="ffill")

    result = cast(xr.DataArray, footprint.fp * flux.flux)
    return result


# helper functions for time-resolved calculation
def calc_hourly_freq(times: xr.DataArray, dim: str = "time", input_nanoseconds: bool = False) -> int:
    """Infer frequency of DataArray of times.

    Set `input_nanoseconds` to True if the times are in terms of nanoseconds.
    Otherwise times are assumed to be in terms of hours.
    """
    nanosecond_to_hour = 1 / (1e9 * 60.0 * 60.0)

    if input_nanoseconds:
        return int(times.diff(dim=dim).values.mean() * nanosecond_to_hour)
    else:
        return int(times.diff(dim=dim).values.mean())


def time_resolved_and_residual_footprints(fp_high_time_res: xr.DataArray) -> xr.Dataset:
    """Extract time-resolved and residual footprints from older style 'high time resolution' footprints."""
    try:
        max_h_back = fp_high_time_res.H_back.max()
    except AttributeError as e:
        raise ValueError(
            "Footprint must have `H_back` dimension to extract time-resolved and residual components."
        ) from e

    fp_residual = fp_high_time_res.sel(H_back=max_h_back, drop=True)
    fp_time_resolved = fp_high_time_res.where(fp_high_time_res.H_back != max_h_back, drop=True)

    result = xr.Dataset({"fp_time_resolved": fp_time_resolved, "fp_residual": fp_residual})
    return result


def _max_h_back(fp: xr.DataArray | xr.Dataset) -> int:
    if "H_back" not in fp.dims:
        raise ValueError("Footprint does not have 'H_back' dimension.")

    max_h_back = int(fp.H_back.max().values)

    return max_h_back + 1  # add one because H_back used to go up to 24, now it goes up to 23


def _fp_time_and_h_back_freq_gcd(fp: xr.DataArray | xr.Dataset) -> int:
    # calculate footprint time resolution
    fp_res_time_hours = calc_hourly_freq(fp.time, input_nanoseconds=True)

    # Define resolution on high frequency dimension in number of hours
    # At the moment this is matched to the Hback dimension
    fp_res_h_back_hours = calc_hourly_freq(fp["H_back"], dim="H_back")

    # Only allow for high frequency resolution < 24 hours that divides 24 hours evenly.
    if fp_res_h_back_hours > 24:
        raise ValueError(f"High frequency resolution must be <= 24 hours. Current: {fp_res_h_back_hours}h")

    if 24 % fp_res_h_back_hours != 0 or 24 % fp_res_h_back_hours != 0.0:
        raise ValueError(
            f"High frequency resolution must exactly divide into 24 hours. Current: {fp_res_h_back_hours}h"
        )

    # Find the greatest common denominator between time and high frequency resolutions.
    # This is needed to make sure suitable flux frequency is used to allow for indexing.
    # e.g. time: 1H; hf (high frequency): 2H, highest_res_H would be 1H
    # e.g. time: 2H; hf (high frequency): 3H, highest_res_H would be 1H
    highest_res_h = gcd(fp_res_time_hours, fp_res_h_back_hours)

    return highest_res_h


def _padded_flux_slice_start_and_end(fp: xr.DataArray | xr.Dataset) -> tuple[np.datetime64, np.datetime64]:
    """Start and end of full range of dates to select from the flux input."""
    times = fp.time  # release times of particles
    max_h_back = _max_h_back(fp)

    start = times.values[0] - np.timedelta64(max_h_back, "h")
    end = times.values[-1] + np.timedelta64(1, "s")

    return start, end


def _make_low_freq_flux(flux: xr.DataArray, fp: xr.DataArray | xr.Dataset) -> xr.DataArray:
    start, end = _padded_flux_slice_start_and_end(fp)

    flux_low_freq = (
        flux.resample({"time": "1MS"})
        .mean()
        .sel(time=slice(start, end))
        .transpose("lat", "lon", "time")
        .reindex_like(fp, method="ffill")
    )

    return flux_low_freq


def _make_hf_flux_rolling_avg_array(
    flux_high_freq: xr.DataArray,
    fp_high_time_res: xr.DataArray | xr.Dataset,
) -> xr.DataArray:
    # create windows (backwards in time) with `max_h_back` many time points,
    # starting at each time point in flux_hf_rolling.time
    max_h_back = _max_h_back(fp_high_time_res)
    highest_res_h = _fp_time_and_h_back_freq_gcd(fp_high_time_res)
    window_size = max_h_back // highest_res_h
    flux_hf_rolling = flux_high_freq.rolling(time=window_size).construct("H_back")

    # set H_back coordinates using highest_res_H frequency
    h_back_type = fp_high_time_res.H_back.dtype
    flux_hf_rolling = flux_hf_rolling.assign_coords(
        {"H_back": np.arange(0, max_h_back, highest_res_h, dtype=h_back_type)[::-1]}
    )

    # select subsequence of H_back times to match high res fp (i.e. fp without max H_back coord)
    flux_hf_rolling = flux_hf_rolling.sel(H_back=fp_high_time_res.H_back)

    return flux_hf_rolling


def _make_high_freq_flux(flux: xr.DataArray, fp: xr.DataArray | xr.Dataset) -> xr.DataArray:
    fp_highest_res_hours = _fp_time_and_h_back_freq_gcd(fp)
    start, end = _padded_flux_slice_start_and_end(fp)
    offset = time_of_day_offset(start)

    # select values with start time padded to include first rolling window backwards in time
    flux_high_freq = flux.sel(time=slice(start, end))

    # resample flux
    freq = f"{fp_highest_res_hours}h"
    flux_res_hours = calc_hourly_freq(flux.time, input_nanoseconds=True)
    flux_resampler = flux_high_freq.resample({"time": freq}, offset=offset)

    if flux_res_hours <= fp_highest_res_hours:
        # downsampling
        flux_high_freq = flux_resampler.mean()
    else:
        # upsampling
        flux_high_freq = flux_resampler.ffill()

    # reindex to align
    full_dates = pd.date_range(start, end, freq=freq, inclusive="left").to_numpy()
    flux_high_freq = flux_high_freq.reindex({"time": full_dates}, method="ffill")

    # create rolling windows
    flux_high_freq = _make_hf_flux_rolling_avg_array(flux_high_freq, fp)

    return flux_high_freq


# time-resolved calculation
def fp_x_flux_time_resolved(
    fp: xr.DataArray | xr.Dataset, flux: xr.DataArray | xr.Dataset, averaging: str | None = None
) -> xr.DataArray:

    if isinstance(flux, xr.Dataset):
        flux = flux.flux
        flux = cast(xr.DataArray, flux)

    # convert old fp from `fp_HiTRes` data variable to dataset with `fp_time_resolved` and
    # `fp_residual` data variables
    if isinstance(fp, xr.DataArray):
        fp = time_resolved_and_residual_footprints(fp)

    flux = reindex_on_dims(flux, reindex_like=fp, dims=["lat", "lon"])

    # Make sure any NaN values are set to zero as this is a multiplicative and summing operation
    fp = fp.fillna(0.0)
    flux = flux.fillna(0.0)

    # apply averaging to footprints
    if averaging is not None:
        # TODO: could check if resampling is necessary
        # TODO: we might also want to change how we resample, depending on whether we're upsampling or downsampling
        fp = fp.resample(time=averaging).ffill()

    # create low res. (monthly) flux and calculate low res. fp x flux
    flux_low_freq = _make_low_freq_flux(flux, fp)
    fp_x_flux_residual = fp.fp_residual * flux_low_freq

    # Calculate time resolution for flux
    flux_res_hours = calc_hourly_freq(flux.time, input_nanoseconds=True)

    # if resolution coarser than "H_back" dimension, just sum over "H_back" and use low freq. flux
    if flux_res_hours > _max_h_back(fp):
        return fp.fp_time_resolved.sum("H_back") * flux_low_freq + fp_x_flux_residual

    # create high frequency flux (resampled to gcd of footprint time and H_back frequencies) with H_back dim
    flux_high_freq = _make_high_freq_flux(flux, fp)

    fp_x_flux = (flux_high_freq * fp.fp_time_resolved).sum("H_back") + fp_x_flux_residual

    return fp_x_flux
