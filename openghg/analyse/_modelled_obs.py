from math import gcd
from typing import cast

import numpy as np
import pandas as pd
import xarray as xr

from openghg.analyse._utils import match_dataset_dims


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


def make_hf_flux_rolling_avg_array(
    flux_high_freq: xr.DataArray,
    fp_high_time_res: xr.DataArray,
    highest_res_H: int,
    max_h_back: int,
) -> xr.DataArray:
    # create windows (backwards in time) with `max_h_back` many time points,
    # starting at each time point in flux_hf_rolling.time
    window_size = max_h_back // highest_res_H
    flux_hf_rolling = flux_high_freq.rolling(time=window_size).construct("H_back")

    # set H_back coordinates using highest_res_H frequency
    h_back_type = fp_high_time_res.H_back.dtype
    flux_hf_rolling = flux_hf_rolling.assign_coords(
        {"H_back": np.arange(0, max_h_back, highest_res_H, dtype=h_back_type)[::-1]}
    )

    # select subsequence of H_back times to match high res fp (i.e. fp without max H_back coord)
    flux_hf_rolling = flux_hf_rolling.sel(H_back=fp_high_time_res.H_back)

    return flux_hf_rolling


def compute_fp_x_flux(
    fp_HiTRes: xr.DataArray,
    flux_high_freq: xr.DataArray,
    flux_low_freq: xr.DataArray,
    highest_res_H: int,
    max_h_back: int,
    flux_res_H: int,
) -> xr.DataArray:
    # do low res calculation
    fp_residual = fp_HiTRes.sel(H_back=fp_HiTRes.H_back.max(), drop=True)  # take last H_back value
    flux_low_freq = flux_low_freq.reindex_like(fp_residual, method="ffill")  # forward fill times

    fpXflux_residual = flux_low_freq * fp_residual

    # get high freq fp
    fp_high_freq = fp_HiTRes.where(fp_HiTRes.H_back != fp_HiTRes.H_back.max(), drop=True)

    # if flux_res_H > 24, then flux_high_freq = flux_low_freq, and we don't take a sum over windows of flux_high_freq
    if flux_res_H > 24:
        fpXflux = (flux_low_freq * fp_high_freq).sum("H_back")
    else:
        flux_high_freq = make_hf_flux_rolling_avg_array(
            flux_high_freq, fp_high_freq, highest_res_H, max_h_back
        )
        fpXflux = (flux_high_freq * fp_high_freq).sum("H_back")

    return fpXflux + fpXflux_residual


# time-resolved calculation
def fp_x_flux_time_resolved(
    fp_HiTRes: xr.DataArray, flux_ds: xr.Dataset, averaging: str | None = None
) -> xr.DataArray:

    fp_HiTRes, flux_ds = match_dataset_dims([fp_HiTRes, flux_ds], dims=["lat", "lon"])  # type: ignore
    fp_HiTRes = cast(xr.DataArray, fp_HiTRes)

    # Make sure any NaN values are set to zero as this is a multiplicative and summing operation
    fp_HiTRes = fp_HiTRes.fillna(0.0)
    flux_ds["flux"] = flux_ds["flux"].fillna(0.0)

    # Calculate time resolution for both the flux and footprints data
    flux_res_H = calc_hourly_freq(flux_ds.time, input_nanoseconds=True)
    fp_res_time_H = calc_hourly_freq(fp_HiTRes.time, input_nanoseconds=True)

    fp_res_Hback_H = calc_hourly_freq(fp_HiTRes["H_back"], dim="H_back")

    # Define resolution on time dimension in number in hours
    if averaging:
        try:
            time_res_H = int(averaging)
            time_resolution = f"{time_res_H}H"
        except (ValueError, TypeError):
            time_res_H = int(averaging[0])
            time_resolution = averaging
    else:
        # If not specified derive from time from combined dataset
        time_res_H = fp_res_time_H
        time_resolution = f"{time_res_H}H"

    # Resample fp timeseries to match time resolution
    if fp_res_time_H != time_res_H:
        fp_HiTRes = fp_HiTRes.resample(time=time_resolution).ffill()

    # Define resolution on high frequency dimension in number of hours
    # At the moment this is matched to the Hback dimension
    time_hf_res_H = fp_res_Hback_H

    # Only allow for high frequency resolution < 24 hours
    if time_hf_res_H > 24:
        raise ValueError(f"High frequency resolution must be <= 24 hours. Current: {time_hf_res_H}H")
    elif 24 % time_hf_res_H != 0 or 24 % time_hf_res_H != 0.0:
        raise ValueError(
            f"High frequency resolution must exactly divide into 24 hours. Current: {time_hf_res_H}H"
        )

    # Find the greatest common denominator between time and high frequency resolutions.
    # This is needed to make sure suitable flux frequency is used to allow for indexing.
    # e.g. time: 1H; hf (high frequency): 2H, highest_res_H would be 1H
    # e.g. time: 2H; hf (high frequency): 3H, highest_res_H would be 1H
    highest_res_H = gcd(time_res_H, time_hf_res_H)
    highest_resolution = f"{highest_res_H}H"

    # create time array to loop through, with the required resolution
    # fp_HiTRes.time is the release time of particles into the model
    time_array = fp_HiTRes["time"]

    # Define maximum hour back
    max_h_back = int(fp_HiTRes.H_back.max().values)

    # Define full range of dates to select from the flux input
    date_start = time_array[0]
    date_start_back = date_start - np.timedelta64(max_h_back, "h")
    date_end = time_array[-1] + np.timedelta64(1, "s")

    # Create times for matching to the flux
    full_dates = pd.date_range(
        date_start_back.values, date_end.values, freq=highest_resolution, inclusive="left"
    ).to_numpy()

    # Create low frequency flux data (monthly)
    flux_ds_low_freq = flux_ds.resample({"time": "1MS"}).mean().sel(time=slice(date_start_back, date_end))
    flux_ds_low_freq = flux_ds_low_freq.transpose(*("lat", "lon", "time"))

    # Select and align high frequency flux data
    flux_ds_high_freq = flux_ds.sel(time=slice(date_start_back, date_end))
    if flux_res_H <= 24:
        offset = pd.Timedelta(
            hours=date_start_back.dt.hour.data
            + date_start_back.dt.minute.data / 60.0
            + date_start_back.dt.second.data / 3600.0
        )
        offset = cast(pd.Timedelta, offset)
        if flux_res_H <= highest_res_H:
            # Downsample flux to match to footprints frequency
            flux_ds_high_freq = flux_ds_high_freq.resample({"time": highest_resolution}, offset=offset).mean()
        elif flux_res_H > highest_res_H:
            # Upsample flux to match footprints frequency and forward fill
            flux_ds_high_freq = flux_ds_high_freq.resample(
                {"time": highest_resolution}, offset=offset
            ).ffill()
        # Reindex to match to correct values
        flux_ds_high_freq = flux_ds_high_freq.reindex({"time": full_dates}, method="ffill")
    elif flux_res_H > 24:
        # TODO this case should be handled outside of the "compute_fp_x_flux" function
        # If flux is not high frequency use the monthly averages instead.
        flux_ds_high_freq = flux_ds_low_freq

    # TODO: Add check to make sure time values are exactly aligned based on date range

    # Extract flux data from dataset
    flux_high_freq = flux_ds_high_freq.flux
    flux_low_freq = flux_ds_low_freq.flux

    fpXflux = compute_fp_x_flux(
        fp_HiTRes,
        flux_high_freq,
        flux_low_freq,
        highest_res_H,
        max_h_back,
        flux_res_H,
    )

    return fpXflux
