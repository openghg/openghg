from typing import cast

import numpy as np
import pandas as pd
import xarray as xr

from openghg.analyse._alignment import reindex_time_to_target
from openghg.analyse._fp_x_flux import (
    _forward_fill_flux_hourly,
    _hourly_lags,
    _validate_flux_coverage,
    _validate_interval_start_time,
    _validate_monthly_flux_coverage,
)
from openghg.analyse._utils import reindex_on_dims


def fp_x_flux_integrated(
    footprint: xr.Dataset, flux: xr.Dataset, allow_climatology: bool = True
) -> xr.DataArray:
    """Calculate footprint times flux.

    Args:
        footprint: footprint data; should have `fp` data variable.
        flux: flux data; should have `flux` data variable.
        allow_climatology: when aligning flux data to footprint data, if one year is present and outside the target
            date range of the footprint data, allow the flux data to be treated as a climatology and reindexed based on this.

    Returns:
        DataArray containing footprint times flux.

    """
    flux = reindex_on_dims(flux, footprint, ["lat", "lon"])
    # align separately on time
    # TODO: if method="nearest" was acceptable, then we could align all coordinates at once with reindex_like
    flux = reindex_time_to_target(flux, footprint, allow_climatology=allow_climatology)

    # align chunks for time after filling
    fp_time_chunks = footprint.fp.chunksizes.get("time")
    if fp_time_chunks is not None:
        fp_time_chunk = fp_time_chunks[0]
        flux = flux.chunk({"time": fp_time_chunk})

    result = footprint.fp.pint.quantify() * flux.flux.pint.quantify()
    return cast(xr.DataArray, result.pint.dequantify())


def make_integrated_low_freq_flux(flux: xr.DataArray | xr.Dataset) -> xr.Dataset:
    """Create monthly-mean flux for integrated footprint calculations."""
    flux_var = flux.flux if isinstance(flux, xr.Dataset) else flux
    flux_low_freq = flux_var.resample({"time": "1MS"}).mean()
    return flux_low_freq.to_dataset(name="flux")


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


def _padded_flux_slice_start_and_end(fp: xr.DataArray | xr.Dataset) -> tuple[np.datetime64, np.datetime64]:
    """Start and end of full range of dates to select from the flux input."""
    times = fp.time  # release times of particles
    max_h_back = _max_h_back(fp)

    start = times.values.min() - np.timedelta64(max_h_back, "h")
    end = times.values.max() + np.timedelta64(1, "s")

    return start, end


def _make_low_freq_flux(flux: xr.DataArray, fp: xr.DataArray | xr.Dataset) -> xr.DataArray:
    start, end = _padded_flux_slice_start_and_end(fp)
    # Monthly means are labelled at month start, including for mid-month releases.
    start = pd.Timestamp(start).to_period("M").start_time

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
    flux_hf_rolling = flux_high_freq.rolling(time=max_h_back).construct("H_back")

    # Hourly lag offsets select values, rather than specifying bin widths.
    h_back_type = fp_high_time_res.H_back.dtype
    flux_hf_rolling = flux_hf_rolling.assign_coords(
        {"H_back": np.arange(max_h_back, dtype=h_back_type)[::-1]}
    )
    flux_hf_rolling.H_back.attrs["units"] = fp_high_time_res.H_back.attrs.get("units")

    # select subsequence of H_back times to match high res fp (i.e. fp without max H_back coord)
    flux_hf_rolling = flux_hf_rolling.sel(H_back=fp_high_time_res.H_back)

    return flux_hf_rolling


def _make_high_freq_flux(
    flux: xr.DataArray,
    fp: xr.DataArray | xr.Dataset,
) -> xr.DataArray:
    start, end = _padded_flux_slice_start_and_end(fp)
    flux_res_hours = _validate_interval_start_time(flux.time, label="Flux")

    # Retain the native interval containing the earliest lag target.
    flux_high_freq = flux.sel(time=slice(start - np.timedelta64(flux_res_hours, "h"), end))
    flux_high_freq = _forward_fill_flux_hourly(flux_high_freq, step_hours=flux_res_hours)
    flux_high_freq = _make_hf_flux_rolling_avg_array(flux_high_freq, fp)

    # Hourly windows follow the native flux phase, including sub-hour offsets.
    flux_high_freq = flux_high_freq.reindex(
        {"time": fp.time}, method="ffill", tolerance=pd.Timedelta(hours=1)
    )

    flux_high_freq.attrs["units"] = flux.attrs.get("units")

    return flux_high_freq


# time-resolved calculation
def fp_x_flux_time_resolved(
    fp: xr.DataArray | xr.Dataset, flux: xr.DataArray | xr.Dataset, averaging: str | None = None
) -> xr.DataArray:
    if fp.sizes.get("time") == 0:
        raise ValueError("Footprint time coordinate must not be empty.")
    if flux.sizes.get("time") == 0:
        raise ValueError("Flux time coordinate must not be empty.")

    if isinstance(flux, xr.Dataset):
        flux = flux.flux
        flux = cast(xr.DataArray, flux)

    # convert old fp from `fp_HiTRes` data variable to dataset with `fp_time_resolved` and
    # `fp_residual` data variables
    if isinstance(fp, xr.DataArray):
        fp = time_resolved_and_residual_footprints(fp)

    # Missing physical declarations follow the canonical footprint and flux units.
    fp = fp.assign(
        {
            name: fp[name].assign_attrs(units="m2 s mol-1")
            for name in ("fp_time_resolved", "fp_residual")
            if "units" not in fp[name].attrs
        }
    )
    if "units" not in flux.attrs:
        flux = flux.assign_attrs(units="mol m-2 s-1")

    flux = reindex_on_dims(flux, reindex_like=fp, dims=["lat", "lon"])

    # Make sure any NaN values are set to zero as this is a multiplicative and summing operation
    fp = fp.fillna(0.0)
    flux = flux.fillna(0.0)

    # apply averaging to footprints
    if averaging is not None:
        # TODO: could check if resampling is necessary
        # TODO: we might also want to change how we resample, depending on whether we're upsampling or downsampling
        fp = fp.resample(time=averaging).ffill()

    # explicit variables to make mypy happy...
    fp_time_resolved: xr.DataArray = fp.fp_time_resolved
    fp_residual: xr.DataArray = fp.fp_residual

    # Validate native intervals and the full lag halo before either calculation branch.
    _hourly_lags(fp_time_resolved)
    flux_res_hours = _validate_interval_start_time(flux.time, label="Flux")
    _validate_flux_coverage(flux, fp_time_resolved, step_hours=flux_res_hours)
    _validate_monthly_flux_coverage(flux, fp_time_resolved)

    # create low res. (monthly) flux and calculate low res. fp x flux
    flux_low_freq = _make_low_freq_flux(flux, fp)
    fp_x_flux_residual = fp_residual.pint.quantify() * flux_low_freq.pint.quantify()

    # if resolution coarser than "H_back" dimension, just sum over "H_back" and use low freq. flux
    if flux_res_hours > _max_h_back(fp):
        result = (
            fp_time_resolved.sum("H_back").pint.quantify() * flux_low_freq.pint.quantify()
            + fp_x_flux_residual
        )
        return cast(xr.DataArray, result.pint.dequantify())

    # Create hourly native-interval flux windows with the requested lag offsets.
    flux_high_freq = _make_high_freq_flux(flux, fp)

    fp_x_flux = (flux_high_freq.pint.quantify() * fp_time_resolved.pint.quantify()).sum(
        "H_back"
    ) + fp_x_flux_residual

    return cast(xr.DataArray, fp_x_flux.pint.dequantify())
