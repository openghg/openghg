import logging
from typing import cast, Literal

import numpy as np
import pandas as pd
from numpy.typing import ArrayLike

from openghg.types import XrDataLike, XrDataLikeMatch, XrDataLikeMatch2


logger = logging.getLogger("openghg.analyse")
logger.setLevel(logging.INFO)  # Have to set level for logger as well as handler


def extract_obs_freq(obs: XrDataLike) -> float | None:
    obs_attributes = obs.attrs

    if "averaged_period" in obs_attributes:
        return float(obs_attributes["averaged_period"])

    if "sampling_period" in obs_attributes:
        sampling_period = obs_attributes["sampling_period"]

        if sampling_period == "NOT_SET":
            return None

        if sampling_period == "multiple":
            # If we have a varying sampling_period, make sure we always resample to footprint
            return 1.0

        return float(sampling_period)

    if "sampling_period_estimate" in obs_attributes:
        estimate = obs_attributes["sampling_period_estimate"]
        logger.warning(f"Using estimated sampling period of {estimate}s for observational data")
        return float(estimate)

    return None


def infer_freq_in_seconds(times: ArrayLike, tol: float = 1.0) -> float:
    obs_data_period_s = np.nanmedian(np.diff(times) / 1e9).astype("float32")

    obs_data_period_s_min = np.diff(times).min() / 1e9
    obs_data_period_s_max = np.diff(times).max() / 1e9

    max_diff = (obs_data_period_s_max - obs_data_period_s_min).astype(float)

    # Check if the periods differ by more than 1 second
    if max_diff > tol:
        raise ValueError("Sample period can be not be derived from observations")

    return float(obs_data_period_s)


def time_overlap(
    times1: np.ndarray, times2: np.ndarray, freq1: pd.Timedelta, freq2: pd.Timedelta
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Find start and end dates where gives times overlap.

    Frequencies for each time array are used to pad the end dates.

    Args:
        times1: array of times
        times2: array of times
        freq1: frequency/period corresponding to times1
        freq2: frequency/period corresponding to times2

    Returns:
        tuple of start and end dates

    """

    startdate1 = pd.to_datetime(times1[0])
    startdate2 = pd.to_datetime(times2[0])

    # add frequencies to end dates
    enddate1 = pd.to_datetime(times1[-1]) + freq1
    enddate2 = pd.to_datetime(times2[-1]) + freq2

    start_date = max(startdate1, startdate2)
    end_date = min(enddate1, enddate2)

    return start_date, end_date


def tweak_start_and_end_dates(
    start_date: pd.Timestamp, end_date: pd.Timestamp, freq2: pd.Timedelta
) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    # Ensure lower range is covered for times1
    start1 = start_date - pd.Timedelta("1ns")

    # Ensure extra buffer is added for footprint based on fp timeperiod.
    # This is to ensure footprint can be forward-filled to obs (in later steps)
    start2 = start_date - (freq2 - pd.Timedelta("1ns"))

    # Subtract very small time increment (1 nanosecond) to make this an exclusive selection
    end = end_date - pd.Timedelta("1ns")

    return start1, start2, end


def align_obs_and_other(
    obs: XrDataLikeMatch,
    other: XrDataLikeMatch2,
    resample_to: Literal["obs", "other", "coarsest"] | str = "coarsest",
    platform: str | None = None,
) -> tuple[XrDataLikeMatch, XrDataLikeMatch2]:
    """Slice and resample obs and footprint data to align along time.

    This slices the date to the smallest time frame spanned by both the footprint and obs,
    using the sliced start date.

    The time dimension is resampled based on the resample_to input using the mean.
    The resample_to options are:
     - "coarsest" - resample to the coarsest resolution between obs and footprints
     - "obs" - resample to observation data frequency
     - "footprint" - resample to footprint data frequency
     - a valid resample period e.g. "2H"

    Args:
        obs: obs data (either as xr.DataArray or xr.Dataset)
        other: other data to align with (either as xr.DataArray or xr.Dataset)
        resample_to: Resample option to use: either data based or using a valid pandas resample period.
        platform: Observation platform used to decide whether to resample

    Returns:
        tuple: Two xarray DataArrays or Datasets with aligned time dimensions. The types of the returned
          data match the types of the input data.
    """
    resample_keyword_choices = ("obs", "other", "coarsest")

    # Check whether resample has been requested by specifying a specific period rather than a keyword
    if resample_to in resample_keyword_choices:
        force_resample = False
    else:
        force_resample = True

    if platform is not None:
        platform = platform.lower()
        # Do not apply resampling for "satellite" (but have re-included "flask" for now)
        if platform == "satellite":
            return obs, other

    # try to get period/freq from attributes
    obs_data_period_s = extract_obs_freq(obs)

    # if None is returned, we need to try to infer the period/frequency
    if obs_data_period_s is None:
        obs_data_period_s = infer_freq_in_seconds(obs.time.values)
        estimate = f"{obs_data_period_s:.1f}"
        logger.warning(f"Sampling period was estimated (inferred) from data frequency: {estimate}s")
        obs.attrs["sampling_period_estimate"] = estimate

    # TODO: Check regularity of the data - will need this to decide is resampling
    # is appropriate or need to do checks on a per time point basis

    obs_data_period_ns = int(obs_data_period_s * 1e9)
    obs_data_timeperiod = pd.Timedelta(obs_data_period_ns, unit="ns")

    # Derive the footprints period from the frequency of the data
    other_data_period_ns = np.nanmedian((other.time.data[1:] - other.time.data[0:-1]).astype("int64"))
    other_data_timeperiod = pd.Timedelta(other_data_period_ns, unit="ns")

    # If resample_to is set to "coarsest", check whether "obs" or "footprint" have lower resolution
    if resample_to == "coarsest":
        if obs_data_timeperiod >= other_data_timeperiod:
            resample_to = "obs"
        elif obs_data_timeperiod < other_data_timeperiod:
            resample_to = "footprint"

    # get common start and end dates, accounting for periods in the end dates
    start_date, end_date = time_overlap(
        obs.time.values, other.time.values, obs_data_timeperiod, other_data_timeperiod
    )

    # tweak to make end date exclusive, obs start inclusive, and other start time nearly one full period earlier
    start_obs_slice, start_other_slice, end_slice = tweak_start_and_end_dates(
        start_date, end_date, other_data_timeperiod
    )

    obs = obs.sel(time=slice(start_obs_slice, end_slice))
    other = other.sel(time=slice(start_other_slice, end_slice))

    if obs.time.size == 0 or other.time.size == 0:
        raise ValueError("Obs data and Footprint data don't overlap")

    # Only non satellite datasets with different periods need to be resampled
    timeperiod_diff_s = np.abs(obs_data_timeperiod - other_data_timeperiod).total_seconds()
    tolerance = 1e-9  # seconds

    if timeperiod_diff_s >= tolerance or force_resample:
        offset = pd.Timedelta(hours=start_date.hour + start_date.minute / 60.0 + start_date.second / 3600.0)
        offset = cast(pd.Timedelta, offset)

        if resample_to == "obs":
            resample_period = str(round(obs_data_timeperiod / np.timedelta64(1, "h"), 5)) + "H"
            other = other.resample(indexer={"time": resample_period}, offset=offset).mean()

        elif resample_to == "footprint":
            resample_period = str(round(other_data_timeperiod / np.timedelta64(1, "h"), 5)) + "H"
            obs = obs.resample(indexer={"time": resample_period}, offset=offset).mean()

        else:
            resample_period = resample_to
            other = other.resample(indexer={"time": resample_period}, offset=offset).mean()
            obs = obs.resample(indexer={"time": resample_period}, offset=offset).mean()

    return obs, other
