import logging
from typing import Literal, TypeVar

import numpy as np
import pandas as pd
import xarray as xr
from numpy.typing import ArrayLike

from openghg.types import ReindexMethod, XrDataLike


logger = logging.getLogger("openghg.analyse")
logger.setLevel(logging.INFO)  # Have to set level for logger as well as handler


def _extract_obs_freq(obs: XrDataLike) -> float | None:
    """Find the observation sampling period based on the associated attributes.

    Args:
        obs: Observation data. Expect attributes to be included which signal the averaging or sampling period.

    Returns:
        float | None: Sampling period value or None if no value is found.

    """
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
    """Infer the frequency of a sequence of floats representing time in nanoseconds.

    Args:
        times: times in nanoseconds
        tol: tolerance for difference between min. and max. gaps between time
          points.

    Returns:
        float representing inferred (median) frequency in seconds.

    Raises:
        ValueError: if the difference between the min. and max. gaps in the data
          exceeds the specified tolerance.

    """
    obs_data_period_s = np.nanmedian(np.diff(times) / 1e9).astype("float32")

    obs_data_period_s_min = np.diff(times).min() / 1e9
    obs_data_period_s_max = np.diff(times).max() / 1e9

    max_diff = (obs_data_period_s_max - obs_data_period_s_min).astype(float)

    # Check if the periods differ by more than 1 second
    if max_diff > tol:
        raise ValueError("Sample period cannot be derived from observations")

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


def _buffer_start_and_end_dates(
    start_date: pd.Timestamp, end_date: pd.Timestamp, freq2: pd.Timedelta
) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    """Adjust start and end dates, and create a second start time including an extra period.

    This is used when `start_date` and `end_date` are calculated by `time_overlap`. Suppose
    the input times to `time_overlap` are `times1` and `times2`.
    Then the values returned, start1, start2, and end, have the property that:
    - selecting slice(start1, end) from `times1` is inclusive of `start_date` and
      exclusive of `end_date`.
    - selecting slice(start2, end) from `times2` will give a value before (or on) `start_date`

    Args:
        start_date: start date
        end_date: end date
        freq2: frequency/period to shift second start time back by

    Returns:
        tuple of times (start1, start2, end), where start2 is shifted back in time by freq2.

    """
    # Ensure lower range is covered for times1
    start1 = start_date - pd.Timedelta("1ns")

    # Ensure extra buffer is added for footprint based on fp timeperiod.
    # This is to ensure footprint can be forward-filled to obs (in later steps)
    start2 = start_date - (freq2 - pd.Timedelta("1ns"))

    # Subtract very small time increment (1 nanosecond) to make this an exclusive selection
    end = end_date - pd.Timedelta("1ns")

    return start1, start2, end


def timedelta_to_hourly_freq(td: pd.Timedelta) -> str:
    """Convert a Timedelta into a frequency string in units of hours."""
    return str(round(td / pd.to_timedelta(1, "h"), 5)) + "H"


T1 = TypeVar("T1", xr.DataArray, xr.Dataset)
T2 = TypeVar("T2", xr.DataArray, xr.Dataset)


def resample_obs_and_other(
    obs: T1,
    other: T2,
    resample_to: Literal["obs", "other", "coarsest"] | str = "coarsest",
) -> tuple[T1, T2]:
    """Slice and resample obs and other data to align along time.

    This slices the date to the smallest time frame spanned by both the other and obs data,
    using the sliced start date.

    The time dimension is resampled based on the resample_to input
    using the mean.

    The resample_to options are:
     - "coarsest" - resample to the coarsest resolution between obs and footprints
     - "obs" - resample to observation data frequency
     - "other" - resample to footprint data frequency
     - a valid resample period e.g. "2H"

    Args:
        obs: obs data (either as xr.DataArray or xr.Dataset)
        other: other data to align with (either as xr.DataArray or xr.Dataset). This is usually
          footprint data.
        resample_to: Resample option to use: either data based or using a valid pandas resample period.

    Returns:
        tuple: Two xarray DataArrays or Datasets with aligned time dimensions. The types of the returned
          data match the types of the input data.
    """
    # try to get period/freq from attributes
    obs_data_period_s = _extract_obs_freq(obs)

    # if None is returned, we need to try to infer the period/frequency
    if obs_data_period_s is None:
        obs_data_period_s = infer_freq_in_seconds(obs.time.values)
        estimate = f"{obs_data_period_s:.1f}"
        logger.warning(f"Sampling period was estimated (inferred) from data frequency: {estimate}s")
        obs.attrs["sampling_period_estimate"] = estimate

    # TODO: Check regularity of the data - will need this to decide is resampling
    # is appropriate or need to do checks on a per time point basis

    obs_data_timeperiod = pd.to_timedelta(obs_data_period_s, unit="s")

    # Derive the other data's period from the frequency of the data
    other_data_period_ns = int(np.nanmedian(np.diff(other.time.values)))
    other_data_timeperiod = pd.to_timedelta(other_data_period_ns, unit="ns")

    # get common start and end dates, accounting for periods in the end dates
    start_date, end_date = time_overlap(
        obs.time.values, other.time.values, obs_data_timeperiod, other_data_timeperiod
    )

    # tweak to make end date exclusive, obs start inclusive, and other start time nearly one full period earlier
    start_obs_slice, start_other_slice, end_slice = _buffer_start_and_end_dates(
        start_date, end_date, other_data_timeperiod
    )

    obs = obs.sel(time=slice(start_obs_slice, end_slice))
    other = other.sel(time=slice(start_other_slice, end_slice))

    if obs.time.size == 0 or other.time.size == 0:
        raise ValueError("Obs data and Footprint data don't overlap")

    # Offset for resampling
    offset = start_date - pd.to_datetime(start_date.date())  # time past start of day

    # If specific period has been passed, resample
    resample_keyword_choices = ("obs", "other", "coarsest")

    if resample_to not in resample_keyword_choices:
        obs = obs.resample(indexer={"time": resample_to}, offset=offset).mean()
        other = other.resample(indexer={"time": resample_to}, offset=offset).mean()

        return obs, other

    # Only resample if periods differ
    if obs_data_timeperiod != other_data_timeperiod:
        if resample_to == "coarsest":
            if obs_data_timeperiod >= other_data_timeperiod:
                resample_to = "obs"
            elif obs_data_timeperiod < other_data_timeperiod:
                resample_to = "other"

        if resample_to == "obs":
            resample_period = timedelta_to_hourly_freq(obs_data_timeperiod)
            other = other.resample(indexer={"time": resample_period}, offset=offset).mean()
        else:  # "other"
            resample_period = timedelta_to_hourly_freq(other_data_timeperiod)
            obs = obs.resample(indexer={"time": resample_period}, offset=offset).mean()

    return obs, other


def _indexes_match(dataset_A: xr.Dataset, dataset_B: xr.Dataset) -> bool:
    """Check if two datasets need to be reindexed_like for combine_datasets

    Args:
        dataset_A: First dataset to check
        dataset_B: Second dataset to check
    Returns:
        bool: True if indexes match, else False
    """
    common_indices = (key for key in dataset_A.indexes.keys() if key in dataset_B.indexes.keys())

    for index in common_indices:
        if not len(dataset_A.indexes[index]) == len(dataset_B.indexes[index]):
            return False

        # Check number of values that are not close (testing for equality with floating point)
        if index == "time":
            # For time override the default to have ~ second precision
            rtol = 1e-10
        else:
            rtol = 1e-5

        index_diff = np.sum(
            ~np.isclose(
                dataset_A.indexes[index].values.astype(float),
                dataset_B.indexes[index].values.astype(float),
                rtol=rtol,
            )
        )

        if not index_diff == 0:
            return False

    return True


def combine_datasets(
    dataset_A: xr.Dataset,
    dataset_B: xr.Dataset,
    method: ReindexMethod = "ffill",
    tolerance: float | None = None,
) -> xr.Dataset:
    """Merges two datasets and re-indexes to the first dataset.

    If "fp" variable is found within the combined dataset,
    the "time" values where the "lat", "lon" dimensions didn't match are removed.

    Args:
        dataset_A: First dataset to merge
        dataset_B: Second dataset to merge
        method: One of None, nearest, ffill, bfill.
                See xarray.DataArray.reindex_like for list of options and meaning.
                Defaults to ffill (forward fill)
        tolerance: Maximum allowed tolerance between matches.

    Returns:
        xarray.Dataset: Combined dataset indexed to dataset_A
    """
    if _indexes_match(dataset_A, dataset_B):
        dataset_B_temp = dataset_B
    else:
        # load dataset_B to avoid performance issue (see xarray issue #8945)
        dataset_B_temp = dataset_B.load().reindex_like(dataset_A, method=method, tolerance=tolerance)

    merged_ds = dataset_A.merge(dataset_B_temp)

    if "fp" in merged_ds:
        if all(k in merged_ds.fp.dims for k in ("lat", "lon")):
            flag = np.isfinite(merged_ds.fp.mean(["lat", "lon"]))
            merged_ds = merged_ds.where(flag.compute(), drop=True)

    return merged_ds
