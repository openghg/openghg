import re
from pathlib import Path
import logging
import pandas as pd
from pandas import DateOffset, Timedelta, Timestamp
from xarray import DataArray, Dataset

from openghg.types import TimePeriod, optionalPathType

logger = logging.getLogger("openghg.store")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler

# TupleTimeType = Tuple[Union[int, float, None], Union[str, None]]

__all__ = ["infer_date_range", "update_zero_dim"]


def infer_date_range(
    time: DataArray,
    filepath: optionalPathType = None,
    period: str | tuple | None = None,
    continuous: bool = True,
) -> tuple[Timestamp, Timestamp, str]:
    """
    Infer the date range from the time dimension.

    If the time dimension only includes one value the date range will be:
     - derived from the period (if supplied)
     - derived from the filepath using pattern matching (if supplied)
       - 4 digits assumed to be a year
       - 6 digits assumed to be a month
     - assumed to be yearly

     If the time dimension includes multiple values the date range can be
     inferred from this directly. The period will be derived as follows:
     - if continuous=True, assumes data is meant to be regular and will
       infer period and date range from the data, raising an error if unable to do so.
     - if continuous=False
       - if period is not specified, will attempt to infer a period and if
         unable to do so will set this to "varies".
       - if period is specified, will use this value
          - will compare against the inferred period and log a warning

    Args:
        time: DataArray containing time values
        filepath: Full path to original netcdf file
        period: Period of measurements.
                If specified, should be one of:
                    - "yearly", "monthly"
                    - suitable pandas Offset Alias
                    - tuple of (value, unit) as would be passed to pandas.Timedelta function
        continuous: Whether time stamps have to be continuous.
    Returns:
        Timestamp, Timestamp, str: Derived start date, end date and period (containing the value and unit).
    """
    from openghg.util import create_frequency_str, parse_period, relative_time_offset, timestamp_tzaware

    if filepath is not None:
        filepath = Path(filepath)

    null_freq = TimePeriod()  # contains value=None, unit=None

    # Find frequency from period, if specified
    if period is not None:
        input_freq = parse_period(period)
    else:
        input_freq = null_freq

    # Changed this from len(time) as a length of a single value
    # DataArray was throwing an len() of unsized object error
    n_dates = time.size

    if n_dates == 1:
        try:
            timestamp = time.values[0]
        except IndexError:
            raise ValueError(
                "'time' coord has 0 dimensions. Please update this data to remove ambiguity."
                + "\nCan use openghg.store.update_zero_dim() to add this 'time' dimension to all variables"
            )
            # try:
            #     start_date = timestamp_tzaware(timestamp=time.values)
            # except ValueError:
            #     raise ValueError("Can't read date from dataset.")
        else:
            start_date = timestamp_tzaware(timestamp)

        if filepath is not None:
            filename = Path(filepath).stem
            filename_identifiers = filename.split("_")
            filename_identifiers.reverse()  # Date identifier usually at the end

            for identifier in filename_identifiers:
                string_match = re.search(r"^(\d{6}|\d{4})$", identifier)
                if string_match is not None:
                    date_match = string_match.group()
                    break
                else:
                    continue
            else:
                date_match = ""

            # Set as default as annual if unable to derive from filepath
            inferred_freq = TimePeriod(1, "years")

            if len(date_match) == 6:
                # "yyyymm" format indicates monthly data
                expected_date = f"{start_date.year}{start_date.month:02}"
                if date_match == expected_date:
                    inferred_freq = TimePeriod(1, "months")
            elif len(date_match) == 4:
                # "yyyy" format indicates yearly data
                expected_date = str(start_date.year)
                if date_match == expected_date:
                    inferred_freq = TimePeriod(1, "years")

        else:
            # Set as default as annual if filepath not supplied
            inferred_freq = TimePeriod(1, "years")

        # Because frequency cannot be inferred from the data and only the filename,
        # use the user specified input in preference of the inferred value
        if input_freq != null_freq:
            # time_value: Optional[Union[int, float]] = input_freq[0]
            # time_unit: Optional[str] = input_freq[1]
            time_value, time_unit = input_freq
        else:
            if inferred_freq != null_freq:
                logger.info(f"Only one time point, inferring frequency of {inferred_freq}")
                time_value, time_unit = inferred_freq

        # Check input period against inferred period
        if inferred_freq != input_freq and period is not None:
            logger.warning(
                f"Input period of {period} did not map to frequency inferred from filename: {inferred_freq} (date extracted: {date_match})"
            )

        # Create time offset and use to create start and end datetime
        time_delta = relative_time_offset(unit=time_unit, value=time_value)
        start_date = timestamp_tzaware(time.values[0])
        end_date = start_date + time_delta - Timedelta(seconds=1)

        period_str = create_frequency_str(time_value, time_unit)

    else:
        # Here we trim the timestamps to millisecond precision to reduce the likelihood of
        # floating point errors result in ns differences in period
        timestamps = pd.to_datetime(time.values.astype("datetime64[ms]"), utc=True)
        timestamps = timestamps.sort_values()

        inferred_period = pd.infer_freq(timestamps)
        if inferred_period is None:
            if continuous:
                raise ValueError(
                    "Continuous data with no gaps is expected but no time period can be inferred. Run with continuous=False (and optionally specify period input) to remove this constraint."
                )
            else:
                inferred_freq = null_freq
        else:
            inferred_freq = parse_period(inferred_period)

        # Because frequency will be inferred from the data, use the inferred
        # value in preference to any user specified input.
        # Note: this is opposite to the other part of this branch.
        if inferred_freq != null_freq:
            if input_freq != null_freq and input_freq != inferred_freq:
                logger.warning(f"Input period: {period} does not map to inferred frequency {inferred_freq}")
            time_value, time_unit = inferred_freq
        else:
            time_value, time_unit = input_freq

        # Create time offset, using inferred offset
        start_date = timestamp_tzaware(time[0].values)
        if time_value is not None:
            time_delta = DateOffset(**{time_unit: time_value})
            end_date = timestamp_tzaware(time[-1].values) + time_delta - Timedelta(seconds=1)
        else:
            end_date = timestamp_tzaware(time[-1].values)

        if time_value is not None:
            period_str = create_frequency_str(time_value, time_unit)
        else:
            period_str = "varies"

    return start_date, end_date, period_str


def update_zero_dim(ds: Dataset, dim: str = "time") -> Dataset:
    """
    Check whether a dimension within an xarray Dataset object is 0-size
    (0 dimension) and update time to 1-size (1 dimension) if so.

    Args:
        ds: Input Dataset
        dim: name of dimension to check

    Returns:
        ds: Dataset, updated if needed.
    """

    da = ds[dim]
    if not da.dims:
        ds = ds.expand_dims(dim={dim: 1})

    return ds
