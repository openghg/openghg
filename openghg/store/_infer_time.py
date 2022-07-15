from typing import Optional, Union, Tuple
from pathlib import Path
import re
import pandas as pd
from pandas import DateOffset, Timedelta, Timestamp
from xarray import DataArray
from openghg.util import (
    timestamp_tzaware,
    parse_period,
    create_frequency_str,
    relative_time_offset,
)

TupleTimeType = Tuple[Union[int, float], str]


def infer_date_range(
    time: DataArray,
    filepath: Optional[Union[str, Path]] = None,
    period: Optional[Union[str, tuple]] = None,
    continuous: bool = True,
) -> Tuple[Timestamp, Timestamp, str]:
    """
    Infer the date range from the time dimension. If the time dimension
    only includes one value the date range will be:
     - derived from the period (if supplied)
     - derived from the filepath using pattern matching (if supplied)
       - 4 digits assumed to be a year
       - 6 digits assumed to be a month
     - assumed to be yearly

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

    if filepath is not None:
        filepath = Path(filepath)

    # Find frequency from period, if specified
    if period is not None:
        freq: Optional[TupleTimeType] = parse_period(period)
    else:
        freq = None

    # Changed this from len(time) as a length of a single value
    # DataArray was throwing an len() of unsized object error
    n_dates = time.size

    if n_dates == 1:
        try:
            start_date = timestamp_tzaware(timestamp=time.values[0])
        except IndexError:
            raise NotImplementedError(
                "This type of BC file is not currently supported. Please see issue #349"
            )
            # try:
            #     start_date = timestamp_tzaware(timestamp=time.values)
            # except ValueError:
            #     raise ValueError("Can't read date from dataset.")

        if filepath is not None:
            filename = filepath.stem  # Filename without the extension
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
            inferred_freq: Optional[TupleTimeType] = (1, "years")

            if len(date_match) == 6:
                # "yyyymm" format indicates monthly data
                expected_date = f"{start_date.year}{start_date.month:02}"
                if date_match == expected_date:
                    inferred_freq = (1, "months")
            elif len(date_match) == 4:
                # "yyyy" format indicates yearly data
                expected_date = str(start_date.year)
                if date_match == expected_date:
                    inferred_freq = (1, "years")

        else:
            # Set as default as annual if filepath not supplied
            inferred_freq = (1, "years")

        # Because frequency cannot be inferred from the data and only the filename,
        # use the user specified input in preference of the inferred value
        if freq is not None:
            time_value: Optional[Union[int, float]] = freq[0]
            time_unit: Optional[str] = freq[1]
        else:
            if inferred_freq is not None:
                print(f"Only one time point, inferring frequency of {inferred_freq}")
                time_value, time_unit = inferred_freq

        # Check input period against inferred period
        if inferred_freq != freq:
            print(
                f"Warning: Input period of {period} did not map to frequency inferred from filename: {inferred_freq} (date extracted: {date_match})"
            )

        # Create time offset and use to create start and end datetime
        time_delta = relative_time_offset(unit=time_unit, value=time_value)
        start_date = timestamp_tzaware(time.values[0])
        end_date = start_date + time_delta - Timedelta(seconds=1)

        period_str = create_frequency_str(time_value, time_unit)

    else:
        timestamps = pd.to_datetime([timestamp_tzaware(t) for t in time.values])
        timestamps = timestamps.sort_values()

        inferred_period = pd.infer_freq(timestamps)
        if inferred_period is None:
            if continuous:
                raise ValueError(
                    "Continuous data with no gaps is expected but no time period can be inferred. Run with continous=False to remove this constraint."
                )
            else:
                inferred_freq = None
                time_value, time_unit = None, None
        else:
            inferred_freq = parse_period(inferred_period)
            time_value, time_unit = inferred_freq

        # Because frequency will be inferred from the data, use the inferred
        # value in preference to any user specified input.
        # Note: this is opposite to the other part of this branch.
        if freq is not None and inferred_freq is not None and freq != inferred_freq:
            print(f"Warning: Input period: {period} does not map to inferred frequency {inferred_freq}")
            freq = inferred_freq

        # Create time offset, using inferred offset
        start_date = timestamp_tzaware(time[0].values)
        if time_value is not None:
            time_delta = DateOffset(**{time_unit: time_value})
            end_date = timestamp_tzaware(time[-1].values) + time_delta - Timedelta(seconds=1)
        else:
            end_date = timestamp_tzaware(time[-1].values)

        if inferred_period is not None:
            period_str = create_frequency_str(time_value, time_unit)
        else:
            period_str = "varies"

    return start_date, end_date, period_str
