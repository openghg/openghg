"""
This is used as a base for the other dataclasses and shouldn't be used directly.
"""

from itertools import islice
import logging

from pandas import Timestamp, Timedelta
import xarray as xr

from openghg.objectstore import get_datasource


logger = logging.getLogger("openghg.dataobjects")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def truncated_dict_repr(d: dict, length: int = 4) -> str:
    if len(d) <= length:
        return repr(d)

    trun_d = dict(islice(d.items(), length))
    result = repr(trun_d)

    # add "..." to indicate truncation
    result = result[:-1] + ", ...}"

    return result


class _BaseData:
    def __init__(
        self,
        metadata: dict,
        data: xr.Dataset | None = None,
        uuid: str | None = None,
        version: str | None = None,
        start_date: str | Timestamp | None = None,
        end_date: str | Timestamp | None = None,
        sort: bool = True,
        elevate_inlet: bool = False,
        attrs_to_check: dict | None = None,
    ) -> None:
        """
        This handles data for each of the data type classes. It accepts either a Dataset
        or a UUID and version to lazy load a Dataset from a zarr store. If dates are passed then the
        dataset is sliced to the requested time period.

        Args:
            metadata: Dictionary of metadata
            data: Dataset if data is already loaded
            uuid: UUID of Datasource to retrieve data from
            version: Version of data requested from Datasource
            start_date: Start date of data to retrieve
            end_date: End date of data to retrieve
            sort: Sort the resulting Dataset by the time dimension, defaults to True
            elevate_inlet: Force the elevation of the inlet attribute
            attrs_to_check: Attributes to check for duplicates. If duplicates are present
                a new data variable will be created containing the values from each dataset
                If a dictionary is passed, the attribute(s) will be retained and the new value assigned.
                If a list/string is passed, the attribute(s) will be removed.
        """
        from openghg.util import timestamp_epoch, timestamp_now

        if data is None and uuid is None and version is None:
            raise ValueError("Must supply either data or uuid and version")

        self.metadata = metadata
        self._uuid = uuid

        self._data_type = self.metadata.get("data_type")

        self._start_date = start_date
        self._end_date = end_date

        if elevate_inlet:
            raise NotImplementedError("elevate_inlet not implemented yet")

        if attrs_to_check is not None:
            raise NotImplementedError("attrs_to_check not implemented yet")

        sorted = False  # Check so we don't sort more than once

        if data is not None:
            self.data = data
        elif uuid is not None and version is not None:
            slice_time = False
            if start_date is not None or end_date is not None:
                slice_time = True
                if start_date is None:
                    start_date = timestamp_epoch()
                if end_date is None:
                    end_date = timestamp_now()

            self._version = version
            self._bucket = metadata["object_store"]

            datasource = get_datasource(bucket=self._bucket, uuid=uuid, data_type=self._data_type)

            version = version or "latest"  # can't pass version=None to Datasource.get_data
            self.data = datasource.get_data(version=version)

            if slice_time:
                # If slicing by time, this must be sorted along the time dimension
                if sort is False:
                    logger.warning(
                        f"Ignoring sort={sort} input as it is necessary to sort the data when extracting a start and end date range."
                    )

                self.data = self.data.sortby("time")
                sorted = True

                if self.data.time.size > 1:
                    start_date = start_date - Timedelta("1s")
                    # TODO: May want to consider this extra 1s subtraction as end_date on data has already has -1s applied.
                    end_date = end_date - Timedelta("1s")

                    # TODO - I feel we should do this in a tider way
                    start_date = start_date.tz_localize(None)
                    end_date = end_date.tz_localize(None)

                    self.data = self.data.sel(time=slice(start_date, end_date))
        else:
            raise ValueError(
                "Must supply either data or uuid and version, cannot create an empty data object."
            )

        if sort and not sorted:
            try:
                self.data = self.data.sortby("time")
            except KeyError:
                logger.debug("Cannot sort data by time as no time dimension present")

    def __bool__(self) -> bool:
        return bool(self.data)

    def __repr__(self) -> str:
        cls = self.__class__.__name__
        meta = self.metadata.copy()
        to_skip = [
            "conditions_of_use",
            "max_latitude",
            "variables",
            "title",
            "processed on",
            "time_resolution",
            "author",
            "ukghg sectors",
            "min_latitude",
            "versions",
            "processed by",
            "time_resolved",
            "max_longitude",
            "data_type",
            "processed",
            "min_longitude",
            "latest_version",
            "period",
            "time_period",
            "height",
            "conventions",
            "timestamp",
            "uuid",
        ]
        for key in to_skip:
            if key in meta:
                del meta[key]
        meta_str = truncated_dict_repr(meta)
        return f"{cls}(metadata={meta_str}, uuid={self._uuid or self.metadata.get('uuid')})"

    def __str__(self) -> str:
        return f"Data:\n{self.data}\nMetadata:\n{self.metadata}"
