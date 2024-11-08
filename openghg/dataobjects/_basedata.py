"""
This is used as a base for the other metadata/data classes.
"""
from __future__ import annotations

import logging
from typing import Dict, Iterable, Optional, overload, Union
from typing_extensions import Self

import pandas as pd
import xarray as xr

from openghg.store.storage import LocalZarrStore
from openghg.types import HasMetadataAndData
from ._data_object import DataObject


logger = logging.getLogger("openghg.dataobjects")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


DateType = Union[str, pd.Timestamp]


class BaseData:
    def __init__(
        self,
        metadata: Dict,
        data: Optional[xr.Dataset] = None,
        uuid: Optional[str] = None,
        version: Optional[str] = None,
        start_date: Optional[Union[str, pd.Timestamp]] = None,
        end_date: Optional[Union[str, pd.Timestamp]] = None,
        sort: bool = True,
        elevate_inlet: bool = False,
        attrs_to_check: Optional[Dict] = None,
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

        self._start_date = start_date
        self._end_date = end_date
        self._zarrstore = None

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

            self._zarrstore = LocalZarrStore(bucket=self._bucket, datasource_uuid=uuid, mode="r")

            self.data = self._zarrstore.get(version=version)
            if slice_time:
                # If slicing by time, this must be sorted along the time dimension
                if sort is False:
                    logger.warning(
                        f"Ignoring sort={sort} input as it is necessary to sort the data when extracting a start and end date range."
                    )

                self.data = self.data.sortby("time")
                sorted = True

                if self.data.time.size > 1:
                    start_date = start_date - pd.Timedelta("1s")
                    # TODO: May want to consider this extra 1s subtraction as end_date on data has already has -1s applied.
                    end_date = end_date - pd.Timedelta("1s")

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

    def __str__(self) -> str:
        return f"Data: {self.data}\nMetadata: {self.metadata}"

    @classmethod
    def from_data_object(
        cls,
        do: DataObject,
        version: str = "latest",
        start_date: Optional[DateType] = None,
        end_date: Optional[DateType] = None,
        sort: bool = False,
    ) -> Self:
        data = do.get_data(start_date, end_date, version)
        return cls(do.metadata, data=data, sort=sort)

    @overload
    @classmethod
    def cast(cls, other: HasMetadataAndData) -> Self:
        ...

    @overload
    @classmethod
    def cast(cls, other: Iterable[HasMetadataAndData]) -> list[Self]:
        ...

    @classmethod
    def cast(cls, other: Union[HasMetadataAndData, Iterable[HasMetadataAndData]]) -> Union[Self, list[Self]]:
        if isinstance(other, Iterable):
            return [cls(metadata=x.metadata, data=x.data) for x in other]
        return cls(metadata=other.metadata, data=other.data)
