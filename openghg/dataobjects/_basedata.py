"""
This is used as a base for the other metadata/data classes.

BaseData holds metadata and data returned from the object store.
"""

from __future__ import annotations

import logging
from typing import Iterable, MutableMapping, Optional, overload, Union
from typing_extensions import Self

import pandas as pd
import xarray as xr

from openghg.objectstore import DataObject
from openghg.types import HasMetadataAndData


logger = logging.getLogger("openghg.dataobjects")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


DateType = Union[str, pd.Timestamp]


class BaseData:
    def __init__(
        self,
        metadata: MutableMapping,
        data: xr.Dataset,
        sort: bool = False,
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
        self.metadata = dict(metadata)
        self.data = data

        if "time" in self.data:
            if sort and not self.data.indexes["time"].is_monotonic_increasing:
                self.data = self.data.sortby("time")

            # HACK: to deal with this issue: https://github.com/pydata/xarray/issues/9753
            if len(self.data.time) == 1:
                self.data = self.data.compute()

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
    def cast(cls, other: HasMetadataAndData) -> Self: ...

    @overload
    @classmethod
    def cast(cls, other: Iterable[HasMetadataAndData]) -> list[Self]: ...

    @classmethod
    def cast(cls, other: Union[HasMetadataAndData, Iterable[HasMetadataAndData]]) -> Union[Self, list[Self]]:
        if isinstance(other, Iterable):
            return [cls(metadata=x.metadata, data=x.data) for x in other]
        return cls(metadata=other.metadata, data=other.data)
