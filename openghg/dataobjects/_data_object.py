"""Module for class DataObject"""

from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any, Iterator, Optional, Union

import pandas as pd
import xarray as xr

from openghg.store.base import Datasource
from openghg.types import ObjectStoreError
from openghg.util import daterange_overlap

from ._basedata import _BaseData

DateType = Union[str, pd.Timestamp]


class DataObject(MutableMapping):
    """DataObjects represent a unit of metadata and data stored in the object store.

    A DataObject acts like a dictionary of metadata, but also has methods to query and
    return data from the Datasource associated with that metadata.
    """

    def __init__(
        self,
        metadata: dict,
        bucket: Optional[str] = None,
    ) -> None:
        if "uuid" not in metadata:
            raise ValueError("Metadata must contain UUID.")

        uuid = metadata["uuid"]

        if "object_store" not in metadata and bucket is None:
            raise ValueError("If 'object_store' not in metadata, you must provide a value for `bucket`.")

        self.bucket = bucket or metadata["object_store"]

        self._metadata = metadata

        try:
            self._datasource = Datasource(self.bucket, uuid)
        except ObjectStoreError:
            # this option is to allow tests for search without adding real data to the object store
            self._datasource = Datasource(self.bucket)

    @property
    def metadata(self) -> dict:
        return self._metadata

    def __iter__(self) -> Iterator:
        return iter(self._metadata)

    def __len__(self) -> int:
        return len(self._metadata)

    def __getitem__(self, key) -> Any:
        # TODO: add lookup for Datasource metadata as well, so it doesn't need to be copied to the metastore
        return self._metadata[key]

    def __setitem__(self, key, value) -> None:
        self._metadata[key] = value

    def __delitem__(self, key) -> None:
        del self._metadata[key]

    def get_data(
        self,
        start_date: Optional[DateType] = None,
        end_date: Optional[DateType] = None,
        version: str = "latest",
    ) -> xr.Dataset:
        result = self._datasource.get_data(version=version)

        # NOTE: len(result.time) > 1 is a hack to avoid slicing annual fluxes
        # but it only works if there is a single year of fluxes
        # TODO: move this slicing logic to datasource, where it can be
        # implemented more carefully
        if (start_date or end_date) and len(result.time) > 1:
            if start_date is not None:
                start_date = pd.to_datetime(start_date)
            if end_date is not None:
                end_date = pd.to_datetime(end_date)

            result = result.sortby("time").sel(time=slice(start_date, end_date))
        return result

    def has_data_between(
        self, start_date: Optional[DateType] = None, end_date: Optional[DateType] = None
    ) -> bool:
        # TODO: move this logic to Datasource

        latest_version = self._datasource._latest_version
        date_keys = self._datasource._data_keys[latest_version] if self._datasource._data_keys else []

        if start_date is not None or end_date is not None:
            if start_date is None:
                start_date = pd.Timestamp(0)

            if end_date is None:
                end_date = pd.Timestamp.now()

            new_daterange = f"{start_date}_{end_date}"

            return any(daterange_overlap(existing, new_daterange) for existing in date_keys)

        return True

    def to_basedata(
        self,
        version: str = "latest",
        start_date: Optional[DateType] = None,
        end_date: Optional[DateType] = None,
        sort: bool = False,
    ) -> _BaseData:
        data = self.get_data(start_date, end_date, version)
        return _BaseData(self.metadata, data=data, sort=sort)

    def copy(self) -> DataObject:
        return DataObject(self.metadata.copy(), self.bucket)
