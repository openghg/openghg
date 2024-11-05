"""Module for class DataObject"""
from collections.abc import MutableMapping
from typing import Any, Iterator, Optional, Union

import pandas as pd
import xarray as xr

from openghg.store.base import Datasource
from openghg.types import ObjectStoreError
from openghg.util import dates_overlap, timestamp_epoch, timestamp_now, timestamp_tzaware


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

        bucket = bucket or metadata["object_store"]

        self._metadata = metadata

        try:
            self._datasource = Datasource(bucket, uuid)
        except ObjectStoreError:
            # this option is to allow tests for search without adding real data to the object store
            self._datasource = Datasource(bucket)

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
    ) -> xr.Dataset:
        result = self._datasource.get_data()
        if start_date or end_date:
            result = result.sel(time=slice(start_date, end_date))
        return result

    def has_data_between(
        self, start_date: Optional[DateType] = None, end_date: Optional[DateType] = None
    ) -> bool:
        self_start = self._metadata.get("start_date", timestamp_epoch())
        self_end = self._metadata.get("end_date", timestamp_now())

        if start_date is not None or end_date is not None:
            if start_date is None:
                start_date = timestamp_epoch()
            else:
                start_date = timestamp_tzaware(start_date) + pd.Timedelta("1s")  # type: ignore ...this is unlikely to be NaT

            if end_date is None:
                end_date = timestamp_now()
            else:
                end_date = timestamp_tzaware(end_date) - pd.Timedelta("1s")  # type: ignore ...this is unlikely to be NaT

            return dates_overlap(start_a=start_date, end_a=end_date, start_b=self_start, end_b=self_end)  # type: ignore start_date and end_date are not None

        return True
