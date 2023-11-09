"""
This is used as a base for the other dataclasses and shouldn't be used directly.
"""
from typing import Dict, Optional, Union
from openghg.store.zarr import LocalZarrStore
import xarray as xr
from pandas import Timestamp


class _BaseData:
    def __init__(
        self,
        metadata: Dict,
        data: Optional[xr.Dataset] = None,
        uuid: Optional[str] = None,
        version: Optional[str] = None,
        start_date: Optional[Union[str, Timestamp]] = None,
        end_date: Optional[Union[str, Timestamp]] = None,
        sort: bool = False,
        elevate_inlet: bool = False,
        attrs_to_check: Optional[Dict] = None,
    ) -> None:
        """
        Args:
            metadata: Dictionary of metadata
            data: Dataset if data is already loaded
            uuid: UUID of Datasource to retrieve data from
            version: Version of data requested from Datasrouce
            start_date: Start date of data to retrieve
            end_date: End date of data to retrieve
            sort: Sort the resulting Dataset by the time dimension, defaults to False
            elevate_inlet: Force the elevation of the inlet attribute
            attrs_to_check: Attributes to check for duplicates. If duplicates are present
                a new data variable will be created containing the values from each dataset
                If a dictionary is passed, the attribute(s) will be retained and the new value assigned.
                If a list/string is passed, the attribute(s) will be removed.
        """
        from openghg.util import timestamp_epoch, timestamp_now, dates_in_range

        if data is None and uuid is None and version is None:
            raise ValueError("Must supply either data or uuid and version")

        self.metadata = metadata
        self._lazy = False
        self._uuid = uuid

        if elevate_inlet:
            raise NotImplementedError("elevate_inlet not implemented yet")

        if attrs_to_check is not None:
            raise NotImplementedError("attrs_to_check not implemented yet")

        if sort:
            raise NotImplementedError("sort not implemented yet")

        if data is not None:
            self.data = data
        elif uuid is not None and version is not None:
            date_keys = self.metadata["versions"][version]

            if start_date is not None or end_date is not None:
                if start_date is None:
                    start_date = timestamp_epoch()
                if end_date is None:
                    end_date = timestamp_now()

                date_keys = dates_in_range(keys=date_keys, start_date=start_date, end_date=end_date)
            else:
                date_keys = self.metadata["versions"][version]

            self._bucket = metadata["object_store"]
            self._version = version

            zarrstore = LocalZarrStore(bucket=self._bucket, datasource_uuid=uuid, mode="r")
            self._memory_stores = zarrstore.copy_to_memorystore(keys=date_keys, version=version)
            self.data = xr.open_mfdataset(paths=self._memory_stores, engine="zarr", combine="by_coords")
            zarrstore.close()
        else:
            raise ValueError("Must supply either data or uuid and version, cannot create an empty data object.")

    def __bool__(self) -> bool:
        return bool(self.data)

    def __str__(self) -> str:
        return f"Data: {self.data}\nMetadata: {self.metadata}"
