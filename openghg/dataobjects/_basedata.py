"""
This is used as a base for the other dataclasses and shouldn't be used directly.
"""
from typing import Dict, Optional
from openghg.store.base import LocalZarrStore
import xarray as xr


class _BaseData:
    def __init__(
        self,
        metadata: Dict,
        data: Optional[xr.Dataset] = None,
        uuid: Optional[str] = None,
        version: Optional[str] = None,
    ) -> None:
        if data is None and uuid is None and version is None:
            raise ValueError("Must supply either data or uuid and version")

        self.metadata = metadata
        self._bucket = metadata["object_store"]
        self._data = data
        self._version = version
        self._lazy = False
        self._uuid = uuid

        if uuid is not None and version is not None:
            self._lazy = True
            self._memory_stores = []
            self._zarrstore = LocalZarrStore(bucket=self._bucket, datasource_uuid=uuid, mode="r")

    def __bool__(self) -> bool:
        return bool(self._data)

    @property
    def data(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> xr.Dataset:
        if self._lazy and self._data is None:
            date_keys = self.metadata["versions"][self._version]["keys"]
            self._memory_stores = self._zarrstore.copy_to_stores(keys=date_keys, version=self._version)
            self._data = xr.open_mfdataset(paths=self._memory_stores, engine="zarr", combine="by_coords")

        return self._data
