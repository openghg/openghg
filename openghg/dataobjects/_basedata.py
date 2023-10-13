"""
This is used as a base for the other dataclasses and shouldn't be used directly.
"""
from typing import Dict

# from openghg.objectstore import open_zarr_store
import zarr


class _BaseData:
    def __init__(self, uuid: str, version: str, metadata: Dict, compute: bool = False) -> None:
        self._bucket = metadata["bucket"]
        self._uuid = uuid
        self.version = version
        self.metadata = metadata
        self.compute = compute
        self.data = None
        self._memory_store = {}

        # Retrieve the data from the memory store
        # lazy_zarr_store = open_zarr_store(bucket=self._bucket, datasource_uuid=self._uuid)
        # lazy_zarr_store = LocalZarrStore(bucket=self._bucket, datasource_uuid=uuid, mode="r")
        # Copy the data we want to the memory store
        # zarr.convenience.copy_store(
        #     source=lazy_zarr_store, dest=self._memory_store, source_path=version, if_exists="replace"
        # )

    def __bool__(self) -> bool:
        return bool(self._memory_store)
