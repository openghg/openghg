from collections.abc import Callable, Iterable
import logging
from pathlib import Path
import re
from typing import Any, cast, Generic, Literal, TypeVar

import pandas as pd
import xarray as xr
import zarr

from openghg.types import DataOverlapError
from openghg.util._versioning import SimpleVersioning
from ._encoding import get_zarr_encoding, normalise_zarr_encoding
from ._indexing import contiguous_regions, IndexingError, OverlapDeterminer
from ._store import Store, UpdateError, VersionedStore
from ._zarr_compat import (
    ZarrStoreBase,
    clear_store,
    copy_store,
    has_async_store_api,
    make_local_store,
    make_memory_store,
)

logger = logging.getLogger("openghg.storage")
logger.setLevel(logging.DEBUG)


def parse_to_zarr_kwargs(to_zarr_kwargs: dict) -> dict:
    accepted_keys = ["write_empty_chunks", "zarr_format", "storage_options", "align_chunks"]
    result = {"zarr_format": 2, "align_chunks": True}
    for k, v in to_zarr_kwargs.items():
        if k in accepted_keys:
            result[k] = v
    if result["zarr_format"] not in (None, 2, 3):
        raise ValueError("zarr_format must be None, 2, or 3.")
    return result


ZST = TypeVar("ZST", bound=ZarrStoreBase)


class ZarrStore(Store, Generic[ZST]):
    """Zarr store for storing a single dataset."""

    def __init__(
        self,
        zarr_store: ZST,
        append_dim: str = "time",
        index_options: dict | None = None,
        compressor: Any | None = None,
        filters: Any | None = None,
        encoding: dict | None = None,
        **to_zarr_kwargs: Any,
    ) -> None:
        """Pass an instantiated Zarr Store.

        Note: for commonly used types of ZarrStore, we can create convenience functions
        to create ZarrStore objects.

        Args:
            zarr_store: instantiated Zarr Store.
            append_dim: dimension to insert new data along.
            index_options: options for index, such as `method = "nearest"`
            compressor: numcodecs compressor for format 2, or a native
                ``zarr.codecs`` codec for format 3. Codecs are not converted between formats.
            filters: filters compatible with the selected format.
            encoding: dictionary mapping data variables to encoding dictionary
            to_zarr_kwargs: arguments that could be passed to `xr.Dataset.to_zarr`.
              Not all parameters will be passed on. See here for the full description
              of the parameters: https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_zarr.html

              Accepted arguments are:
              - `write_empty_chunks`
              - `zarr_format`: None, 2, or 3. New stores default to format 2;
                format 3 requires Zarr 3. Existing stores retain their stored format.
              - `align_chunks`: True by default to prevent overlapping Dask writes.
              - `storage_options`: only relevant to cloud storage, see
                 https://github.com/pydata/xarray/pull/5615

        """
        super().__init__()
        self._store = zarr_store
        self.append_dim = append_dim
        self.index_options = index_options or {}
        self.compressor = compressor
        self.filters = filters
        self.encoding = encoding or {}
        self.to_zarr_kwargs = to_zarr_kwargs

    # use property to control assignment of `to_zarr_kwargs`
    @property
    def to_zarr_kwargs(self) -> dict:
        return self._to_zarr_kwargs

    @to_zarr_kwargs.setter
    def to_zarr_kwargs(self, value: dict) -> None:
        self._to_zarr_kwargs = parse_to_zarr_kwargs(value)

    @property
    def store(self) -> ZST:
        """Underlying Zarr storage."""
        return self._store

    @property
    def _zarr_format(self) -> int:
        """Retain an existing store's format; default new stores to format 2."""
        if not self._is_empty():
            if not has_async_store_api():
                return 2
            return int(zarr.open_group(self.store, mode="r", use_consolidated=False).metadata.zarr_format)
        return int(self.to_zarr_kwargs.get("zarr_format") or 2)

    @property
    def _write_kwargs(self) -> dict[str, Any]:
        """Choose metadata handling from the actual storage format."""
        result = self.to_zarr_kwargs.copy()
        result["zarr_format"] = self._zarr_format
        if result["zarr_format"] == 3 and not has_async_store_api():
            raise ValueError("Writing Zarr format 3 requires the Zarr 3 package.")
        result["consolidated"] = result["zarr_format"] == 2
        return result

    @property
    def index(self) -> pd.Index:
        """Index of append dimension of data.

        This index is in the order of the data as it is stored on disk,
        which is needed for proper alignment during updates.
        """
        return self._get(sort=False).get_index(self.append_dim)

    @property
    def _overlap_determiner(self) -> OverlapDeterminer:
        return OverlapDeterminer(index=self.index, **self.index_options)

    def _is_empty(self) -> bool:
        """Return whether the current underlying Zarr store contains no keys."""
        if has_async_store_api():
            from zarr.core.sync import sync

            return bool(sync(self.store.is_empty("")))
        return not bool(cast(Any, self.store))

    def __bool__(self) -> bool:
        return not self._is_empty()

    def clear(self) -> None:
        clear_store(self.store)

    def bytes_stored(self) -> int:
        if has_async_store_api():
            from zarr.core.sync import sync

            return int(sync(self.store.getsize_prefix("")))

        legacy_store = cast(Any, self.store)
        return (
            sum(legacy_store.getsize(key) for key in legacy_store) if hasattr(legacy_store, "getsize") else 0
        )

    def _get(self, sort: bool = True) -> xr.Dataset:
        if not bool(self):
            return xr.Dataset()

        # need to sort to be consistent with MemoryStore
        result = xr.open_zarr(self.store, consolidated=self._zarr_format == 2)

        if sort:
            result = result.sortby(self.append_dim)

        return cast(xr.Dataset, result)

    def get(self) -> xr.Dataset:
        return self._get(sort=True)

    def insert(self, data: xr.Dataset, on_overlap: Literal["error", "ignore"] = "error") -> None:
        if self._is_empty():
            encoding = get_zarr_encoding(data.data_vars, self.compressor, self.filters)
            encoding.update(self.encoding)
            data.to_zarr(
                store=self.store,
                mode="w",
                compute=True,
                encoding=normalise_zarr_encoding(encoding),
                **self._write_kwargs,
            )
        else:
            if self._overlap_determiner.has_overlaps(data.get_index(self.append_dim)):
                if on_overlap == "error":
                    raise DataOverlapError("Cannot insert data with overlaps if `on_overlap` == 'error'")

                # otherwise, select non-overlaps
                data = self._overlap_determiner.select_nonoverlaps(data, self.append_dim)
                if data.sizes.get(self.append_dim) == 0:
                    logger.info("No data to insert.")
                    return None

            data.to_zarr(
                store=self.store,
                mode="a",
                append_dim=self.append_dim,
                compute=True,
                **self._write_kwargs,
            )

    def update(self, data: xr.Dataset, on_nonoverlap: Literal["error", "ignore"] = "error") -> None:

        if self._is_empty():
            raise UpdateError("Cannot update empty Store.")
        else:
            if self._overlap_determiner.has_nonoverlaps(data.get_index(self.append_dim)):
                if on_nonoverlap == "error":
                    raise UpdateError("Cannot add new values with `update`.")

                # otherwise, select conflicts/overlapping values
                data = self._overlap_determiner.select_overlaps(data, self.append_dim)

            # nothing to update
            if not bool(data) or data.sizes.get(self.append_dim) == 0:
                logger.warning("No data to update with.")
                return None

            try:
                data.to_zarr(
                    store=self.store,
                    mode="r+",
                    region="auto",
                    compute=True,
                    **self._write_kwargs,
                )
            except (ValueError, IndexError):
                kwargs = self.index_options.copy()

                # only allow one source value to align to a given target value; if multiple source values
                # align to the same target value, an error will be raised by `contiguous_regions` if `limit=1`.
                if "method" in kwargs:
                    kwargs["limit"] = 1

                try:
                    source_regions, target_regions, _ = contiguous_regions(
                        data.get_index(self.append_dim), self.index, **kwargs
                    )
                except IndexingError as e:
                    raise UpdateError(
                        f"Multiple input values map to the same stored value with index options {self.index_options}."
                    ) from e

                # Corrections may lazily read this store. Evaluate them before any
                # fallback write changes values needed by another region.
                data = data.compute()

                # can only write to specified region if data vars have dimension in common with that region
                # so we will first write the variables without the append dimension (since this is the dim
                # where we specify regions), then we will write the variables that have the append dim as
                # a dimension.
                non_region_vars = [dv for dv in data.data_vars if self.append_dim not in data[dv].dims]

                # don't catch any errors here, since these errors are unrelated to alignment
                data[non_region_vars].to_zarr(
                    store=self.store,
                    mode="r+",
                    region="auto",
                    compute=True,
                    **self._write_kwargs,
                )

                # now proceed with variables that contain the append dim
                data = data.drop_vars(non_region_vars)

                # Distinct regions may share a stored chunk. Complete each write
                # before starting the next; align_chunks protects tasks within it.
                for sregion, tregion in zip(source_regions, target_regions):
                    region = {self.append_dim: slice(tregion[0], tregion[-1] + 1)}
                    data.isel({self.append_dim: sregion}).to_zarr(
                        store=self.store,
                        mode="r+",
                        region=region,
                        compute=True,
                        **self._write_kwargs,
                    )


def get_zarr_directory_store(
    path: Path, append_dim: str = "time", index_options: dict | None = None, **kwargs: Any
) -> ZarrStore[ZarrStoreBase]:
    """Factory function to create ZarrStore objects based on a local Zarr store.

    Args:
        path: path to Zarr store location.
        append_dim: dimension to append new data along.
        index_options: options for index along append dimension; for instance
          `method = "nearest"`.
        kwargs: `compressor`, `filters`, `encoding`, or arguments that could be
          passed to `xr.Dataset.to_zarr`. New stores default to format 2; format 3
          requires Zarr 3 and native ``zarr.codecs`` codecs. Existing stores retain
          their stored format, even when a different format is requested.

    Returns:
        ZarrStore backed by the installed Zarr storage API.

    """
    store = make_local_store(path)
    return ZarrStore[ZarrStoreBase](store, append_dim=append_dim, index_options=index_options, **kwargs)


def get_zarr_memory_store(
    append_dim: str = "time", index_options: dict | None = None, **kwargs: Any
) -> ZarrStore[ZarrStoreBase]:
    """Factory function to create ZarrStore objects based on an in-memory Zarr store.

    Args:
        append_dim: dimension to append new data along.
        index_options: options for index along append dimension; for instance
          `method = "nearest"`.
        kwargs: `compressor`, `filters`, `encoding`, or arguments that could be
          passed to `xr.Dataset.to_zarr`. New stores default to format 2; format 3
          requires Zarr 3 and native ``zarr.codecs`` codecs. Existing stores retain
          their stored format, even when a different format is requested.

    Returns:
        ZarrStore backed by the installed Zarr storage API.

    """
    store = make_memory_store()
    return ZarrStore[ZarrStoreBase](store, append_dim=append_dim, index_options=index_options, **kwargs)


class VersionedZarrStore(VersionedStore, SimpleVersioning[ZST], ZarrStore[ZST]):
    """Zarr storage with versions.

    This class uses the methods from `ZarrStore` but overrides the `._store` attribute
    to point to the current version of the zarr store held by the `SimpleVersioning` class.

    Overriding `._store`
    """

    def __init__(
        self,
        factory: Callable[[str], ZST],
        versions: Iterable[str] | None = None,
        append_dim: str = "time",
        index_options: dict | None = None,
        compressor: Any | None = None,
        filters: Any | None = None,
        encoding: dict | None = None,
        **to_zarr_kwargs: Any,
    ) -> None:
        """Create VersionedZarrStore object.

        The `factory` and `versions` arguments are used to set up the
        versioning, and the remaining arguments are passed to `ZarrStore`.

        Args:
            factory: function that produces a zarr store, given a version. For
              instance, "v1" might map to a zarr directory store with path
              `root_path / "v1"`.
            versions: Versions to instantiate; these are loaded using the
              `factory` function.
            append_dim: dimension to append new data along.
            index_options: options for the index used to resolve overlaps/conflicts when
              adding data to the store.
            compressor: numcodecs compressor for format 2, or a native
                ``zarr.codecs`` codec for format 3. Codecs are not converted between formats.
            filters: filters compatible with the selected format.
            encoding: dictionary mapping data variables to encoding dictionary
            to_zarr_kwargs: arguments that could be passed to `xr.Dataset.to_zarr`.
              Not all parameters will be passed on. See here for the full description
              of the parameters: https://docs.xarray.dev/en/latest/generated/xarray.Dataset.to_zarr.html

              Accepted arguments are:
              - `write_empty_chunks`
              - `zarr_format`: None, 2, or 3. New stores default to format 2;
                format 3 requires Zarr 3. Existing stores retain their stored format.
              - `align_chunks`: True by default to prevent overlapping Dask writes.
              - `storage_options`: only relevant to cloud storage, see
                 https://github.com/pydata/xarray/pull/5615

        """
        super().__init__(
            factory=factory,
            versions=versions,
        )

        # manually set attributes for underlying ZarrStore (except for `self._store`,
        # which is delegated to the current version).
        self.append_dim = append_dim
        self.index_options = index_options or {}
        self.compressor = compressor
        self.filters = filters
        self.encoding = encoding or {}
        self.to_zarr_kwargs = parse_to_zarr_kwargs(to_zarr_kwargs)

    # make ._store an alias for ._current
    @property
    def _store(self) -> ZST:
        return self._current

    @_store.setter
    def _store(self, value: ZST) -> None:
        self._current = value

    def copy_to_version(self, v: str) -> None:
        """Copy current version to specified version.

        The version "v" is created if it doesn't exist, and is overwritten otherwise.

        Encoded chunks and metadata are copied without decoding. Replacing an
        existing version is not transactional; a failed replacement can leave
        partial data. Failed copies to new versions are removed.

        Args:
            v: version to copy to

        Raises:
            VersionError if no version is currently checked out.

        """
        source = self._current  # will raise VersionError if no version checked out

        if v == self.current_version:
            return
        is_new = v not in self.versions
        dest = self.factory(v) if is_new else self._versions[v]
        try:
            clear_store(dest)
            copy_store(source, dest)
        except Exception:
            if is_new:
                clear_store(dest)
            raise
        self._versions[v] = dest


def get_versioned_zarr_directory_store(
    path: Path,
    versions: Iterable[str] | None = None,
    append_dim: str = "time",
    index_options: dict | None = None,
    version_pat: str = r"v\d+",
    **kwargs: Any,
) -> VersionedZarrStore[ZarrStoreBase]:
    """Factory function to create VersionedZarrStore objects based on a local Zarr store.

    Args:
        path: root directory for versioned Zarr stores.
        versions: list of versions to load.
        append_dim: dimension to append new data along.
        index_options: options for the index used to resolve overlaps/conflicts when
            adding data to the store.
        version_pat: regex pattern to match existing versions
        kwargs: `compressor`, `filters`, `encoding`, or arguments that could be
          passed to `xr.Dataset.to_zarr`. New stores default to format 2; format 3
          requires Zarr 3 and native ``zarr.codecs`` codecs. Existing stores retain
          their stored format, even when a different format is requested.

    Returns:
        VersionedZarrStore object with a local Zarr store as the underlying
        storage.

    """
    path = path.absolute()
    versions = set([]) if versions is None else set(versions)

    # make path or look for versions if it already exists
    if not path.exists():
        path.mkdir(parents=True)
    else:
        # look for existing versions
        compiled_reg = re.compile(version_pat)
        for f in sorted(path.iterdir()):
            if compiled_reg.match(str(f.name)):
                versions.add(f.name)

    # factory function to create a Directory Stores corresponding to versions
    def factory(v: str) -> ZarrStoreBase:
        """Factory function to create a Directory Store corresponding to version."""
        return make_local_store(path / v)

    return VersionedZarrStore[ZarrStoreBase](
        factory=factory,
        versions=versions,
        append_dim=append_dim,
        index_options=index_options,
        **kwargs,
    )


def get_versioned_zarr_memory_store(
    versions: Iterable[str] | None = None,
    append_dim: str = "time",
    index_options: dict | None = None,
    **kwargs: Any,
) -> VersionedZarrStore[ZarrStoreBase]:
    """Factory function to create VersionedZarrStore objects based on an in-memory Zarr store.

    Args:
        versions: list of versions to load.
        append_dim: dimension to append new data along.
        index_options: options for the index used to resolve overlaps/conflicts when
          adding data to the store.
        kwargs: `compressor`, `filters`, `encoding`, or arguments that could be
          passed to `xr.Dataset.to_zarr`. New stores default to format 2; format 3
          requires Zarr 3 and native ``zarr.codecs`` codecs. Existing stores retain
          their stored format, even when a different format is requested.

    Returns:
        VersionedZarrStore object with an in-memory Zarr store as the underlying
        storage.

    """

    def factory(_: str) -> ZarrStoreBase:
        """Factory for versioning."""
        return make_memory_store()

    return VersionedZarrStore[ZarrStoreBase](
        factory=factory,
        versions=versions,
        append_dim=append_dim,
        index_options=index_options,
        **kwargs,
    )
