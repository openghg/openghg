from collections.abc import Iterable
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
import xarray as xr


class OverlapDeterminer:
    def __init__(self, index: pd.Index, **index_options: Any) -> None:
        """Create OverlapDeterminer object.

        This object can be used to test other arrays/indexes (or, generally, Iterables)
        against the stored index to check if any values in the other array overlap (or "conflict")
        with the stored index.

        With no index options, an overlap is an exact match, but options can be passed
        to match inexact matches. For instance

        >>> OverlapDeterminer(idx, method="nearest", tolerance=1e-2)

        will consider any value within 1e-2 of a values in `idx` to be a conflict.

        Args:
            index: a Pandas Index object.
            **index_options: Options to pass to `pd.Index.get_indexer`. These
                are the same options that can be passed to Xarray's `sel`. For
                instance `method` could be "ffill", "bfill", or "nearest", and a
                limit or tolerance can be set.

        """
        self.index = index
        self.index_options = index_options

    def overlaps(self, other: Iterable) -> npt.NDArray[np.bool_]:
        indexer = self.index.get_indexer(other, **self.index_options)
        result: npt.NDArray[np.bool_] = indexer != -1

        return result

    def nonoverlaps(self, other: Iterable) -> npt.NDArray[np.bool_]:
        return ~self.overlaps(other)

    def has_overlaps(self, other: Iterable) -> bool:
        return bool(np.any(self.overlaps(other)))

    def has_nonoverlaps(self, other: Iterable) -> bool:
        return bool(np.any(self.nonoverlaps(other)))

    def select_overlaps(self, ds: xr.Dataset, dim: str) -> xr.Dataset:
        try:
            other = ds[dim].values
        except KeyError as e:
            raise ValueError(f"Dimension {dim} not found.") from e

        return ds.where(ds[dim][self.overlaps(other)], drop=True)

    def select_nonoverlaps(self, ds: xr.Dataset, dim: str) -> xr.Dataset:
        try:
            other = ds[dim].values
        except KeyError as e:
            raise ValueError(f"Dimension {dim} not found.") from e

        return ds.where(ds[dim][self.nonoverlaps(other)], drop=True)
