from collections.abc import Iterable
from typing import Any, Protocol

import numpy as np
import numpy.typing as npt
import pandas as pd
import xarray as xr


class HasConflictsMethod(Protocol):
    def conflicts(self, other: Iterable) -> npt.NDArray[np.bool_]: ...


class ConflictsMixin(HasConflictsMethod):
    """Mixin to add helper methods for a class with a `.conflicts()` method."""

    def nonconflicts(self, other: Iterable) -> npt.NDArray[np.bool_]:
        return ~self.conflicts(other)

    def has_conflicts(self, other: Iterable) -> bool:
        return bool(np.any(self.conflicts(other)))

    def has_nonconflicts(self, other: Iterable) -> bool:
        return bool(np.any(self.nonconflicts(other)))

    def select_conflicts(self, ds: xr.Dataset, dim: str) -> xr.Dataset:
        try:
            other = ds[dim].values
        except KeyError as e:
            raise ValueError(f"Dimension {dim} not found.") from e

        return ds.where(ds[dim][self.conflicts(other)], drop=True)

    def select_nonconflicts(self, ds: xr.Dataset, dim: str) -> xr.Dataset:
        try:
            other = ds[dim].values
        except KeyError as e:
            raise ValueError(f"Dimension {dim} not found.") from e

        return ds.where(ds[dim][self.nonconflicts(other)], drop=True)


class ConflictDeterminer(ConflictsMixin):
    def __init__(self, index: pd.Index, **index_options: Any) -> None:
        """Create ConflictDeterminer object.

        This object can be used to test other arrays/indexes (or, generally, Iterables)
        against the stored index to check if any values in the other array "conflict" with
        the stored index.

        With no index options, a "conflict" is an exact match, but options can be passed
        to match inexact matches. For instance

        >>> ConflictDeterminer(idx, method="nearest", tolerance=1e-2)

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

    def conflicts(self, other: Iterable) -> npt.NDArray[np.bool_]:
        indexer = self.index.get_indexer(other, **self.index_options)
        return indexer != -1
