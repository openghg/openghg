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


# ----------------------------------------
# Finding contiguous regions
# ----------------------------------------


def _sort_by_target(source: np.ndarray, target: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Sort `target` array and apply the same permutation to `source`."""
    shuf = np.argsort(target)
    return source[shuf], target[shuf]


def is_monotonic(idx: pd.Index) -> bool:
    """Return True if index is increasing or decreasing."""
    # explicitly convert to bool for mypy...
    return bool(idx.is_monotonic_increasing or idx.is_monotonic_decreasing)


def _alignment_indexers(
    source: pd.Index | np.ndarray,
    target: pd.Index,
    ignore_missing: bool = False,
    **kwargs: Any,
) -> tuple[np.ndarray, np.ndarray]:
    """Get indexers for aligning source and target."""
    if kwargs and not is_monotonic(target):
        raise ValueError("Cannot align to unsorted index with tolerances.")

    # type hints to help mypy... this shouldn't be necessary with numpy >= 2.3
    # and mypy >= 1.16.
    # see: https://github.com/numpy/numpy/issues/28076
    source_idxer: np.typing.NDArray[np.integer] = np.arange(len(source))
    align_idxer: np.typing.NDArray[np.integer] = target.get_indexer(source, **kwargs)

    if (align_idxer == -1).any():
        if not ignore_missing:
            raise ValueError(
                "If `ignore_missing==False`, the source index must be a subset of the target index "
            )
        source_idxer = source_idxer[align_idxer != -1]
        align_idxer = align_idxer[align_idxer != -1]

    return source_idxer, align_idxer


def _alignment_indexers_with_tolerances(
    source: pd.Index | np.ndarray,
    target: pd.Index,
    ignore_missing: bool = False,
    **kwargs: Any,
) -> tuple[np.ndarray, np.ndarray]:
    """Get indexers for aligning source and target with tolerances.

    To align with tolerances, the target index must be monotonic (increasing or decreasing). To align
    with a target that might not be monotonic, we sort the target index, align to the sorted target index,
    then undo the sorting.

    If working with data in memory, there isn't really any reason to do this, since you could just sort the
    target data. However, we cannot sort Zarr data stored on disk, so we must align as if we have sorted the
    Zarr data, and then translate the alignment back to the order of the data on disk.

    To be very precise, suppose our target index is the sequence t_1, t_2,..., t_n. The sorted target index
    is t_{p(1)} <= t_{p(2)} <= ... <= t_{p(n)}, where p is a permutation of 1,...,n. The permutation p can be found
    using `np.argsort`. Let's use T_i to denote t_{p(i)}.

    When we align a source index s_1, s_2,..., s_m to the sorted target index T_1, T_2,..., T_n, we are finding
    indices a_1, a_2,..., a_m so that s_j equals T_{a_j} up to tolerance (we'll clarify what this means below).

    We want a sequence b_1, b_2,..., b_m so that s_j equals t_{b_j} up to tolerance. Since T_{a_j} = t_{p(a_j)}, we see
    that we can take b_j = p(a_j). That is, the alignment indexer for the original target index is found by applying the
    sorting indexer to the alignment indexer for the sorted target index.

    When we say "s_j equals T_{a_j} up to tolerance", we mean that if `method="ffill"` and `tolerance=e`, then

    1) T_{a_j} <= s_j <= T_{a_j} + e
    2) there is no index k so that T_{a_j} < T_k <= s_j.

    If `method=bfill`, then the direction of the inequalities are reversed. If `method="nearest"`, then both conditions
    are considered and the closest value of T_k (from either forward or backward filling) is chosen.
    """
    source_idxer = np.arange(len(source))

    target_sort_idxer = np.argsort(target)
    target_sorted = target[target_sort_idxer]

    align_idxer = target_sorted.get_indexer(source, **kwargs)

    if (align_idxer == -1).any():
        if not ignore_missing:
            raise ValueError(
                "If `ignore_missing==False`, the source index must be a subset of the target index "
            )
        source_idxer = source_idxer[align_idxer != -1]
        align_idxer = align_idxer[align_idxer != -1]

    unsorted_align_idxer = target_sort_idxer[align_idxer]
    return source_idxer, unsorted_align_idxer


def alignment_indexers(
    source: pd.Index | np.ndarray,
    target: pd.Index,
    ignore_missing: bool = False,
    **kwargs: Any,
) -> tuple[np.ndarray, np.ndarray]:
    if kwargs and not is_monotonic(target):
        return _alignment_indexers_with_tolerances(source, target, ignore_missing, **kwargs)
    return _alignment_indexers(source, target, ignore_missing, **kwargs)


def contiguous_regions(
    source: pd.Index | np.ndarray,
    target: pd.Index,
    ignore_missing: bool = False,
    increasing_target_regions: bool = True,
    **kwargs: Any,  # TODO: I think we have a type for this...
) -> tuple[list[np.ndarray], list[np.ndarray], np.ndarray]:
    """Get indices partitioning `source` and `target` into matching contiguous regions.

    For example, if

    >>> target = pd.DatetimeIndex(pd.date_range("2020-01-01", freq="1d", periods=10))
    >>> source = target[:5].union(target[8:])

    Then

    >>> source[[0, 1, 2, 3, 4]] == target[[0, 1, 2, 3, 4]]
    >>> source[[5, 6]] == target[[8, 9]]

    and there is a gap of length 4 between the two target regions. This function returns these sets of indices and the
    list of gaps:

    >>> contiguous_regions(source, target)
    [array([0, 1, 2, 3, 4]), array([5, 6])] [array([0, 1, 2, 3, 4]), array([8, 9])] [4]

    Keyword args can be used to add tolerances to the alignment. For instance, if we modify each date of the
    source index by adding a random timedelta of less than one day, then we will get the same partition using
    `method="ffill"` and `tolerance=pd.Timedelta("1d")`.

    Args:
        source: index (or index values) from source data
        target: index from target data
        ignore_missing: if True, ignore indices in `source` that do not correspond to any index in `target`.
        increasing_target_regions: if True, sort the target indexer after aligning. This avoids gaps due to
            the target index being out of order. This requires shuffling the source data, but the source and
            target regions need to be sorted later if we're updating a Zarr store, so generally this should be
            set to `True`.
        **kwargs: keyword args to pass to `pd.Index.get_indexer`, namely: `method`, `tolerance`, `limit`.

    Returns:
        lists of index arrays for `source` and `target` matching regions of `source` to contiguous regions of `target`,
        as well as an array of the lengths of gaps between the contiguous regions.
    """
    source_idx, idxer = alignment_indexers(source, target, ignore_missing, **kwargs)

    if increasing_target_regions:
        source_idx, idxer = _sort_by_target(source_idx, idxer)

    # split target regions into contiguous blocks by looking for sequences in `idxer` of the form
    # m, m + 1, m + 2, ...
    diffs = np.diff(idxer)
    gap_idxs = np.flatnonzero(diffs != 1)

    return np.split(source_idx, gap_idxs + 1), np.split(idxer, gap_idxs + 1), diffs[gap_idxs]
