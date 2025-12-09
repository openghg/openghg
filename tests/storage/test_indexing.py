import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.storage._indexing import _alignment_indexers, _alignment_indexers_with_tolerances, contiguous_regions, is_monotonic, OverlapDeterminer


#------------------------------
# OverlapDeterminer tests
#------------------------------


@pytest.fixture()
def cd_datetime_exact():
    idx = pd.date_range("2020-01-01", "2020-01-02", freq="1h")
    return OverlapDeterminer(idx)


@pytest.fixture()
def cd_datetime_nearest_1min():
    idx = pd.date_range("2020-01-01", "2020-01-02", freq="1h")
    return OverlapDeterminer(idx, method="nearest", tolerance=pd.Timedelta("60s"))


# times used in tests
times = ["2020-01-01", "2020-01-01 00:00:49", "2020-01-01 00:30:00", "2020-01-02", "2020-01-02 00:01:01"]


@pytest.mark.parametrize(
    ("other", "expected"),
    [
        (["2020-01-01"], [True]),
        (["2020-01-01 00:00:01"], [False]),
        (times, [True, False, False, True, False]),
    ],
)
def test_overlaps_exact(cd_datetime_exact, other, expected):
    assert cd_datetime_exact.has_overlaps(other) == any(expected)
    assert cd_datetime_exact.has_nonoverlaps(other) == any([not exp for exp in expected])
    assert all(cd_datetime_exact.overlaps(other) == expected)


@pytest.mark.parametrize(
    ("other", "expected"),
    [
        (["2020-01-01"], [True]),
        (["2020-01-01 00:00:01"], [True]),
        (times, [True, True, False, True, False]),
    ],
)
def test_overlaps_nearest_1min(cd_datetime_nearest_1min, other, expected):
    assert cd_datetime_nearest_1min.has_overlaps(other) == any(expected)
    assert cd_datetime_nearest_1min.has_nonoverlaps(other) == any([not exp for exp in expected])
    assert all(cd_datetime_nearest_1min.overlaps(other) == expected)


def test_select_overlaps_nonoverlaps(cd_datetime_nearest_1min):
    """Test selecting overlaps and non-overlaps from Dataset."""
    ds = xr.Dataset({"x": (["time"], np.arange(len(times)))}, coords={"time": (["time"], times)})

    xr.testing.assert_equal(ds.isel(time=[0, 1, 3]), cd_datetime_nearest_1min.select_overlaps(ds, "time"))

    xr.testing.assert_equal(ds.isel(time=[2, 4]), cd_datetime_nearest_1min.select_nonoverlaps(ds, "time"))


#----------------------------------------
# Alignment and contiguous regions tests
#----------------------------------------


# TODO: make a more diverse set of test indexes
@pytest.fixture
def indexes():
    idx1 = pd.date_range("2020-01-01", "2020-01-02", freq="h", inclusive="left")
    idx2 = idx1[:5].union(idx1[10:])  # missing indices 5, 6, 7, 8, 9
    idx3 = idx1[:5].union(idx1[5:10] + pd.Timedelta("1m")).union(idx1[10:])  # modified indices 5, 6, 7, 8, 9
    idx4 = idx1 + pd.Timedelta("4h")  # shift by 4 hours
    idx5 = pd.date_range("2020-01-02", "2020-01-03", freq="h", inclusive="left")  # index starting after idx1

    return [idx1, idx2, idx3, idx4, idx5]


# TODO: parametrize this test
def test_alignment_of_index_to_shuffle_of_itself(indexes):
    """Check if we can align an index to a shuffled version of itself.

    The most important case here is when we want to align with a method like "nearest".
    Pandas' `Index` object doesn't support alignment to a non-monotonic index (i.e. to an
    index that is not increasing or decreasing). But we need to be able to do this to align
    to stored data, since we can't sort the stored data.

    The function `_alignment_indexers` doesn't support alignment with tolerances if the target
    index is non-monotonic. Thus we expect an error to be raised in this case.
    """
    rng = np.random.default_rng(seed=1234567)

    for align_kwargs in [{}, {"method": "nearest"}]:
        for err, align_func in [(True, _alignment_indexers), (False, _alignment_indexers_with_tolerances)]:
            for idx in indexes:
                for _ in range(5):
                    shuf = rng.permutation(len(idx))
                    idx_shuf = idx[shuf]

                    try:
                        # get alignment indexer
                        _, idxer = align_func(idx, idx_shuf, **align_kwargs)
                    except ValueError:
                        # Expect errors for _alignment_indexers when kwargs passed since
                        # target is not monotonic (increasing or decreasing).
                        # The _alignment_indexers_with_tolerances function should handle this case.
                        if not err:
                            assert False, "error shouldn't have been raised"
                        else:
                            assert align_kwargs and not is_monotonic(idx_shuf)
                    else:
                        # check alignment worked
                        pd.testing.assert_index_equal(idx, idx_shuf[idxer])

                        # check that alignment idxer is the inverse of the shuffle used to create the target (`idx_shuf`)
                        inv_shuf = np.argsort(shuf)
                        np.testing.assert_equal(idxer, inv_shuf)


# TODO: parametrize this test?
def test_alignment_with_tolerances(indexes):
    """Test alignment with tolerances.

    If we perturb the source index slightly, but add a tolerance to the alignment to
    compensate, then we should get the same result (provided the perturbation/tolerance
    is small compared to the separation of the index values).
    """
    rng = np.random.default_rng(seed=12345)

    for idx in indexes:
        for _ in range(5):
            shuf = rng.permutation(len(idx))
            idx_shuf = idx[shuf]

            perturbation = pd.to_timedelta(0.5 * rng.random(len(idx)), unit="h")
            idx_perturb = idx.values + perturbation

            _, idxer = _alignment_indexers_with_tolerances(idx, idx_shuf)
            _, idxer_perturb = _alignment_indexers_with_tolerances(idx_perturb, idx_shuf, method="nearest", tolerance=pd.Timedelta("0.5h"), limit=1)

            np.testing.assert_equal(idxer, idxer_perturb)


def make_missing(n, rng, size):
    """Make an array with values from 0 to n, with some values missing at random."""
    missing = rng.choice(n, size)
    arr = np.array([x for x in range(n) if x not in missing])
    return arr, missing


def split_by_missing(target, missing):
    """Split numbers 0 to n by a set of missing values."""
    regions = []
    current = []
    for i in range(len(target)):
        if target[i] in missing:
            if current:
                regions.append(np.array(current))
            current = []
        else:
            current.append(i)

    if current:
        regions.append(np.array(current))
    return regions


@pytest.mark.parametrize(("n", "size"), [(10, 2), (100, 5)])
def test_contiguous_regions(n, size):
    """If the source index is missing values, the target region should be partitioned on these values.

    For example,

    >>> target = pd.Index([1, 2, 3, 4, 5])
    >>> source = np.array([1, 2, 4, 5])
    >>> contiguous_regions(source, target)
    ([array([0, 1]), array([2, 3])], [array([0, 1]), array([3, 4])], array([2]))

    So the target index is split into two regions by the missing value 3.

    This test randomly removes some elements from the target index and then checks
    that the regions are created by splitting on the missing elements.

    For simplicity, we take the target index to be 0, 1, 2,..., n - 1.
    """
    rng = np.random.default_rng(seed=1234567)
    target = pd.Index(np.arange(n))

    for _ in range(5):
        for _ in range(5):
            source, missing = make_missing(n, rng, size)
            _, target_regions, _ = contiguous_regions(source, target)

            expected = split_by_missing(target, missing)
            assert all((a == b).all() for a, b in zip(target_regions, expected))


@pytest.mark.parametrize(("n", "size"), [(10, 2), (100, 5)])
def test_contiguous_regions_shuffled_target(n, size):
    """This test is the same as `test_contiguous_regions`, except the target is shuffled.

    For example,

    >>> target = pd.Index([3, 10, 6, 7, 4, 1, 2, 8, 9, 5])
    >>> source = np.array([1, 2, 3, 4, 5, 7, 8, 9, 10])
    >>> contiguous_regions(source, target)
    ([array([2, 8]), array([5, 3, 0, 1, 6, 7, 4])], [array([0, 1]), array([3, 4, 5, 6, 7, 8, 9])], array([2]))

    So again, the target is split on the missing value 6, which happens to be in the second position. The
    source index is shuffled to align to these write regions.
    """
    rng = np.random.default_rng(seed=1234567)

    for _ in range(5):
        target = pd.Index(rng.permutation(n))
        for _ in range(5):
            source, missing = make_missing(n, rng, size)
            _, target_regions, _ = contiguous_regions(source, target)

            expected = split_by_missing(target, missing)
            assert all((a == b).all() for a, b in zip(target_regions, expected))
