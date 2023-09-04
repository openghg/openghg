"""
Tests for the TinyDB based metastore.

It should be possible to use these tests with any metastore,
provided the fixtures are changed.
"""
import pytest

from openghg.store import load_metastore
from openghg.store.metastore._metastore import TinyDBMetaStore
from openghg.store.metastore._classic_metastore import get_metakey
from openghg.types import MetastoreError


@pytest.fixture
def metastore(tmp_path):
    """Open metastore with no data type.

    Note: `tmp_path` is function scope, so the metastore is
    reset for each test that uses this fixture.
    """
    bucket = str(tmp_path)
    with load_metastore(bucket=bucket, key='') as session:
        metastore = TinyDBMetaStore(
            bucket=bucket,
            session=session)
        yield metastore


@pytest.fixture
def surface_metastore(tmp_path):
    """Open metastore with key for `ObsSurface`.

    NOTE: we should be able to use any 'key' here besides '' to
    show that this metastore is independent of the metastore provided
    by the `metastore` fixture.
    """
    bucket = str(tmp_path)
    with load_metastore(bucket=bucket, key=get_metakey("surface")) as session:
        metastore = TinyDBMetaStore(
            bucket=bucket,
            session=session)
        yield metastore


def test_search_empty(metastore):
    results = metastore.search()
    assert results == []


def test_add(metastore):
    metastore.add({"key1": "val1"})
    result = metastore.search()

    assert len(result) == 1

    assert result[0]['key1'] == 'val1'


def test_search(metastore):
    """Test searching when there are multiple items
    in the metastore.
    """
    metastore.add({"key": "val1"})
    metastore.add({"key": "val2"})

    result_all = metastore.search()

    assert len(result_all) == 2

    result1 = metastore.search({'key': 'val1'})

    assert result1[0]['key'] == 'val1'

    result2 = metastore.search({'key': 'val2'})

    assert result2[0]['key'] == 'val2'


def test_lowercase_add_search(metastore):
    """Check that keys are stored in lower case, and
    converted to lower case for searches.
    """
    metastore.add({"KEY": 1})
    metastore.add({"key": 2})

    result1 = metastore.search({"key": 1})

    assert len(result1) == 1

    result2 = metastore.search({"KEY": 2})

    assert len(result2) == 1


def test_surface_metastore(surface_metastore):
    """Check if we can use the metastore with a non-empty
    data type.
    """
    surface_metastore.add({"key1": "val1"})
    result = surface_metastore.search()

    assert len(result) == 1

    assert result[0]['key1'] == 'val1'


def test_multiple_metastores(metastore, surface_metastore):
    """Check that metastores with different data types do not
    interact.
    """
    metastore.add({"key": 1})
    surface_metastore.add({"key": 1})

    res1 = metastore.search()

    assert len(res1) == 1

    res2 = surface_metastore.search()

    assert len(res2) == 1


def test_read_only_metastore(tmp_path):
    """Check that adding to a metastore opened with
    a read-only TinyDB raises a MetaStoreError
    """
    bucket = str(tmp_path)
    with pytest.raises(MetastoreError):
        with load_metastore(bucket=bucket, key='', mode='r') as session:
            read_only_metastore = TinyDBMetaStore(
                bucket=bucket,
                session=session,
            )
            read_only_metastore.add({"key": "val"})


def test_delete(metastore):
    """Check if we can delete data."""
    metastore.add({"key": 123})
    metastore.delete({"key": 123})

    results = metastore.search()

    assert len(results) == 0


def test_delete_multiple_raises_error_by_default(metastore):
    """By default, the TinyDBMetaStore will throw an error if multiple
    records will be deleted.
    """
    metastore.add({"key": 123})
    metastore.add({"key": 123})

    with pytest.raises(MetastoreError):
        metastore.delete({"key": 123})


def test_delete_multiple(metastore):
    """Deleting multiple records is possible if we say we set
    `delete_one` to `False`.
    """
    metastore.add({"key": 123})
    metastore.add({"key": 123})

    metastore.delete({"key": 123}, delete_one=False)

    results = metastore.search()

    assert len(results) == 0
