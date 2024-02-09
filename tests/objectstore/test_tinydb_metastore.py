"""
Tests for the TinyDB based metastore.

It should be possible to use these tests with any metastore,
provided the fixtures are changed.
"""
import pytest
import tinydb

from openghg.objectstore.metastore import TinyDBMetaStore
from openghg.types import MetastoreError


@pytest.fixture
def metastore(tmp_path):
    """Open metastore with no data type.

    Note: `tmp_path` is function scope, so the metastore is
    reset for each test that uses this fixture.
    """
    filename = str(tmp_path / 'metastore._data')
    with tinydb.TinyDB(filename) as database:
        metastore = TinyDBMetaStore(database=database)
        yield metastore


@pytest.fixture
def alternate_metastore(tmp_path):
    """Open metastore with a different filepath.

    This will be used to show that two different
    metastore will not interact.
    """
    filename = str(tmp_path / 'alternate_metastore._data')
    with tinydb.TinyDB(filename) as database:
        metastore = TinyDBMetaStore(database=database)
        yield metastore


def test_search_empty(metastore):
    results = metastore.search()
    assert results == []


def test_add(metastore):
    metastore.insert({"key1": "val1"})
    result = metastore.search()

    assert len(result) == 1

    assert result[0]['key1'] == 'val1'


def test_search(metastore):
    """Test searching when there are multiple items
    in the metastore.
    """
    metastore.insert({"key": "val1"})
    metastore.insert({"key": "val2"})

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
    metastore.insert({"KEY": 1})
    metastore.insert({"key": 2})

    result1 = metastore.search({"key": 1})

    assert len(result1) == 1

    result2 = metastore.search({"KEY": 2})

    assert len(result2) == 1


def test_select(metastore):
    for i in range(10):
        metastore.insert({'uuid': i, 'key': 'val'})

    results = metastore.select('uuid')

    assert results == list(range(10))


def test_alternate_metastore(alternate_metastore):
    """Check if we can use the metastore with a non-empty
    data type.
    """
    alternate_metastore.insert({"key1": "val1"})
    result = alternate_metastore.search()

    assert len(result) == 1

    assert result[0]['key1'] == 'val1'


def test_multiple_metastores(metastore, alternate_metastore):
    """Check that metastores with different data types do not
    interact.
    """
    metastore.insert({"key": 1})
    alternate_metastore.insert({"key": 1})

    res1 = metastore.search()

    assert len(res1) == 1

    res2 = alternate_metastore.search()

    assert len(res2) == 1


def test_delete(metastore):
    """Check if we can delete data."""
    metastore.insert({"key": 123})
    metastore.delete({"key": 123})

    results = metastore.search()

    assert len(results) == 0


def test_delete_multiple_raises_error_by_default(metastore):
    """By default, the TinyDBMetaStore will throw an error if multiple
    records will be deleted.
    """
    metastore.insert({"key": 123})
    metastore.insert({"key": 123})

    with pytest.raises(MetastoreError):
        metastore.delete({"key": 123})


def test_delete_multiple(metastore):
    """Deleting multiple records is possible if we say we set
    `delete_one` to `False`.
    """
    metastore.insert({"key": 123})
    metastore.insert({"key": 123})

    metastore.delete({"key": 123}, delete_one=False)

    results = metastore.search()

    assert len(results) == 0


def test_overwrite_update(metastore):
    """Test updating an entry by overwriting an existing
    value.
    """
    metastore.insert({"key": 123})
    metastore.update(where={"key": 123}, to_update={"key": 321})

    result = metastore.search()[0]

    assert result["key"] == 321


def test_add_update(metastore):
    """Test updating an entry by adding a key-value pair an
    existing record.
    """
    metastore.insert({"key1": 123})
    metastore.update(where={"key1": 123}, to_update={"key2": 321})

    result = metastore.search()[0]

    assert result["key1"] == 123
    assert result["key2"] == 321


def test_add_and_overwrite_update(metastore):
    """Test updating an entry by overwriting an existing
    value and adding a new key-value pair.
    """
    metastore.insert({"key1": 123})
    metastore.update(where={"key1": 123}, to_update={"key1": 321, "key2": "asdf"})

    result = metastore.search()[0]

    assert result["key1"] == 321
    assert result["key2"] == "asdf"


def test_update_error_if_not_unique(metastore):
    """Check if an error is raised if we try to update
    multiple records at once.
    """
    metastore.insert({"key": 123})
    metastore.insert({"key": 123})

    with pytest.raises(MetastoreError):
        metastore.update(where={"key": 123}, to_update={"key2": 234})
