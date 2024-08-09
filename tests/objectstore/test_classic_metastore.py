import pytest
import tinydb
from openghg.objectstore import get_object_from_json
from openghg.objectstore.metastore import open_metastore
from openghg.objectstore.metastore._classic_metastore import SafetyCachingMiddleware
from openghg.types import MetastoreError
from tinydb.storages import JSONStorage


def test_metastore_read_write_mode(tmp_path):
    bucket = str(tmp_path)
    key = "default"  # this is the key used by `open_metastore` if 'data_type' isn't a known data type.
    with open_metastore(bucket=bucket, data_type=key, mode="rw") as metastore:
        metastore.insert({"some_key": "some_value"})

    metastore_data_a = get_object_from_json(bucket=bucket, key=key)

    # Pull out some data in read-only mode
    with open_metastore(bucket=bucket, data_type=key, mode="r") as metastore:
        records = metastore.search()

    assert records == [{"some_key": "some_value"}]

    with pytest.raises(MetastoreError):
        with open_metastore(bucket=bucket, data_type=key, mode="r") as metastore:
            metastore.insert({"another_key": "some_value"})

    metastore_data_b = get_object_from_json(bucket=bucket, key=key)

    assert metastore_data_a == metastore_data_b

    with pytest.raises(ValueError):
        with open_metastore(bucket=bucket, data_type=key, mode="a") as metastore:
            metastore.insert({"another_key": "some_value"})


def test_safety_caching_middleware_cache(tmp_path):
    """Check that SafetyCachingMiddleware only writes once
    database is closed.
    """
    db_file = tmp_path / "test.json"
    with tinydb.TinyDB(db_file, storage=SafetyCachingMiddleware(JSONStorage)) as db:
        db.insert({"some_key": "some_value"})

        # nothing written yet
        with tinydb.TinyDB(db_file) as db2:
            assert len(db2) == 0

    with tinydb.TinyDB(db_file) as db2:
        assert len(db2) == 1


def test_safety_caching_middleware_error(tmp_path):
    """Check that SafetyCachingMiddleware raises and error
    if the database is changed by another process.

    The error will be raised when the database opened with Safety caching
    is closed.
    """
    # Add some data to tinydb
    db_file = tmp_path / "test.json"
    with tinydb.TinyDB(db_file) as db:
        first_item = db.insert({"some_key": "some_value"})

    # show that data won't be saved if tinydb changed before writes happen
    with pytest.raises(MetastoreError):
        with tinydb.TinyDB(db_file, storage=SafetyCachingMiddleware(JSONStorage)) as db:
            db.insert({"another_key": "another_value"})

            # another modification made elsewhere
            with tinydb.TinyDB(db_file) as db2:
                db2.remove(doc_ids=[first_item])


def test_safety_caching_middleware_no_write_no_error(tmp_path):
    """A similar scenario to the error test, but no writes are made, so
    no error is raised.
    """
    # add some data to tinydb
    db_file = tmp_path / "test.json"
    with tinydb.TinyDB(db_file) as db:
        first_item = db.insert({"some_key": "some_value"})

    # unlike the previous test, no error will be raised here
    with tinydb.TinyDB(db_file, storage=SafetyCachingMiddleware(JSONStorage)) as db:
        # make a read
        results = db.all()

        # another modification made elsewhere
        with tinydb.TinyDB(db_file) as db2:
            db2.remove(doc_ids=[first_item])
