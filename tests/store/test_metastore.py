import pytest
from openghg.store import load_metastore
from openghg.objectstore import get_writable_bucket, get_object_from_json
from openghg.types import MetastoreError


def test_metastore_read_write_mode():
    bucket = get_writable_bucket(name="user")
    key = "test_readonly_store"
    with load_metastore(bucket=bucket, key=key, mode="rw") as db:
        db.insert({"some_key": "some_value"})

    metastore_data_a = get_object_from_json(bucket=bucket, key=key)

    # Pull out some data in read-only mode
    with load_metastore(bucket=bucket, key=key, mode="r") as db:
        records = [r for r in db]

    assert records == [{'some_key': 'some_value'}]

    with pytest.raises(MetastoreError):
        with load_metastore(bucket=bucket, key=key, mode="r") as db:
            db.insert({"another_key": "some_value"})

    metastore_data_b = get_object_from_json(bucket=bucket, key=key)

    assert metastore_data_a == metastore_data_b

    with pytest.raises(ValueError):
        with load_metastore(bucket=bucket, key=key, mode="a") as db:
            db.insert({"another_key": "some_value"})
