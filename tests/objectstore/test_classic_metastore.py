import pytest

from openghg.objectstore import get_object_from_json
from openghg.objectstore.metastore import open_metastore
from openghg.types import MetastoreError


def test_metastore_read_write_mode(tmp_path):
    bucket = str(tmp_path)
    key = "default"  # this is the key used by `open_metastore` if 'data_type' isn't a known data type.
    with open_metastore(bucket=bucket, data_type=key, mode="rw") as db:
        db.add({"some_key": "some_value"})

    metastore_data_a = get_object_from_json(bucket=bucket, key=key)

    # Pull out some data in read-only mode
    with open_metastore(bucket=bucket, data_type=key, mode="r") as db:
        records = db.search()

    assert records == [{'some_key': 'some_value'}]

    with pytest.raises(MetastoreError):
        with open_metastore(bucket=bucket, data_type=key, mode="r") as db:
            db.add({"another_key": "some_value"})

    metastore_data_b = get_object_from_json(bucket=bucket, key=key)

    assert metastore_data_a == metastore_data_b

    with pytest.raises(ValueError):
        with open_metastore(bucket=bucket, data_type=key, mode="a") as db:
            db.add({"another_key": "some_value"})
