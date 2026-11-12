import tempfile

import pytest
from openghg.objectstore import (
    get_bucket,
    get_object_data_path,
    get_object_lock_path,
    get_user_objectstore_path,
    get_writable_bucket,
)
from openghg.types import ObjectStoreError


def test_get_bucket_tutorial_store_works(monkeypatch):
    bucket = get_bucket()
    tmpdir = tempfile.gettempdir()
    assert tmpdir in bucket

    monkeypatch.setenv("OPENGHG_TUT_STORE", "1")

    bucket = get_bucket()
    assert "tutorial_store" in bucket


def test_writable_bucket_no_name_given_raises():
    with pytest.raises(ObjectStoreError):
        get_writable_bucket()


def test_get_writable_buckets():
    user_store = get_user_objectstore_path()

    bucket = get_writable_bucket(name="user")

    tmpdir = tempfile.gettempdir()
    assert tmpdir in bucket

    assert bucket == str(user_store)

    with pytest.raises(ObjectStoreError):
        get_writable_bucket()

    group_path = get_writable_bucket(name="group")
    assert tmpdir in group_path
    assert "group" in group_path

    with pytest.raises(ObjectStoreError):
        get_writable_bucket(name="shared")


def test_get_writable_buckets_no_writable(mocker):
    mock_config = {
        "user_id": "test-id-123",
        "config_version": "2",
        "object_store": {
            "user": {
                "path": "/tmp/mock_path",
                "permissions": "r",
            },
        },
    }

    mocker.patch("openghg.objectstore._local_store.read_local_config", return_value=mock_config)

    with pytest.raises(ObjectStoreError):
        get_writable_bucket()


def test_get_bucket():
    tmpdir = tempfile.gettempdir()
    b = get_bucket()
    assert tmpdir in b


def test_custom_user_store_preserves_location_and_rejects_local_helpers(monkeypatch):
    location = "custom://server/zone/catalog"
    monkeypatch.setattr(
        "openghg.objectstore._local_store.read_local_config",
        lambda: {
            "object_store": {
                "user": {"path": location, "permissions": "rw", "factory": "example.backend:factory"}
            }
        },
    )
    assert get_bucket() == get_bucket("user") == get_writable_bucket("user") == location
    with pytest.raises(ObjectStoreError, match="requires a local user object store"):
        get_user_objectstore_path()
    monkeypatch.setenv("OPENGHG_TUT_STORE", "1")
    with pytest.raises(ObjectStoreError, match="requires a local user object store"):
        get_bucket()


def test_get_object_lock_path(tmp_path):
    bucket = str(tmp_path)
    key1 = "key1"
    obj_path = tmp_path / (key1 + "._data")
    obj_path.touch()

    data_path = get_object_data_path(bucket, key1)
    lock_path = get_object_lock_path(bucket, key1)

    assert data_path.stem == lock_path.stem.split(".")[0]
    assert data_path.suffix == lock_path.suffixes[0]
    assert lock_path.suffixes[1] == ".lock_v2"

    # test getting lock even if file doesn't exist yet
    key2 = "key2"
    lock_path = get_object_lock_path(bucket, key2)
