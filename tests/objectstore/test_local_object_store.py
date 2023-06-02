# from pathlib import Path

import pytest
from openghg.objectstore import (
    get_local_objectstore_path,
    get_bucket,
    get_writable_bucket,
)
from openghg.types import ObjectStoreError
import tempfile


def test_get_bucket_tutorial_store_works(monkeypatch):
    bucket = get_bucket()
    tmpdir = tempfile.gettempdir()
    assert tmpdir in bucket

    monkeypatch.setenv("OPENGHG_TUT_STORE", "1")

    bucket = get_bucket()
    assert "tutorial_store" in bucket


def test_get_writable_bucket(mocker):
    # First use the mocked config file with only a single user bucket
    user_store = get_local_objectstore_path()

    bucket = get_writable_bucket()

    tmpdir = tempfile.gettempdir()
    assert tmpdir in bucket

    assert bucket == str(user_store)

    mock_config = {
        "user_id": "test-id-123",
        "config_version": "2",
        "object_store": {
            "user": {
                "path": str(tmpdir),
                "permissions": "rw",
            },
            "shared": {"path": "/tmp/mock_path", "permissions": "rw"},
        },
    }

    mocker.patch("openghg.objectstore._local_store.read_local_config", return_value=mock_config)

    with pytest.raises(ObjectStoreError):
        get_writable_bucket()

    shared_path = get_writable_bucket(name="shared")
    assert shared_path == "/tmp/mock_path"

    with pytest.raises(ObjectStoreError):
        get_writable_bucket(name="badger")


def test_get_writable_bucket_no_writable(mocker):
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
