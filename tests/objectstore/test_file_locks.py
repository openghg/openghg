from threading import Thread
from timeit import default_timer

from openghg.objectstore.metastore import DataClassMetaStore
import pytest


@pytest.fixture
def get_metastores(tmp_path, mocker):
    bucket = str(tmp_path)
    key = "key"
    mocker.patch("openghg.objectstore.metastore._classic_metastore.get_metakey", return_value=key)

    ms1 = DataClassMetaStore(bucket, key)
    ms2 = DataClassMetaStore(bucket, key)

    yield ms1, ms2

    ms1.close()
    ms2.close()


def test_lock(get_metastores):
    """Check that a second DataClassMetaStore instance
    must wait to acquire a lock if the metastore is already
    locked.
    """
    ms1, ms2 = get_metastores
    expected_duration = 2.0

    ms1.acquire_lock()

    # attempt to acquire lock, but give up
    # if it takes longer than 'expected_duration'
    t = Thread(target=ms2.acquire_lock)
    start = default_timer()
    t.start()
    t.join(timeout=expected_duration)
    duration = default_timer() - start

    assert duration >= expected_duration


def test_no_lock(get_metastores):
    """Check that a second DataClassMetaStore instance
    doesn't need to wait to acquire a lock if the metastore
    is *not* already locked.
    """
    ms1, ms2 = get_metastores
    expected_duration = 2.0

    t = Thread(target=ms2.acquire_lock)
    start = default_timer()
    t.start()
    t.join(timeout=expected_duration)
    duration = default_timer() - start

    assert duration < expected_duration
