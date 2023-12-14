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

    # attempt to acquire lock, but give up if
    # it takes longer than 'expected_duration'
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


def test_lock_is_advisory(tmp_path, mocker):
    """Check that the metastore can be modified by a second metastore
    instance even if it is locked by a first metastore instance.

    Thus, only acquiring a lock is stopped by the filelock. This means
    that the lock is "advisory", and must be opted into explicitly in
    our code.

    NOTE: due to the cache in SafetyCachingMiddleware
    """
    bucket = str(tmp_path)
    key = "key1"
    mocker.patch("openghg.objectstore.metastore._classic_metastore.get_metakey", return_value=key)

    ms1 = DataClassMetaStore(bucket, key)
    ms1.insert({"key": "val"})
    ms1.close()  # need to call close to write due to SafetyCachingMiddleware
    ms1.acquire_lock()  # this is actually still possible...

    assert len(ms1._db.all()) == 1

    ms2 = DataClassMetaStore(bucket, key)
    ms2.delete({"key": "val"})
    ms2.close()  # commit changes

    ms1.release_lock()

    # now check length (with new instance to be sure
    # the metastore is read from disk)
    ms3 = DataClassMetaStore(bucket, key)

    assert len(ms3._db.all()) == 0
    ms3.close()
