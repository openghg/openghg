from openghg.store.base import BaseStore
from openghg.objectstore import get_object_from_json, get_writable_bucket, set_object_from_json


def test_default_metakey_is_class_attribute():
    assert BaseStore.metakey() == ""


def test_legacy_hash_state_is_discarded():
    """Legacy file and retrieval hashes are neither loaded nor persisted."""
    bucket = get_writable_bucket(name="user")
    set_object_from_json(
        bucket=bucket,
        key=BaseStore.key(),
        data={
            "_file_hashes": {"old-file-hash": "old-file.nc"},
            "_retrieved_hashes": {"old-data-hash": {"site": "timestamp"}},
        },
    )

    with BaseStore(bucket=bucket) as base_store:
        assert not hasattr(base_store, "_file_hashes")
        assert not hasattr(base_store, "_retrieved_hashes")

    saved_state = get_object_from_json(bucket=bucket, key=BaseStore.key())
    assert "_file_hashes" not in saved_state
    assert "_retrieved_hashes" not in saved_state
