"""Rejected legacy writes must preserve descriptors as well as saved payloads."""

from copy import deepcopy

import pandas as pd
import pytest
import xarray as xr

from openghg.dataobjects import DataManager
from openghg.objectstore import locking_object_store
from openghg.types import ObjectStoreError


@pytest.fixture
def publication(tmp_path):
    data = xr.Dataset(
        {"mf": ("time", [1.0, 2.0])},
        coords={"time": pd.date_range("2020-01-01", periods=2, freq="D")},
    )
    with locking_object_store(str(tmp_path), "surface") as store:
        uuid = store.create(
            metadata={"site": "original", "data_type": "surface", "tags": ["original"]},
            data=data,
            period="1D",
        )
        source = store.get_datasource(uuid)
        with source.begin_edit() as edit:
            edit.update_attributes(to_update={"comment": "committed"})
            edit.commit()
    return str(tmp_path), uuid, data


@pytest.mark.parametrize("operation", ["update", "delete", "manager_delete", "manager_attributes"])
def test_legacy_rejection_preserves_catalogue_and_saved_data(publication, monkeypatch, operation):
    bucket, uuid, data = publication
    with locking_object_store(bucket, "surface") as store:
        before = deepcopy(store.search(uuid=uuid)[0])
    incoming_metadata = {"site": "modified", "tags": ["modified"]}
    if operation.startswith("manager"):
        monkeypatch.setattr("openghg.dataobjects._datamanager.get_writable_bucket", lambda name: bucket)
        manager = DataManager(metadata={uuid: before}, store="test")
        with pytest.raises(ObjectStoreError, match="immutable"):
            if operation == "manager_delete":
                manager.delete_datasource(uuid)
            else:
                manager.update_attributes(uuid, to_update={"comment": "modified"})
    else:
        with locking_object_store(bucket, "surface") as store:
            with pytest.raises(ObjectStoreError, match="immutable"):
                if operation == "update":
                    store.update(
                        uuid,
                        metadata=incoming_metadata,
                        data=data,
                        keys_to_delete=["data_type"],
                        extend_keys=["tags"],
                        if_exists="combine",
                    )
                else:
                    store.delete(uuid)
    with locking_object_store(bucket, "surface") as store:
        assert store.search(uuid=uuid)[0] == before
        assert incoming_metadata == {"site": "modified", "tags": ["modified"]}
        source = store.get_datasource(uuid)
        assert source.latest_version == "v2"
        assert source.get_data().attrs == {"comment": "committed"}
        xr.testing.assert_equal(source.get_data("v1").mf, data.mf)
        xr.testing.assert_equal(source.get_data().mf, data.mf)


def test_descriptor_only_update_remains_supported(publication):
    bucket, uuid, data = publication
    with locking_object_store(bucket, "surface") as store:
        store.update(uuid, metadata={"site": "corrected"})
        assert store.search(uuid=uuid)[0]["site"] == "corrected"
        source = store.get_datasource(uuid)
        assert source.latest_version == "v2"
        xr.testing.assert_equal(source.get_data().mf, data.mf)


def test_unknown_version_read_does_not_create_a_phantom_version(publication):
    bucket, uuid, _ = publication
    with locking_object_store(bucket, "surface") as store:
        source = store.get_datasource(uuid)
        before = source.all_data_keys()
        with pytest.raises(KeyError, match="Invalid version"):
            source.data_keys("v999")
        assert source.all_data_keys() == before
        with source.begin_edit() as edit:
            edit.update_attributes(to_update={"comment": "next"})
            assert edit.commit() == "v3"
        reloaded = store.get_datasource(uuid)
        assert set(reloaded.all_data_keys()) == {"v1", "v2", "v3"}


@pytest.mark.parametrize("operation", ["update", "delete"])
def test_read_only_payload_handle_rejected_before_catalogue_change(publication, monkeypatch, operation):
    bucket, uuid, data = publication
    with locking_object_store(bucket, "surface") as store:
        before = deepcopy(store.search(uuid=uuid)[0])
        source = store.get_datasource(uuid)
        source._mode = "r"
        monkeypatch.setattr(store, "get_datasource", lambda uuid: source)
        with pytest.raises(PermissionError, match="read-only"):
            if operation == "update":
                store.update(uuid, metadata={"site": "modified"}, data=data)
            else:
                store.delete(uuid)
        assert store.search(uuid=uuid)[0] == before
