"""Store-bound writable handles and their explicit editing lifetime."""

from contextlib import contextmanager
from types import ModuleType, SimpleNamespace
from uuid import uuid4
import sys

import pandas as pd
import pytest
import xarray as xr

from openghg.dataobjects import DataManager, data_manager
from openghg.objectstore import get_writable_bucket, locking_object_store
from openghg.types import ObjectStoreError


@pytest.fixture
def dataset():
    return xr.Dataset(
        {"mf": ("time", [1.0, 2.0, 3.0])},
        coords={"time": pd.date_range("2020-01-01", periods=3, freq="D")},
        attrs={"title": "original"},
    )


@pytest.fixture
def stored_datasource(dataset):
    """Publish the same UUID in two stores with distinct data and descriptors."""
    uuid = str(uuid4())
    for name, offset in [("user", 0), ("group", 100)]:
        bucket = get_writable_bucket(name)
        with locking_object_store(bucket, "surface") as store:
            ds = store.datasource_factory.new(uuid)
            ds.add(dataset + offset, period="1D")
            ds.save()
            store.metastore.insert(
                {"uuid": uuid, "data_type": "surface", "site": "handle-test", "owner": name}
            )
    return uuid


def test_manager_datasource_is_bound_to_selected_store(stored_datasource, dataset):
    uuid = stored_datasource
    manager = data_manager(data_type="surface", store="user", uuid=uuid)
    with manager.datasource(uuid) as ds:
        assert "owner" not in ds.metadata  # Search descriptors must not become datasource state.
        with ds.begin_edit() as edit:
            edit.update(dataset.assign(mf=dataset.mf + 10))
            assert edit.commit(message="Correct calibration") == "v2"
    assert manager.metadata[uuid]["owner"] == "user"
    assert manager.metadata[uuid]["latest_version"] == "v2"
    assert "object_store" not in manager.metadata[uuid]
    with manager.datasource(uuid) as reloaded:
        assert "owner" not in reloaded.metadata
        xr.testing.assert_equal(reloaded.get_data("v1").mf, dataset.mf)
        xr.testing.assert_equal(reloaded.get_data().mf, dataset.mf + 10)
    other = data_manager(data_type="surface", store="group", uuid=uuid)
    with other.datasource(uuid) as ds:
        assert ds.latest_version == "v1"
        xr.testing.assert_equal(ds.get_data().mf, dataset.mf + 100)


@pytest.mark.parametrize("exception", [False, True])
def test_manager_exit_discards_pending_editor(stored_datasource, dataset, exception):
    manager = data_manager(data_type="surface", store="user", uuid=stored_datasource)

    @contextmanager
    def expected_exit():
        if exception:
            with pytest.raises(ValueError, match="caller failed"):
                yield
        else:
            yield

    with expected_exit():
        with manager.datasource(stored_datasource) as ds:
            editor = ds.begin_edit()
            editor.update(dataset.assign(mf=dataset.mf + 20))
            if exception:
                raise ValueError("caller failed")
    assert ds._active_edit is None
    with pytest.raises(RuntimeError):
        editor.commit()
    with pytest.raises(RuntimeError, match="Reopen"):
        ds.begin_edit()
    with pytest.raises(RuntimeError, match="Reopen"):
        ds.save()
    xr.testing.assert_equal(ds.get_data().mf, dataset.mf)
    with manager.datasource(stored_datasource) as reloaded:
        assert reloaded.latest_version == "v1"
        xr.testing.assert_equal(reloaded.get_data().mf, dataset.mf)


def test_lazy_saved_read_survives_context_exit_and_later_commit(stored_datasource, dataset):
    manager = data_manager(data_type="surface", store="user", uuid=stored_datasource)
    with manager.datasource(stored_datasource) as ds:
        with ds.begin_edit() as edit:
            edit.update_attributes(to_update={"review": "first"})
            edit.commit()
        lazy = ds.get_data()
    assert lazy.mf.chunks is not None
    with manager.datasource(stored_datasource) as current:
        with current.begin_edit() as edit:
            edit.update(dataset.assign(mf=dataset.mf + 10))
            edit.commit()
    xr.testing.assert_equal(lazy.load().mf, dataset.mf)
    xr.testing.assert_equal(ds.get_data().load().mf, dataset.mf)
    with pytest.raises(RuntimeError, match="Reopen"):
        ds.begin_edit()


def test_manager_requires_selected_single_uuid(stored_datasource):
    manager = data_manager(data_type="surface", store="user", uuid=stored_datasource)
    with pytest.raises(ValueError, match="Invalid UUID"):
        with manager.datasource("not-selected"):
            pytest.fail("Unselected UUID was accepted")
    with pytest.raises(ValueError, match="single"):
        with manager.datasource([stored_datasource]):
            pytest.fail("Multiple UUIDs were accepted")


def test_manager_rejects_read_only_store():
    with pytest.raises(ObjectStoreError):
        DataManager(metadata={}, store="shared")


def test_manager_dispatches_configured_backend_and_refreshes_only_selected_uuid(monkeypatch):
    """Exercise the real factory dispatch without requiring a remote service."""
    calls = []
    raw = SimpleNamespace(_active_edit=None, metadata={"raw": True})
    uuid = "selected"
    manager = DataManager(
        metadata={uuid: {"data_type": "surface"}, "other": {"data_type": "surface", "keep": True}},
        store="user",
    )

    class Backend:
        def __enter__(self):
            calls.append("enter")
            return self

        def __exit__(self, *exc):
            calls.append("exit")

        def get_datasource(self, uuid):
            assert calls[-1] == "enter"
            calls.append(("load", uuid))
            return raw

        def search(self, uuid):
            calls.append(("search", uuid))
            return [{"uuid": uuid, "data_type": "surface", "latest_version": "v9", "object_store": "remote"}]

    def factory(**kwargs):
        calls.append(kwargs)
        return Backend()

    module = ModuleType("test_datasource_backend")
    module.factory = factory
    monkeypatch.setitem(sys.modules, module.__name__, module)
    monkeypatch.setattr(
        "openghg.objectstore._factory.get_object_store_config",
        lambda bucket: {"factory": "test_datasource_backend:factory", "permissions": "rw"},
    )
    with manager.datasource(uuid) as ds:
        assert ds is raw
        ds._edit_context_guard()
        assert ds.metadata == {"raw": True}
    assert calls == [
        {
            "bucket": manager._bucket,
            "data_type": "surface",
            "mode": "rw",
            "skip_keys": None,
            "extend_keys": None,
        },
        "enter",
        ("load", uuid),
        ("search", uuid),
        "exit",
    ]
    assert manager.metadata[uuid]["latest_version"] == "v9"
    assert manager.metadata["other"] == {"data_type": "surface", "keep": True}
    assert "object_store" not in manager.metadata[uuid]
    with pytest.raises(RuntimeError, match="Reopen"):
        raw._edit_context_guard()
