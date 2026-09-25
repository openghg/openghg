"""Local publication and payload-lifetime checks for explicit datasource edits."""

from copy import deepcopy
from pathlib import Path

import pandas as pd
import pytest
import xarray as xr

from openghg.objectstore import Datasource
from openghg.types import ObjectStoreError, ZarrStoreError


@pytest.fixture
def dataset():
    return xr.Dataset(
        {"mf": ("time", [1.0, 2.0])},
        coords={"time": pd.date_range("2020-01-01", periods=2, freq="h")},
        attrs={"source": "original"},
    )


@pytest.fixture
def datasource(tmp_path, dataset):
    datasource = Datasource(str(tmp_path), "local-edit", data_type="surface")
    datasource.add_data({"sampling_period": "3600"}, dataset, data_type="surface")
    datasource.save()
    return datasource


def test_edit_copies_once_and_publishes_stable_reference(datasource, dataset, monkeypatch):
    from openghg.storage import _zarr_copy

    calls = []
    copy_store = _zarr_copy.copy_zarr_store

    def record_copy(*args, **kwargs):
        calls.append(args)
        return copy_store(*args, **kwargs)

    monkeypatch.setattr(_zarr_copy, "copy_zarr_store", record_copy)
    old_reader = datasource.get_data("v1")
    with datasource.begin_edit() as edit:
        reference = edit._payload.reference
        edit.update(dataset.assign(mf=dataset.mf + 10))
        edit.update_attributes(to_update={"source": "corrected"})
        preview = edit.get_data()
        assert datasource.latest_version == "v1"
        assert edit.commit("correction") == "v2"

    assert len(calls) == 1
    assert datasource._version_paths == {"v1": "v1", "v2": reference}
    assert (datasource._stores_path / reference).exists()
    assert preview.mf.values.tolist() == [11.0, 12.0]
    xr.testing.assert_identical(old_reader, dataset)
    reloaded = Datasource.load(datasource.uuid, datasource._bucket)
    assert reloaded.latest_version == "v2"
    assert reloaded.get_data().attrs["source"] == "corrected"
    assert reloaded.get_data().mf.values.tolist() == [11.0, 12.0]
    xr.testing.assert_identical(reloaded.get_data("v1"), dataset)


def test_payload_copies_store_configuration(datasource):
    datasource._store.index_options = {"method": "nearest"}
    datasource._store.encoding = {"mf": {"chunks": (2,)}}
    payload = datasource._begin_payload_edit("v1")
    assert payload.store.append_dim == datasource._store.append_dim
    assert payload.store.index_options == datasource._store.index_options
    assert payload.store.encoding == datasource._store.encoding
    assert payload.store.to_zarr_kwargs == datasource._store.to_zarr_kwargs
    payload.store.index_options.clear()
    payload.store.encoding["mf"]["chunks"] = (1,)
    assert datasource._store.index_options == {"method": "nearest"}
    assert datasource._store.encoding == {"mf": {"chunks": (2,)}}
    payload.abort()
    assert not (datasource._stores_path / payload.reference).exists()


def test_unpublished_store_is_not_a_readable_base(datasource):
    datasource._store.create_version("v999", checkout=True, copy_current=True)
    with pytest.raises(ZarrStoreError, match="v999"):
        datasource.get_data("v999")
    with pytest.raises(ValueError, match="base version"):
        datasource.begin_edit("v999")


def test_failed_copy_cleans_unpublished_generation(datasource, monkeypatch):
    from openghg.storage import _zarr_copy

    copy_store = _zarr_copy.copy_zarr_store

    def fail_after_copy(*args, **kwargs):
        copy_store(*args, **kwargs)
        raise OSError("copy interrupted")

    monkeypatch.setattr(_zarr_copy, "copy_zarr_store", fail_after_copy)
    with pytest.raises(OSError, match="interrupted"):
        datasource._begin_payload_edit("v1")
    assert not (datasource._stores_path / ".generations").exists()
    assert datasource.latest_version == "v1"


def test_stale_writer_and_stale_legacy_handle_cannot_change_publication(datasource, dataset):
    stale = Datasource.load(datasource.uuid, datasource._bucket)
    with datasource.begin_edit() as edit:
        edit.update(dataset.assign(mf=dataset.mf + 1))
        edit.commit("first writer")

    with pytest.raises(ObjectStoreError, match="reload"):
        stale.begin_edit()
    for mutation in (
        stale.save,
        lambda: stale.add_metadata({"site": "changed"}),
        lambda: stale.add_metadata_key("site", "changed"),
        lambda: stale.add(dataset, if_exists="new", new_version=False),
        lambda: stale.update_attributes(to_update={"source": "changed"}),
        lambda: stale.delete_version("v1"),
        stale.delete_all_data,
    ):
        with pytest.raises(ObjectStoreError, match="immutable.*begin_edit"):
            mutation()
    xr.testing.assert_identical(stale.get_data("v1"), dataset)
    assert Datasource.load(datasource.uuid, datasource._bucket).latest_version == "v2"


def test_failed_publication_retains_payload_and_committed_state(datasource, dataset, monkeypatch):
    before = datasource._edit_state()
    edit = datasource.begin_edit()
    edit.update(dataset.assign(mf=dataset.mf + 1))
    reference = edit._payload.reference

    def fail(state):
        raise OSError("publication acknowledgement lost")

    monkeypatch.setattr(datasource, "_write_state", fail)
    with pytest.raises(OSError, match="acknowledgement"):
        edit.commit("uncertain publication")
    assert (datasource._stores_path / reference).exists()
    assert datasource.latest_version == before["_latest_version"]
    assert datasource._data_keys == before["_data_keys"]
    assert Datasource.load(datasource.uuid, datasource._bucket).latest_version == "v1"
    with pytest.raises(ObjectStoreError, match="reload"):
        datasource.begin_edit()


def test_commit_retains_configuration_for_following_edits(datasource, dataset):
    datasource._store.index_options = {"method": "nearest"}
    datasource._store.encoding = {"mf": {"chunks": (2,)}}
    with datasource.begin_edit() as edit:
        edit.update_attributes(to_update={"note": "first"})
        edit.commit()
    with datasource.begin_edit(base=None) as edit:
        assert edit._payload.store.index_options == {"method": "nearest"}
        assert edit._payload.store.encoding == {"mf": {"chunks": (2,)}}
        edit.append(dataset)
        edit.commit()


def test_immutable_catalog_and_metadata_are_defensive_copies(datasource):
    with datasource.begin_edit() as edit:
        edit.update_attributes(to_update={"description": "saved"})
        edit.commit()
    state = datasource._edit_state()
    assert state["_metadata"]["versions"] is not state["_data_keys"]
    snapshot = deepcopy(datasource._data_keys)
    datasource.metadata["versions"].clear()
    datasource.data_keys().clear()
    datasource.all_data_keys().clear()
    assert datasource._data_keys == snapshot
    assert datasource.metadata["versions"] == snapshot


def test_failed_atomic_state_replace_preserves_previous_document(datasource, monkeypatch):
    before = datasource._read_state(datasource._bucket, datasource.uuid)

    def fail_replace(self, destination):
        raise OSError("replace failed")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        datasource._write_state({**before, "_latest_version": "invalid"})
    assert datasource._read_state(datasource._bucket, datasource.uuid) == before
    document_dir = Path(datasource._bucket, datasource.key).parent
    assert sorted(p.name for p in document_dir.iterdir()) == [datasource.uuid + "._data"]


def test_atomic_state_writes_respect_permissions(datasource, tmp_path):
    reference = tmp_path / "umask-reference"
    reference.write_text("ordinary new file")
    state_path = Path(datasource._bucket, datasource.key + "._data")
    assert state_path.stat().st_mode & 0o777 == reference.stat().st_mode & 0o777
    state_path.chmod(0o600)
    datasource._write_state(datasource._edit_state())
    assert state_path.stat().st_mode & 0o777 == 0o600


def test_readonly_state_and_metadata_are_not_written(datasource):
    readonly = Datasource.load(datasource.uuid, datasource._bucket, mode="r")
    for mutation in (
        readonly.save,
        lambda: readonly.add_metadata({"site": "changed"}),
        lambda: readonly.add_metadata_key("site", "changed"),
    ):
        with pytest.raises(PermissionError, match="read-only"):
            mutation()
    with readonly:
        readonly.get_data()


def test_aborted_opt_in_leaves_legacy_deletion_usable(datasource):
    with datasource.begin_edit():
        pass
    datasource.delete_all_data()
    assert not datasource._stores_path.exists()


def test_datasource_context_requires_explicit_commit(datasource):
    with datasource:
        edit = datasource.begin_edit()
        edit.update_attributes(to_update={"note": "discarded"})
    assert datasource._active_edit is None
    assert datasource.latest_version == "v1"
    assert "note" not in datasource.get_data().attrs

    with datasource:
        with datasource.begin_edit() as edit:
            edit.update_attributes(to_update={"note": "saved"})
            edit.commit()
    assert datasource.get_data().attrs["note"] == "saved"
