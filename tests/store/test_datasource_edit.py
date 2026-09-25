"""Public editing semantics, independently of ingestion's legacy flags."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from openghg.objectstore import Datasource
from openghg.storage._zarr_compat import zarr_has_async_store_api
from openghg.types import DataOverlapError, ObjectStoreError, UpdateError


def observations(hours, values=None, **attrs):
    hours = list(hours)
    return xr.Dataset(
        {"mf": ("time", values if values is not None else np.asarray(hours, dtype=float))},
        coords={"time": pd.Timestamp("2020-01-01") + pd.to_timedelta(hours, unit="h")},
        attrs=attrs,
    )


@pytest.fixture(params=[2, 3])
def source(tmp_path, request):
    if request.param == 3 and not zarr_has_async_store_api():
        pytest.skip("Format 3 requires zarr-python 3")
    result = Datasource(str(tmp_path), "editing", data_type="surface")
    result._store.to_zarr_kwargs = {"zarr_format": request.param}
    result.add_metadata({"period": "1h"})
    result.add(observations(range(4), title="original", preserved="yes"))
    result.save()
    return result


def test_batch_is_invisible_until_commit_and_preserves_history(source):
    old = source.get_data("v1")
    with source.begin_edit() as edit:
        edit.append(observations([4, 5]))
        edit.update(observations([1, 3], [11.0, 33.0], title="corrected"))
        edit.upsert(observations([2, 6], [np.nan, 66.0]))
        assert source.latest_version == "v1"
        assert list(source.all_data_keys()) == ["v1"]
        xr.testing.assert_identical(source.get_data().compute(), old.compute())
        preview = edit.get_data()
        assert edit.commit("Batch import and corrections") == "v2"

    current = source.get_data().compute()
    np.testing.assert_allclose(current.mf, [0, 11, np.nan, 33, 4, 5, 66])
    assert current.attrs == {"title": "corrected", "preserved": "yes"}
    xr.testing.assert_identical(preview.compute(), current)
    np.testing.assert_allclose(old.mf.compute(), [0, 1, 2, 3])
    reloaded = Datasource.load(source.uuid, source._bucket)
    xr.testing.assert_identical(reloaded.get_data("v1").compute(), old.compute())
    xr.testing.assert_identical(reloaded.get_data().compute(), current)
    assert reloaded._commits["v2"]["parent"] == "v1"
    assert reloaded._commits["v2"]["message"] == "Batch import and corrections"


@pytest.mark.parametrize("operation", ["append", "update", "upsert", "replace", "attributes"])
def test_exit_without_commit_discards_all_operations(source, operation):
    original = source.get_data().compute()
    with source.begin_edit() as edit:
        if operation == "append":
            edit.append(observations([4]))
        elif operation == "update":
            edit.update(observations([1], [111.0]))
        elif operation == "upsert":
            edit.upsert(observations([1, 4]))
        elif operation == "replace":
            edit.replace(observations([10]))
        else:
            edit.update_attributes(to_update={"title": "new"})
    assert source.latest_version == "v1"
    assert not hasattr(source, "_versioning_policy") or source._versioning_policy != "immutable"
    xr.testing.assert_identical(Datasource.load(source.uuid, source._bucket).get_data().compute(), original)


@pytest.mark.parametrize("operation,error", [("append", DataOverlapError), ("update", UpdateError)])
def test_invalid_operation_discards_entire_batch(source, operation, error):
    with source.begin_edit() as edit:
        edit.append(observations([4]))
        with pytest.raises(error):
            getattr(edit, operation)(observations([0 if operation == "append" else 9]))
        with pytest.raises(RuntimeError, match="closed"):
            edit.commit()
    np.testing.assert_allclose(source.get_data().mf, [0, 1, 2, 3])
    assert source.latest_version == "v1"


def test_replace_can_consume_lazy_working_preview(source):
    with source.begin_edit() as edit:
        preview = edit.get_data().isel(time=slice(1, 3))
        edit.replace(preview.assign(mf=preview.mf * 10))
        edit.commit()
    np.testing.assert_allclose(source.get_data().mf, [10, 20])
    np.testing.assert_allclose(source.get_data("v1").mf, [0, 1, 2, 3])


@pytest.mark.parametrize("operation", ["update", "upsert"])
def test_noncontiguous_swap_reads_preview_before_overwriting_source(source, operation):
    import dask

    with source.begin_edit() as edit:
        # One point per chunk makes the second source read occur after the first
        # region write unless the editor forks the preview's working generation.
        edit.replace(observations(range(4)).chunk(time=1))
        preview = edit.get_data()
        swapped = preview.isel(time=[3, 0]).assign_coords(time=preview.time.isel(time=[0, 3]))
        with dask.config.set(scheduler="synchronous"):
            getattr(edit, operation)(swapped)
            edit.commit()
    np.testing.assert_allclose(source.get_data().mf, [3, 1, 2, 0])


def test_failed_preview_cleanup_closes_editor_without_publishing(source):
    edit = source.begin_edit()
    preview = edit.get_data()

    def fail_cleanup():
        raise OSError("Working generation cleanup failed")

    edit._payload._abort = fail_cleanup
    with pytest.raises(OSError, match="cleanup failed"):
        edit.update(preview.assign(mf=preview.mf + 10))
    with pytest.raises(RuntimeError, match="closed"):
        edit.commit()
    assert source.latest_version == "v1"
    np.testing.assert_allclose(source.get_data().mf, [0, 1, 2, 3])


def test_attribute_only_commit_is_versioned(source):
    with source.begin_edit() as edit:
        edit.update_attributes(to_update={"units": "ppb"}, data_vars="mf", update_global=False)
        edit.update_attributes(to_delete="title", to_update={"comment": "Reviewed"})
        edit.commit("Attribute correction")
    latest = source.get_data()
    assert latest.mf.attrs["units"] == "ppb"
    assert latest.attrs == {"preserved": "yes", "comment": "Reviewed"}
    assert source.get_data("v1").attrs["title"] == "original"
    assert "units" not in source.get_data("v1").mf.attrs


def test_historical_read_does_not_change_default_edit_base(source):
    with source.begin_edit() as edit:
        edit.append(observations([4]))
        edit.commit()
    source.get_data("v1")
    with source.begin_edit() as edit:
        assert edit.base == "v2"
        edit.append(observations([5]))
        assert edit.commit() == "v3"
    np.testing.assert_allclose(source.get_data().mf, range(6))


def test_explicit_old_or_empty_base_still_creates_new_version(source):
    with source.begin_edit() as edit:
        edit.append(observations([4]))
        edit.commit()
    with source.begin_edit(base="v1") as edit:
        edit.update(observations([1], [11.0]))
        assert edit.commit() == "v3"
    np.testing.assert_allclose(source.get_data().mf, [0, 11, 2, 3])
    with source.begin_edit(base=None) as edit:
        edit.upsert(observations([10]))
        assert edit.commit() == "v4"
    np.testing.assert_allclose(source.get_data().mf, [10])


def test_untouched_editor_does_not_create_version_or_enable_policy(source):
    with source.begin_edit() as edit:
        edit.update_attributes()
        with pytest.raises(ValueError, match="no staged changes"):
            edit.commit()
    source.add(observations([4]), new_version=False)
    source.save()
    assert source.latest_version == "v1"


def test_immutable_policy_blocks_all_legacy_mutation_paths_and_stale_handles(source):
    stale = Datasource.load(source.uuid, source._bucket)
    with source.begin_edit() as edit:
        edit.append(observations([4]))
        edit.commit()
    for handle in [source, stale, Datasource.load(source.uuid, source._bucket)]:
        for mutate in [
            lambda: handle.add(observations([5]), new_version=False),
            lambda: handle.add_data({}, observations([5]), "surface"),
            lambda: handle.add_timed_data(observations([5]), "surface", False, False),
            lambda: handle.update_attributes(to_update={"title": "bypass"}),
            lambda: handle.add_metadata({"site": "bypass"}),
            lambda: handle.add_metadata_key("site", "bypass"),
            handle.save,
        ]:
            with pytest.raises(ObjectStoreError, match="immutable"):
                mutate()
    np.testing.assert_allclose(source.get_data("v1").mf, [0, 1, 2, 3])


def test_stale_editor_cannot_commit_or_modify_saved_generation(source):
    other = Datasource.load(source.uuid, source._bucket)
    stale_edit = other.begin_edit()
    stale_edit.append(observations([5]))
    with source.begin_edit() as edit:
        edit.append(observations([4]))
        edit.commit()
    with pytest.raises(ObjectStoreError, match="[Ss]tale|changed|reload"):
        stale_edit.commit()
    stale_edit.abort()
    latest = Datasource.load(source.uuid, source._bucket)
    np.testing.assert_allclose(latest.get_data().mf, [0, 1, 2, 3, 4])


def test_failed_publication_keeps_previous_committed_state(source, monkeypatch):
    original = source.get_data().compute()

    def fail(state):
        raise OSError("Publication unavailable")

    monkeypatch.setattr(source, "_write_state", fail)
    with source.begin_edit() as edit:
        edit.append(observations([4]))
        with pytest.raises(OSError, match="unavailable"):
            edit.commit()
    assert source.latest_version == "v1"
    assert list(source.all_data_keys()) == ["v1"]
    reloaded = Datasource.load(source.uuid, source._bucket)
    xr.testing.assert_identical(reloaded.get_data().compute(), original)


def test_readonly_cannot_start_edit(source):
    readonly = Datasource.load(source.uuid, source._bucket, mode="r")
    with pytest.raises(PermissionError):
        readonly.begin_edit()
