"""Package upgrades retain existing observation stores and their permissions."""

import json
import os
from pathlib import Path
import subprocess
from uuid import uuid4

import dask
import pytest
import xarray as xr
from helpers import get_surface_datapath

from openghg.objectstore import Datasource, get_readable_buckets, get_writable_bucket
from openghg.standardise.surface import parse_crds
from openghg.storage import get_versioned_zarr_directory_store
from openghg.types import ObjectStoreError


@pytest.fixture(params=["user", "group", "shared"])
def existing_store(request, mock_configuration_paths):
    """A Zarr 2 TAC datasource in one of conftest's configured test stores.

    The separate interpreter writes the store before the package upgrade. Only
    this fixture's datasource is removed afterwards, preserving other fixtures.
    """
    legacy_python = os.environ.get("OPENGHG_LEGACY_PYTHON")
    if not legacy_python:
        pytest.skip("Set OPENGHG_LEGACY_PYTHON to a Zarr 2.18.3 / xarray 2025.9.1 interpreter")
    name = request.param
    bucket = mock_configuration_paths["object_store"][name]["path"]
    assert get_readable_buckets()[name] == bucket
    filepath = get_surface_datapath("tac.picarro.1minute.100m.test.dat", "CRDS")
    uuid = f"zarr-upgrade-{uuid4()}"
    command = [legacy_python, "-m", "tests.helpers.legacy_zarr_store"]
    subprocess.run(
        [*command, "write", bucket, str(filepath), uuid], check=True, capture_output=True, text=True
    )
    try:
        yield name, bucket, uuid, filepath, command
    finally:
        # Fixture preparation/cleanup acts as the owner of the shared store;
        # consumers below retain its configured read-only permissions.
        Datasource.load(bucket=bucket, uuid=uuid).delete()


def test_existing_store_reads_without_changing_encoded_data(existing_store):
    """All configured stores expose their pre-upgrade values and metadata unchanged."""
    name, bucket, uuid, filepath, _ = existing_store
    root = Path(bucket, "data", uuid, "zarr", "v1")
    before = {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}
    expected = parse_crds(filepath=filepath, site="tac", network="DECC")["ch4"]["data"].isel(
        time=slice(0, 12)
    )
    datasource = Datasource.load(bucket=bucket, uuid=uuid, mode="r")
    actual = datasource.get_data().load()
    xr.testing.assert_equal(actual, expected)
    assert actual.attrs == json.loads((root / ".zattrs").read_text())
    assert datasource._store.bytes_stored() == sum(len(value) for value in before.values())
    assert before == {
        str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()
    }
    assert json.loads((root / ".zgroup").read_text())["zarr_format"] == 2

    if name == "shared":
        with pytest.raises(ObjectStoreError):
            get_writable_bucket(name=name)
        with pytest.raises(PermissionError):
            datasource.add(actual)


def test_existing_writable_store_preserves_versions_after_append_and_correction(existing_store):
    """Real TAC observations survive chunk-sharing writes and remain readable by Zarr 2."""
    name, bucket, uuid, filepath, command = existing_store
    if name == "shared":
        pytest.skip("The configured shared store is read-only")
    assert get_writable_bucket(name=name) == bucket
    datasource = Datasource.load(bucket=bucket, uuid=uuid)
    original = datasource.get_data("v1").load()
    observations = parse_crds(filepath=filepath, site="tac", network="DECC")["ch4"]["data"].isel(
        time=slice(0, 24)
    )
    store = datasource._store
    store.create_version("v2", checkout=True, copy_current=True)
    correction = observations.isel(time=[1, 2, 8, 9]).copy(deep=True)
    for variable in correction.data_vars:
        if observations[variable].dtype.kind == "f":
            correction[variable] = (
                observations[variable].rolling(time=3, min_periods=1).mean().isel(time=[1, 2, 8, 9])
            )
    with dask.config.set(scheduler="threads", num_workers=4):
        store.insert(observations.isel(time=slice(12, None)).chunk(time=3))
        store.update(correction.chunk(time=1))

    expected = observations.copy(deep=True)
    for variable in expected.data_vars:
        expected[variable][dict(time=[1, 2, 8, 9])] = correction[variable]
    xr.testing.assert_equal(store.get(), expected)
    reopened = get_versioned_zarr_directory_store(datasource._stores_path)
    reopened.checkout_version("v1")
    xr.testing.assert_equal(reopened.get(), original)
    reopened.checkout_version("v2")
    xr.testing.assert_equal(reopened.get(), expected)
    assert json.loads((datasource._stores_path / "v2" / ".zgroup").read_text())["zarr_format"] == 2

    subprocess.run(
        [*command, "read", bucket, str(filepath), uuid], check=True, capture_output=True, text=True
    )
    reopened.delete_version("v2")
    reopened.checkout_version("v1")
    xr.testing.assert_equal(reopened.get(), original)
