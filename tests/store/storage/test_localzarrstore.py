import numpy as np
import pandas as pd
import pytest
import xarray as xr
import numcodecs
from openghg.objectstore import get_writable_bucket
from openghg.store.storage import LocalZarrStore
from openghg.types import ZarrStoreError
from helpers import get_footprint_datapath


@pytest.fixture()
def store():
    bucket = get_writable_bucket(name="user")
    local_store = LocalZarrStore(bucket=bucket, datasource_uuid="test-local-store-123", mode="rw")
    yield local_store
    local_store.delete_all()


def test_localzarrstore_not_writable():
    bucket = get_writable_bucket(name="user")
    local_store = LocalZarrStore(bucket=bucket, datasource_uuid="test-local-store-123", mode="r")

    with pytest.raises(PermissionError):
        local_store.add(version="v1", dataset=xr.Dataset())

    with pytest.raises(PermissionError):
        local_store.overwrite(version="v1", dataset=xr.Dataset())

    with pytest.raises(PermissionError):
        local_store.delete_version(version="v1")

    with pytest.raises(PermissionError):
        local_store.delete_all()


def test_localzarrstore_add_retrieve(store):
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    with xr.open_dataset(datapath) as ds:
        store.add(version="v1", dataset=ds)
        retrieved = store.get(version="v1")
        retrieved = retrieved.compute()
        assert ds.equals(retrieved)

    assert store.version_exists(version="v1")


def test_dataset_retrieved_single_dataset_identical(store):
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    with xr.open_dataset(datapath) as ds:
        store.add(version="v1", dataset=ds)

        retrieved = store.get(version="v1")
        assert ds.identical(retrieved)


def test_localzarrstore_add_successive_dates_append():
    bucket = get_writable_bucket(name="user")
    store = LocalZarrStore(bucket=bucket, datasource_uuid="888-888", mode="rw")

    file_a = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    file_b = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")
    with xr.open_dataset(file_a) as ds, xr.open_dataset(file_b) as ds2:
        store.add(version="v1", dataset=ds)
        store.add(version="v1", dataset=ds2)

        assert store.version_exists(version="v1")

        concat = xr.concat([ds, ds2], dim="time")
        retrieved = store.get(version="v1").compute()

        assert retrieved.equals(concat)
        for var in retrieved.data_vars:
            assert not np.sum(np.isnan(retrieved[var]))


@pytest.mark.xfail(reason="Attributes of Datasets appended to zarr store may currently be overwritten")
def test_localzarrstore_add_files_identical(store):
    file_a = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    file_b = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")
    with xr.open_dataset(file_a) as ds, xr.open_dataset(file_b) as ds2:
        store.add(version="v1", dataset=ds)
        store.add(version="v1", dataset=ds2)

        assert store.version_exists(version="v1")

        concat = xr.concat([ds, ds2], dim="time")
        retrieved = store.get(version="v1").compute()

        assert retrieved.attrs == concat.attrs
        assert retrieved.identical(concat)


def test_overwrite(store):
    fp_1 = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    fp_2 = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")

    with xr.open_dataset(fp_1) as ds:
        store.add(version="v1", dataset=ds)
        ds_loaded = store.get(version="v1")
        assert ds.equals(ds_loaded)

    with xr.open_dataset(fp_2) as ds:
        store.overwrite(version="v1", dataset=ds)
        ds_loaded = store.get(version="v1")
        assert ds.equals(ds_loaded)


def test_bytes_stored_compression(store):
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    original_size = datapath.stat().st_size

    with xr.open_dataset(datapath) as ds:
        store.add(version="v1", dataset=ds, compressor=None)
        uncompressed_bytes = store.bytes_stored()
        assert store.bytes_stored() == 444382

    store.delete_all()

    compressor = numcodecs.Blosc(cname="zstd", clevel=5, shuffle=numcodecs.Blosc.SHUFFLE)
    with xr.open_dataset(datapath) as ds:
        store.add(version="v1", dataset=ds, compressor=compressor)
        compressed_bytes = store.bytes_stored()
        assert np.abs((compressed_bytes - 292896) / 292896) < 0.01
        assert compressed_bytes < original_size
        assert compressed_bytes < uncompressed_bytes


def test_delete_version(store):
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    with xr.open_dataset(datapath) as ds:
        store.add(version="v1", dataset=ds)

    path = store.store_path(version="v1")
    assert path.exists()

    store.delete_version(version="v1")

    assert not path.exists()

    with pytest.raises(ZarrStoreError):
        store.delete_version(version="v1")


def test_delete_all(store):
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    with xr.open_dataset(datapath) as ds:
        store.add(version="v1", dataset=ds)
        store.add(version="v2", dataset=ds)

    path_v1 = store.store_path(version="v1")
    assert path_v1.exists()
    path_v2 = store.store_path(version="v2")
    assert path_v2.exists()

    parent = path_v1.parent

    store.delete_all()

    assert not parent.exists()

    assert not parent.parent.exists()


def test_match_chunking(store):
    time_a = pd.date_range("2012-01-01T00:00:00", "2012-01-31T00:00:00", freq="1d")
    time_b = pd.date_range("2012-02-01T00:00:00", "2012-04-30T00:00:00", freq="1d")

    values_a = np.zeros(len(time_a))
    values_b = np.full(len(time_b), 1)

    data_a = xr.Dataset({"mf": ("time", values_a)}, coords={"time": time_a})
    data_b = xr.Dataset({"mf": ("time", values_b)}, coords={"time": time_b})

    store.add(version="v1", dataset=data_a)

    chunking = store.match_chunking(version="v1", dataset=data_b)

    assert not chunking

    store.delete_all()

    # Let's add some data and then try and add some data with different chunking
    chunks = {"time": 4}
    store.add(version="v1", dataset=data_a)

    data_a_chunked = data_a.chunk(chunks)
    chunking = store.match_chunking(version="v1", dataset=data_a_chunked)

    # As the data we originally put in wasn't chunked then we get the full size of the time coordinate
    # which is 31 here
    assert chunking == {"time": 31}

    # Now try it the other way round, add chunked data and then try to match it with unchunked data
    store.delete_all()

    store.add(version="v1", dataset=data_a_chunked)
    chunking = store.match_chunking(version="v1", dataset=data_a)

    assert chunking == {"time": 4}

    # Now try it with two chunked datasets, should return the chunking of the first dataset
    store.delete_all()

    chunks_a = {"time": 16}

    data_a_chunked = data_a.chunk(chunks_a)
    store.add(version="v1", dataset=data_a_chunked)

    chunks_b = {"time": 12}
    data_b_chunked = data_b.chunk(chunks_b)
    chunking = store.match_chunking(version="v1", dataset=data_b_chunked)

    assert chunking == {"time": 16}

    store.add(version="v1", dataset=data_b_chunked)
    # Let's check that the chunks in the store are correct
    chunked_dataset = store.get(version="v1")

    assert dict(chunked_dataset.chunks) == {"time": (16, 16, 16, 16, 16, 16, 16, 9)}

    # Now try it with two datasets with the same chunking, should return an empty dictionary
    store.delete_all()

    chunks = {"time": 12}
    data_a_chunked = data_a.chunk(chunks)
    store.add(version="v1", dataset=data_a_chunked)

    data_b_chunked = data_b.chunk(chunks)
    chunking = store.match_chunking(version="v1", dataset=data_b_chunked)

    assert not chunking


def test_for_missing_data_from_appending_in_loop():
    """If multiple "source" chunks (e.g. from chunked data we wish to store) need to write
    to the same "target" chunks (in the zarr store), then it is possible that the data will be
    corrupted if multiple writes are set off in a loop.

    This test tries to simulate this situation. Note that it isn't possible to write a deterministic
    test for this, because it depends on how workers for writing the chunks are scheduled.

    For adding 6 inert footprints in a loop, data loss was observed about 40% of the time for at least one
    data variable in the footprint; however, that test is fairly slow, so we will try to recreate it with
    artificial data here.

    See GH Issue #1031 for more details.

    """
    bucket = get_writable_bucket(name="user")

    # make 6 datasets that will have "ragged" start/end chunks
    duration = 820
    chunk_size = 403

    rng = np.random.default_rng(seed=2**32 - 1)

    datasets = []
    for i in range(3):
        ds = xr.Dataset(
            {
                "x": (["time"], rng.normal(0, 1, duration)),
                "y": (["time"], rng.normal(0, 1, duration)),
            },
            coords={
                "time": duration * i + np.arange(duration),
            },
            attrs={},
        )
        datasets.append(ds.chunk({"time": chunk_size}))

    # add data repeatedly, testing for missing values
    for i in range(10):
        local_store = LocalZarrStore(bucket=bucket, datasource_uuid=f"test-local-store-123-{i}", mode="rw")

        for ds in datasets:
            local_store.add(version="v1", dataset=ds)

        retrieved = local_store.get(version="v1")

        missing = retrieved.isnull().sum().compute()
        total_missing = sum(dict(missing).values())

        assert total_missing == 0.0

        local_store.delete_all()
