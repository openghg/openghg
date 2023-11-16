import pytest
import xarray as xr
import numcodecs
from openghg.objectstore import get_writable_bucket
from openghg.store.storage import LocalZarrStore
from openghg.types import ZarrStoreError
from helpers import get_footprint_datapath

# @pytest.fixture(scope="module")
# def store():
#     bucket = get_writable_bucket(name="user")
#     datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
#     store = LocalZarrStore(bucket=bucket, datasource_uuid="123", mode="rw")
#         with xr.open_dataset(datapath) as ds:
#             return ds


@pytest.fixture()
def store():
    bucket = get_writable_bucket(name="user")
    local_store = LocalZarrStore(bucket=bucket, datasource_uuid="test-local-store-123", mode="rw")
    yield local_store
    local_store.delete_all()


def test_localzarrstore_add_retrieve(store):
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    with xr.open_dataset(datapath) as ds:
        store.add(version="v0", dataset=ds)
        retrieved = store.get(version="v0")
        retrieved = retrieved.compute()
        assert ds.equals(retrieved)

    assert store.version_exists(version="v0")


# def test_copy_to_memory_store(store):
#     datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
#     with xr.open_dataset(datapath) as ds:
#         store.add(version="v0", dataset=ds)

#         memory_store = store.copy_to_memorystore(keys=["test"], version="v0")
#         ds_recombined = xr.open_mfdataset(
#             paths=memory_store, engine="zarr", combine="by_coords", consolidated=True
#         )

#         # Let's not let xarray mess around
#         ds_recombined = ds_recombined.compute()
#         assert ds.equals(ds_recombined)


def test_update(store):
    fp_1 = get_footprint_datapath("TAC-100magl_EUROPE_201208.nc")
    fp_2 = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")

    with xr.open_dataset(fp_1) as ds:
        store.add(version="v0", dataset=ds)
        ds_loaded = store.get(version="v0")
        assert ds.equals(ds_loaded)

    with xr.open_dataset(fp_2) as ds:
        store.update(version="v0", dataset=ds)
        ds_loaded = store.get(version="v0")
        assert ds.equals(ds_loaded)


def test_bytes_stored(store):
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    with xr.open_dataset(datapath) as ds:
        store.add(version="v0", dataset=ds, compressor=None)
        assert store.bytes_stored() == 446038


def test_bytes_stored_compression(store):
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    original_size = datapath.stat().st_size
    compressor = numcodecs.Blosc(cname="zstd", clevel=5, shuffle=numcodecs.Blosc.SHUFFLE)
    with xr.open_dataset(datapath) as ds:
        store.add(version="v0", dataset=ds, compressor=compressor)
        assert store.bytes_stored() == 294552

        assert store.bytes_stored() < original_size


def test_delete_version(store):
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    with xr.open_dataset(datapath) as ds:
        store.add(version="v0", dataset=ds)

    path = store.store_path(version="v0")
    assert path.exists()

    store.delete_version(version="v0")

    assert not path.exists()

    with pytest.raises(KeyError):
        store.delete_version(version="v0")


def test_delete_all(store):
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    with xr.open_dataset(datapath) as ds:
        store.add(version="v0", dataset=ds)
        store.add(version="v1", dataset=ds)

    path_v0 = store.store_path(version="v0")
    assert path_v0.exists()
    path_v1 = store.store_path(version="v1")
    assert path_v1.exists()

    parent = path_v0.parent

    store.delete_all()

    assert not parent.exists()


def test_dataset_retrieved_same_shape_etc(store):
    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    with xr.open_dataset(datapath) as ds:
        store.add(version="v0", dataset=ds)

        retrieved = store.get(version="v0")
        assert ds.dims == retrieved.dims
        assert ds.coords.equals(retrieved.coords)
        assert ds.identical(retrieved)


def test_copy_actually_copies(store):
    import pandas as pd
    import numpy as np
    import timeit

    time_a = pd.date_range("2012-01-01T00:00:00", "2012-01-31T00:00:00", freq="1d")
    time_b = pd.date_range("2012-01-29T00:00:00", "2012-04-30T00:00:00", freq="1d")

    values_a = np.zeros(len(time_a))
    values_b = np.full(len(time_b), 1)

    attributes = {}

    data_a = xr.Dataset({"mf": ("time", values_a)}, coords={"time": time_a}, attrs=attributes)
    data_b = xr.Dataset({"mf": ("time", values_b)}, coords={"time": time_b}, attrs=attributes)

    ds_expected = xr.concat([data_a, data_b], dim="time").drop_duplicates("time")

    store.add(version="v0", dataset=data_a)
    # Copy the data into memory using get
    ds_a_from_store = store.get(version="v0")
    store.delete_version(version="v0")
    ds_a_from_store = ds_a_from_store.compute()
    ds_2 = xr.concat([ds_a_from_store, data_b], dim="time").drop_duplicates("time")

    assert not ds_2.equals(ds_expected)
    assert np.sum(np.isnan(ds_2.mf.values))

    store.add(version="v0", dataset=data_a)
    # If we call compute and load everything into memory before deleting the 
    # data from disk then it works
    ds_a_from_store = store.get(version="v0")
    ds_a_from_store = ds_a_from_store.compute()
    store.delete_version(version="v0")
    ds_2 = xr.concat([ds_a_from_store, data_b], dim="time").drop_duplicates("time")

    assert ds_2.equals(ds_expected)
    assert not np.sum(np.isnan(ds_2.mf.values))

    store.add(version="v0", dataset=data_a)

    # Let's try copying it to a dict
    data_a_in_dict = store.copy_to_memorystore(version="v0")
    ds_a_from_dict = xr.open_zarr(store=data_a_in_dict, consolidated=True)
    store.delete_version(version="v0")
    ds_2 = xr.concat([ds_a_from_dict, data_b], dim="time").drop_duplicates("time")

    assert ds_2.equals(ds_expected)
    assert not np.sum(np.isnan(ds_2.mf.values))
