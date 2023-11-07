import datetime
import os
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from addict import Dict as aDict
from helpers import get_surface_datapath, get_footprint_datapath
from openghg.objectstore import get_bucket, get_object_names, delete_object, get_writable_bucket, exists
from openghg.standardise.surface import parse_crds
from openghg.store.base import Datasource
from openghg.types import ObjectStoreError
from openghg.util import create_daterange_str, daterange_overlap, pairwise, timestamp_tzaware

mocked_uuid = "00000000-0000-0000-00000-000000000000"
mocked_uuid2 = "10000000-0000-0000-00000-000000000001"

# Disable this for long strings below - Line break occurred before a binary operator (W503)
# flake8: noqa: W503


@pytest.fixture(scope="session")
def data():
    filename = "bsd.picarro.1minute.248m.min.dat"
    filepath = get_surface_datapath(filename=filename, source_format="CRDS")

    return parse_crds(data_filepath=filepath, site="bsd", network="DECC")


@pytest.fixture
def datasource(bucket):
    return Datasource(bucket=bucket)


@pytest.fixture
def mock_uuid(monkeypatch):
    def mock_uuid():
        return mocked_uuid

    monkeypatch.setattr(uuid, "uuid4", mock_uuid)


@pytest.fixture
def mock_uuid2(monkeypatch):
    def mock_uuid():
        return mocked_uuid2

    monkeypatch.setattr(uuid, "uuid4", mock_uuid)


@pytest.fixture
def bucket():
    return get_bucket()


def test_add_data(data, bucket):
    d = Datasource(bucket=bucket)

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    assert ch4_data["ch4"][0] == pytest.approx(1959.55)
    assert ch4_data["ch4_variability"][0] == pytest.approx(0.79)
    assert ch4_data["ch4_number_of_observations"][0] == pytest.approx(26.0)

    d.add_data(metadata=metadata, data=ch4_data, sort=False, drop_duplicates=False, data_type="surface")

    assert d._zarr_store

    memory_store = d.memory_store(version="v0")
    assert memory_store

    ds = xr.open_mfdataset(paths=memory_store, engine="zarr", combine="by_coords")
    assert ds.equals(ch4_data)

    expected_metadata = {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "60.0",
        "inlet": "248m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "ch4",
        "calibration_scale": "wmo-x2004a",
        "long_name": "bilsdale",
        "inlet_height_magl": "248",
        "data_owner": "simon o'doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "station_longitude": -1.15033,
        "station_latitude": 54.35858,
        "station_long_name": "bilsdale, uk",
        "station_height_masl": 380.0,
        "data_type": "surface",
        "start_date": "2014-01-30 11:12:30+00:00",
        "end_date": "2020-12-01 22:32:29+00:00",
        "latest_version": "v0",
    }

    d.metadata()["versions"]["v0"]["keys"] = ["2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00"]

    assert d.metadata().items() >= expected_metadata.items()


def test_versioning(capfd, bucket):
    min_tac_filepath = get_surface_datapath(filename="tac.picarro.1minute.100m.min.dat", source_format="CRDS")
    detailed_tac_filepath = get_surface_datapath(
        filename="tac.picarro.1minute.100m.201407.dat", source_format="CRDS"
    )

    min_data = parse_crds(data_filepath=min_tac_filepath, site="tac", inlet="100m", network="decc")

    # Take head of data
    # Then add the full data, check versioning works correctly
    metadata = {"foo": "bar"}

    d = Datasource(bucket=bucket)
    # Fix the UUID for the tests
    d._uuid = "4b91f73e-3d57-47e4-aa13-cb28c35d3b3d"

    min_ch4_data = min_data["ch4"]["data"]

    d.add_data(metadata=metadata, data=min_ch4_data, sort=False, drop_duplicates=False, data_type="surface")
    d.save()

    min_keys = d.versions()

    assert min_keys["v0"]["keys"] == ["2012-07-26-13:51:30+00:00_2020-07-04-09:58:30+00:00"]

    detailed_data = parse_crds(data_filepath=detailed_tac_filepath, site="tac", inlet="100m", network="decc")

    detailed_ch4_data = detailed_data["ch4"]["data"]

    # Check new version can be created and stored (using appropriate flag)
    d.add_data(
        metadata=metadata,
        data=detailed_ch4_data,
        data_type="surface",
        sort=False,
        drop_duplicates=False,
        new_version=True,
        if_exists="new",
    )

    d.save()

    detailed_keys = d.versions()

    assert detailed_keys["v1"]["keys"] == ["2014-06-30-00:06:30+00:00_2014-08-01-23:49:30+00:00"]

    # TODO: Add case for if_exists="combine" which should look more like original case above after updates


def test_replace_version(bucket):
    """Test that new data can replace previous data. This involves deleting the previous version
    data and copying across the new data.
    """
    min_tac_filepath = get_surface_datapath(filename="tac.picarro.1minute.100m.min.dat", source_format="CRDS")
    detailed_tac_filepath = get_surface_datapath(
        filename="tac.picarro.1minute.100m.201407.dat", source_format="CRDS"
    )

    min_data = parse_crds(data_filepath=min_tac_filepath, site="tac", inlet="100m", network="decc")

    min_ch4_data = min_data["ch4"]["data"]
    metadata = {"foo": "bar"}

    d = Datasource(bucket=bucket)
    # Fix the UUID for the tests
    d._uuid = "4b91f73e-3d57-47e4-aa13-cb28c35d3b3d"
    d.add_data(metadata=metadata, data=min_ch4_data, sort=False, drop_duplicates=False, data_type="surface")

    # Save initial data
    bucket = get_bucket()
    d.save()

    detailed_data = parse_crds(data_filepath=detailed_tac_filepath, site="tac", inlet="100m", network="decc")

    detailed_ch4_data = detailed_data["ch4"]["data"]

    # Check new version can be created and stored (using appropriate flag)
    d.add_data(
        metadata=metadata,
        data=detailed_ch4_data,
        sort=False,
        drop_duplicates=False,
        data_type="surface",
        if_exists="new",
    )

    # Save and overwrite with new data
    d.save()

    detailed_keys = d.versions()

    assert detailed_keys["v1"]["keys"] == ["2014-06-30-00:06:30+00:00_2014-08-01-23:49:30+00:00"]

    # TODO: Add case for if_exists="combine" which should look more like original case above after updates


def test_get_dataframe_daterange(bucket):
    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)
    random_data = pd.DataFrame(
        data=np.random.randint(0, 100, size=(100, 4)),
        index=pd.date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
        columns=list("ABCD"),
    )

    d = Datasource(bucket=bucket)

    start, end = d.get_dataframe_daterange(random_data)

    assert start == pd.Timestamp("1970-01-01 01:01:00+0000")
    assert end == pd.Timestamp("1970-04-10 01:01:00+0000")


def test_save(mock_uuid2, bucket):
    bucket = get_bucket()

    datasource = Datasource(bucket=bucket)
    datasource.add_metadata_key(key="data_type", value="surface")
    datasource.save()

    exists(bucket=bucket, key=datasource.key())


def test_save_footprint(bucket):
    metadata = {"test": "testing123", "start_date": "2013-06-02", "end_date": "2013-06-30"}

    filepath = get_footprint_datapath(filename="WAO-20magl_UKV_rn_TEST_202112.nc")

    data = xr.open_dataset(filepath)

    bucket = get_bucket()

    datasource = Datasource(bucket=bucket)
    datasource.add_data(
        data=data, metadata=metadata, sort=False, drop_duplicates=False, data_type="footprints"
    )
    datasource.save()

    datasource_2 = Datasource(bucket=bucket, uuid=datasource._uuid)

    memory_store = datasource_2.memory_store(version="latest")
    with xr.open_mfdataset(paths=memory_store, engine="zarr", combine="by_coords") as ds:
        assert ds.equals(data)

    assert datasource_2._data_type == "footprints"


def test_add_metadata_key(datasource):
    datasource.add_metadata_key(key="foo", value=123)
    datasource.add_metadata_key(key="bar", value=456)

    assert datasource._metadata["foo"] == "123"
    assert datasource._metadata["bar"] == "456"


def test_add_metadata_lowercases_correctly(datasource):
    metadata = {
        "AAA": {
            "INLETS": {"inlet_A": "158m", "inlet_b": "12m"},
            "some_metadata": {"OWNER": "foo", "eMAIL": "this@that"},
        }
    }

    datasource.add_metadata(metadata=metadata)

    assert datasource.metadata() == {
        "aaa": {
            "inlets": {"inlet_a": "158m", "inlet_b": "12m"},
            "some_metadata": {"owner": "foo", "email": "this@that"},
        }
    }


def test_save_and_load(data, bucket):
    d = Datasource(bucket=bucket)

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")
    d.save()

    d_2 = Datasource(bucket=bucket, uuid=d.uuid())

    metadata = d_2.metadata()
    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["sampling_period"] == "60.0"
    assert metadata["inlet"] == "248m"

    assert sorted(d_2.data_keys()) == sorted(d.data_keys())
    assert d_2.metadata() == d.metadata()


def test_incorrect_datatype_raises(data, bucket):
    d = Datasource(bucket=bucket)

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    with pytest.raises(TypeError):
        d.add_data(metadata=metadata, data=ch4_data, sort=False, drop_duplicates=False, data_type="CRDS")


def test_update_daterange_replacement(data, bucket):
    metadata = {"foo": "bar"}

    d = Datasource(bucket=bucket)

    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, sort=False, drop_duplicates=False, data_type="surface")

    assert d._start_date == pd.Timestamp("2014-01-30 11:12:30+00:00")
    assert d._end_date == pd.Timestamp("2020-12-01 22:31:30+00:00")

    ch4_short = ch4_data.head(40)

    keys = d.data_keys()

    d.delete_data(version="v0", keys=keys)

    d.add_data(metadata=metadata, data=ch4_short, data_type="surface")

    assert d._start_date == pd.Timestamp("2014-01-30 11:12:30+00:00")
    assert d._end_date == pd.Timestamp("2016-04-02 06:55:30+00:00")


def test_load_dataset(bucket):
    with pytest.raises(NotImplementedError):
        Datasource.load_dataset(bucket=bucket, key="key")


def test_in_daterange(data, bucket):
    metadata = data["ch4"]["metadata"]
    data = data["ch4"]["data"]

    d = Datasource(bucket=bucket)
    d._uuid = "test-id-123"
    d.add_data(metadata=metadata, data=data, data_type="surface")

    assert d.data_keys() == ["2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00"]

    start = pd.Timestamp("2014-1-1")
    end = pd.Timestamp("2014-2-1")
    daterange = create_daterange_str(start=start, end=end)

    dated_keys = d.keys_in_daterange_str(daterange=daterange)

    assert dated_keys == ["2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00"]


def test_key_date_compare(bucket):
    d = Datasource(bucket=bucket)

    keys = [
        "2014-01-30-11:12:30+00:00_2014-11-30-11:23:30+00:00",
        "2015-01-30-11:12:30+00:00_2015-11-30-11:23:30+00:00",
        "2016-04-02-06:52:30+00:00_2016-11-02-12:54:30+00:00",
        "2017-02-18-06:36:30+00:00_2017-12-18-15:41:30+00:00",
        "2018-02-18-15:42:30+00:00_2018-12-18-15:42:30+00:00",
        "2019-02-03-17:38:30+00:00_2019-12-09-10:47:30+00:00",
        "2020-02-01-18:08:30+00:00_2020-12-01-22:31:30+00:00",
    ]

    start = timestamp_tzaware("2014-01-01")
    end = timestamp_tzaware("2018-01-01")

    in_date = d.key_date_compare(keys=keys, start_date=start, end_date=end)

    expected = [
        "2014-01-30-11:12:30+00:00_2014-11-30-11:23:30+00:00",
        "2015-01-30-11:12:30+00:00_2015-11-30-11:23:30+00:00",
        "2016-04-02-06:52:30+00:00_2016-11-02-12:54:30+00:00",
        "2017-02-18-06:36:30+00:00_2017-12-18-15:41:30+00:00",
    ]

    assert in_date == expected

    start = timestamp_tzaware("2026-01-01")
    end = timestamp_tzaware("2029-01-01")

    in_date = d.key_date_compare(keys=keys, start_date=start, end_date=end)

    assert not in_date


def test_integrity_check(data, bucket):
    d = Datasource(bucket=bucket)

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    assert ch4_data["ch4"][0] == pytest.approx(1959.55)
    assert ch4_data["ch4_variability"][0] == pytest.approx(0.79)
    assert ch4_data["ch4_number_of_observations"][0] == pytest.approx(26.0)

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")
    d.save()

    uid = d.uuid()

    d = Datasource(bucket=bucket, uuid=uid)
    d.integrity_check()

    d._zarr_store.delete_all()

    with pytest.raises(ObjectStoreError):
        d.integrity_check()


def test_data_deletion(data, bucket):
    d = Datasource(bucket=bucket)

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")

    keys = d.data_keys()

    zarr_keys = set(d._zarr_store.keys())

    partial_expected_keys = {
        "v0/2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00/ch4/.zarray",
        "v0/2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00/ch4/.zattrs",
        "v0/2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00/ch4/0",
        "v0/2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00/ch4_variability/.zarray",
    }

    assert partial_expected_keys.issubset(zarr_keys)

    d.delete_data(version="latest", keys=keys)

    assert not d._data_keys["v0"]["keys"]
    assert list(d._zarr_store.keys()) == [".zgroup", ".zmetadata", "v0/.zgroup"]


def test_data_version_deletion(data, bucket):
    d = Datasource(bucket=bucket)

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")

    zarr_keys = set(d._zarr_store.keys())

    partial_expected_keys = {
        "v0/2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00/ch4/.zarray",
        "v0/2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00/ch4/.zattrs",
        "v0/2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00/ch4/0",
        "v0/2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00/ch4_variability/.zarray",
    }

    assert partial_expected_keys.issubset(zarr_keys)

    d.delete_version(version="v0")

    assert "v0" not in d._data_keys
    assert list(d._zarr_store.keys()) == [".zgroup", ".zmetadata", "v0/.zgroup"]
