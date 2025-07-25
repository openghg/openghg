import datetime
import uuid

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from helpers import get_surface_datapath, get_footprint_datapath
from openghg.objectstore import get_bucket, exists
from openghg.standardise.surface import parse_crds
from openghg.objectstore import Datasource
from openghg.types import ObjectStoreError, ZarrStoreError
from openghg.util import create_daterange_str, timestamp_tzaware

mocked_uuid = "00000000-0000-0000-00000-000000000000"
mocked_uuid2 = "10000000-0000-0000-00000-000000000001"


def create_attributes():
    species = "co2"
    site = "TEST_SITE"
    inlet = "10m"
    sampling_period = "60.0"

    return {"species": species, "site": site, "inlet": inlet, "sampling_period": sampling_period}


# Disable this for long strings below - Line break occurred before a binary operator (W503)
# flake8: noqa: W503
def create_three_datasets(a, b, c):
    values_a = np.arange(0, len(a), 1)
    values_b = np.arange(0, len(b), 1)
    values_c = np.arange(0, len(c), 1)

    attributes = create_attributes()

    data_a = xr.Dataset({"mf": ("time", values_a)}, coords={"time": a}, attrs=attributes)
    data_b = xr.Dataset({"mf": ("time", values_b)}, coords={"time": b}, attrs=attributes)
    data_c = xr.Dataset({"mf": ("time", values_c)}, coords={"time": c}, attrs=attributes)

    return data_a, data_b, data_c


@pytest.fixture(scope="session")
def data():
    filename = "bsd.picarro.1minute.248m.min.dat"
    filepath = get_surface_datapath(filename=filename, source_format="CRDS")

    return parse_crds(filepath=filepath, site="bsd", network="DECC")


class UUID:
    """Make a sequence of unique uuids.

    They have the form "uuid1", "uuid2", etc.
    """
    uuid_int = 0

    def __init__(self) -> None:
        self.uuid_int += 1

    def __str__(self) -> str:
        return f"uuid{self.uuid_int}"


@pytest.fixture
def datasource(bucket):
    d = Datasource(uuid=str(UUID()), bucket=bucket)
    yield d
    d.delete_all_data()


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


@pytest.fixture
def datasets_with_gaps():
    time_a = pd.date_range("2012-01-01T00:00:00", "2012-01-31T00:00:00", freq="1d")
    time_b = pd.date_range("2012-04-01T00:00:00", "2012-04-30T00:00:00", freq="1d")
    time_c = pd.date_range("2012-09-01T00:00:00", "2012-09-30T00:00:00", freq="1d")

    return create_three_datasets(time_a, time_b, time_c)


@pytest.fixture()
def datasets_with_overlap():
    time_a = pd.date_range("2012-01-01T00:00:00", "2012-01-31T00:00:00", freq="1d")
    time_b = pd.date_range("2012-01-29T00:00:00", "2012-04-30T00:00:00", freq="1d")
    time_c = pd.date_range("2012-04-16T00:00:00", "2012-09-30T00:00:00", freq="1d")

    return create_three_datasets(time_a, time_b, time_c)


def test_add_data(data, datasource):
    d = datasource

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    assert ch4_data["ch4"][0] == pytest.approx(1959.55)
    assert ch4_data["ch4_variability"][0] == pytest.approx(0.79)
    assert ch4_data["ch4_number_of_observations"][0] == pytest.approx(26.0)

    d.add_data(metadata=metadata, data=ch4_data, sort=False, drop_duplicates=False, data_type="surface")

    assert d._store

    ds = d.get_data(version="v1")

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
        "data_type": "surface",
        "start_date": "2014-01-30 11:12:30+00:00",
        "end_date": "2020-12-01 22:32:29+00:00",
        "latest_version": "v1",
    }

    d.metadata()["versions"]["v1"] = ["2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00"]

    assert d.metadata().items() >= expected_metadata.items()


def test_versioning(datasource):
    min_tac_filepath = get_surface_datapath(filename="tac.picarro.1minute.100m.min.dat", source_format="CRDS")
    detailed_tac_filepath = get_surface_datapath(
        filename="tac.picarro.1minute.100m.201407.dat", source_format="CRDS"
    )

    min_data = parse_crds(filepath=min_tac_filepath, site="tac", inlet="100m", network="decc")

    # Take head of data
    # Then add the full data, check versioning works correctly
    metadata = {"foo": "bar"}

    d = datasource # Datasource(uuid="4b91f73e-3d57-47e4-aa13-cb28c35d3b3d", bucket=bucket)

    min_ch4_data = min_data["ch4"]["data"]

    d.add_data(metadata=metadata, data=min_ch4_data, sort=False, drop_duplicates=False, data_type="surface")
    d.save()

    min_keys = d.all_data_keys()

    assert min_keys["v1"] == ["2012-07-26-13:51:30+00:00_2020-07-04-09:58:30+00:00"]

    detailed_data = parse_crds(filepath=detailed_tac_filepath, site="tac", inlet="100m", network="decc")

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

    detailed_keys = d.all_data_keys()

    assert detailed_keys["v2"] == ["2014-06-30-00:06:30+00:00_2014-08-01-23:49:30+00:00"]

    # TODO: Add case for if_exists="combine" which should look more like original case above after updates


def test_replace_version(bucket):
    """Test that new data can replace previous data. This involves deleting the previous version
    data and copying across the new data.
    """
    min_tac_filepath = get_surface_datapath(filename="tac.picarro.1minute.100m.min.dat", source_format="CRDS")
    detailed_tac_filepath = get_surface_datapath(
        filename="tac.picarro.1minute.100m.201407.dat", source_format="CRDS"
    )

    min_data = parse_crds(filepath=min_tac_filepath, site="tac", inlet="100m", network="decc")

    min_ch4_data = min_data["ch4"]["data"]
    metadata = {"foo": "bar"}

    d = Datasource(uuid="4b91f73e-3d57-47e4-aa13-cb28c35d3b3d", bucket=bucket)

    d.add_data(metadata=metadata, data=min_ch4_data, sort=False, drop_duplicates=False, data_type="surface")

    # Save initial data
    d.save()

    detailed_data = parse_crds(filepath=detailed_tac_filepath, site="tac", inlet="100m", network="decc")

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

    detailed_keys = d.all_data_keys()

    assert detailed_keys["v2"] == ["2014-06-30-00:06:30+00:00_2014-08-01-23:49:30+00:00"]

    # TODO: Add case for if_exists="combine" which should look more like original case above after updates


def test_save(bucket):

    datasource = Datasource(uuid="abc123", bucket=bucket)
    datasource.add_metadata_key(key="data_type", value="surface")
    datasource.save()

    exists(bucket=bucket, key=datasource.key())


def test_save_footprint(bucket, datasource):
    metadata = {"test": "testing123", "start_date": "2013-06-02", "end_date": "2013-06-30"}

    filepath = get_footprint_datapath(filename="WAO-20magl_UKV_rn_TEST_202112.nc")

    data = xr.open_dataset(filepath)

    datasource.add_data(
        data=data, metadata=metadata, sort=False, drop_duplicates=False, data_type="footprints"
    )
    datasource.save()

    datasource_2 = Datasource.load(bucket=bucket, uuid=datasource._uuid, mode="r")

    retrieved_ds = datasource_2.get_data(version="v1")
    assert retrieved_ds.equals(data)

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
    d = Datasource(uuid="abc123", bucket=bucket)

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")
    d.save()

    d_2 = Datasource.load(bucket=bucket, uuid=d.uuid())

    metadata = d_2.metadata()
    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["sampling_period"] == "60.0"
    assert metadata["inlet"] == "248m"

    assert sorted(d_2.data_keys()) == sorted(d.data_keys())
    assert d_2.metadata() == d.metadata()


@pytest.mark.xfail(reason=".add_data no longer checks the data type")
def test_incorrect_datatype_raises(data, bucket):
    d = Datasource(uuid="abc123", bucket=bucket)

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    with pytest.raises(TypeError):
        d.add_data(metadata=metadata, data=ch4_data, sort=False, drop_duplicates=False, data_type="CRDS")


def test_update_daterange_replacement(data, bucket):
    metadata = {"foo": "bar"}

    d = Datasource(uuid="abc123", bucket=bucket)

    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, sort=False, drop_duplicates=False, data_type="surface")

    assert d._start_date == pd.Timestamp("2014-01-30 11:12:30+00:00")
    assert d._end_date == pd.Timestamp("2020-12-01 22:31:30+00:00")

    ch4_short = ch4_data.head(40)

    d.delete_version(version="v1")

    d.add_data(metadata=metadata, data=ch4_short, data_type="surface")

    assert d._start_date == pd.Timestamp("2014-01-30 11:12:30+00:00")
    assert d._end_date == pd.Timestamp("2016-04-02 06:55:30+00:00")


def test_integrity_check(data, bucket):
    d = Datasource(uuid="abc123", bucket=bucket)

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    assert ch4_data["ch4"][0] == pytest.approx(1959.55)
    assert ch4_data["ch4_variability"][0] == pytest.approx(0.79)
    assert ch4_data["ch4_number_of_observations"][0] == pytest.approx(26.0)

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")
    d.save()

    uid = d.uuid()

    d = Datasource.load(bucket=bucket, uuid=uid)
    d.integrity_check()

    d._store.delete_all()

    with pytest.raises(ObjectStoreError):
        d.integrity_check()


def test_data_version_deletion(data, bucket):
    d = Datasource(uuid="abc123", bucket=bucket)

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")

    zarr_keys = set(d._store.keys(version="v1"))

    partial_expected_keys = {
        "ch4/.zarray",
        "ch4/.zattrs",
        "ch4/0",
        "ch4_variability/.zarray",
    }

    assert partial_expected_keys.issubset(zarr_keys)

    d.delete_version(version="v1")

    assert "v1" not in d._data_keys

    with pytest.raises(ZarrStoreError):
        d._store.keys(version="v1")


def test_surface_data_stored_and_dated_correctly(data, datasource):
    d = datasource

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")

    start, end = d.daterange()

    with d.get_data(version="v1") as stored_ds:
        assert stored_ds.equals(ch4_data)

    assert timestamp_tzaware(stored_ds["ch4"].time[0].values) == timestamp_tzaware("2014-01-30T11:12:30")
    assert timestamp_tzaware(start) == timestamp_tzaware("2014-01-30 11:12:30+00:00")
    assert timestamp_tzaware(stored_ds["ch4"].time[-1].values) == timestamp_tzaware("2020-12-01T22:31:30")
    assert timestamp_tzaware(end) == timestamp_tzaware("2020-12-01 22:32:29")


def test_add_data_with_gaps_check_stored_dataset(datasets_with_gaps, datasource):
    data_a, data_b, data_c = datasets_with_gaps
    attributes = create_attributes()

    d = datasource

    d.add_data(metadata=attributes, data=data_a, data_type="surface", new_version=False)
    d.add_data(metadata=attributes, data=data_b, data_type="surface", new_version=False)
    d.add_data(metadata=attributes, data=data_c, data_type="surface", new_version=False)

    assert d.data_keys() == [
        "2012-01-01-00:00:00+00:00_2012-01-31-00:00:59+00:00",
        "2012-04-01-00:00:00+00:00_2012-04-30-00:00:59+00:00",
        "2012-09-01-00:00:00+00:00_2012-09-30-00:00:59+00:00",
    ]

    assert d.latest_version() == "v1"

    with d.get_data(version="latest") as ds:
        assert ds.equals(xr.concat([data_a, data_b, data_c], dim="time"))
        assert ds.time.size == 91


@pytest.mark.xfail(reason="Combining datasets with overlap is not yet supported")
def test_add_data_with_overlap_check_stored_dataset(bucket, datasets_with_overlap):
    time_a = pd.date_range("2012-01-01T00:00:00", "2012-01-31T00:00:00", freq="1d")
    time_b = pd.date_range("2012-01-29T00:00:00", "2012-04-30T00:00:00", freq="1d")
    time_c = pd.date_range("2012-04-16T00:00:00", "2012-09-30T00:00:00", freq="1d")

    values_a = np.zeros(len(time_a))
    values_b = np.full(len(time_b), 1)
    values_c = np.full(len(time_c), 2)

    attributes = create_attributes()

    data_a = xr.Dataset({"mf": ("time", values_a)}, coords={"time": time_a}, attrs=attributes)
    data_b = xr.Dataset({"mf": ("time", values_b)}, coords={"time": time_b}, attrs=attributes)
    data_c = xr.Dataset({"mf": ("time", values_c)}, coords={"time": time_c}, attrs=attributes)

    attributes = create_attributes()

    d = Datasource(uuid="abc123", bucket=bucket)

    d.add_data(metadata=attributes, data=data_a, data_type="surface", new_version=False, if_exists="combine")
    d.add_data(metadata=attributes, data=data_b, data_type="surface", new_version=False, if_exists="combine")
    d.add_data(metadata=attributes, data=data_c, data_type="surface", new_version=False, if_exists="combine")

    with d.get_data(version="v1") as ds:
        ds = ds.compute()
        n_days_expected = pd.date_range("2012-01-01T00:00:00", "2012-09-30T00:00:00", freq="1d").size
        assert ds.time.size == n_days_expected
        combined = xr.concat([data_a, data_b, data_c], dim="time").drop_duplicates("time")
        assert ds.equals(combined)


@pytest.mark.xfail(reason="Combining datasets with overlap is not yet supported")
def test_add_data_out_of_order(bucket, datasets_with_gaps):
    data_a, data_b, data_c = datasets_with_gaps
    attributes = create_attributes()

    d = Datasource(uuid="abc123", bucket=bucket)

    d.add_data(metadata=attributes, data=data_b, data_type="surface", new_version=False, if_exists="combine")
    d.add_data(metadata=attributes, data=data_a, data_type="surface", new_version=False, if_exists="combine")
    d.add_data(metadata=attributes, data=data_c, data_type="surface", new_version=False, if_exists="combine")

    assert d.data_keys() == [
        "2012-01-01-00:00:00+00:00_2012-01-31-00:00:59+00:00",
        "2012-04-01-00:00:00+00:00_2012-04-30-00:00:59+00:00",
        "2012-09-01-00:00:00+00:00_2012-09-30-00:00:59+00:00",
    ]

    expected = xr.concat([data_a, data_b, data_c], dim="time").drop_duplicates("time").sortby("time")

    ds = d.get_data(version="v1").compute()

    assert ds.time.size == expected.time.size
    assert ds.equals(expected)


@pytest.mark.xfail(reason="Data is currently not sorted during standardisation")
def test_add_data_out_of_order_no_combine(bucket, datasets_with_gaps):
    data_a, data_b, data_c = datasets_with_gaps
    attributes = create_attributes()

    d = Datasource(uuid="abc123", bucket=bucket)

    d.add_data(metadata=attributes, data=data_b, data_type="surface", new_version=False)
    d.add_data(metadata=attributes, data=data_a, data_type="surface", new_version=False)
    d.add_data(metadata=attributes, data=data_c, data_type="surface", new_version=False)

    assert d.data_keys() == [
        "2012-01-01-00:00:00+00:00_2012-01-31-00:00:59+00:00",
        "2012-04-01-00:00:00+00:00_2012-04-30-00:00:59+00:00",
        "2012-09-01-00:00:00+00:00_2012-09-30-00:00:59+00:00",
    ]

    expected = xr.concat([data_a, data_b, data_c], dim="time").drop_duplicates("time").sortby("time")

    ds = d.get_data(version="v1").compute()

    assert ds.time.size == expected.time.size
    assert ds.equals(expected)


def test_bytes_stored(data, bucket, datasource):
    d = datasource
    d.add_data(metadata=data["ch4"]["metadata"], data=data["ch4"]["data"], data_type="surface")

    d.save()

    assert abs(d.bytes_stored() - 9609) < 10

    d = Datasource(uuid="xyz456", bucket=bucket)

    assert d.bytes_stored() == 0
