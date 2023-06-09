import datetime
import os
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from addict import Dict as aDict
from helpers import get_surface_datapath
from openghg.objectstore import get_bucket, get_object_names
from openghg.standardise.surface import parse_crds
from openghg.store.base import Datasource
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
def datasource():
    return Datasource()


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


def test_add_data(data):
    d = Datasource()

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    assert ch4_data["ch4"][0] == pytest.approx(1959.55)
    assert ch4_data["ch4_variability"][0] == pytest.approx(0.79)
    assert ch4_data["ch4_number_of_observations"][0] == pytest.approx(26.0)

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")
    d.save()
    bucket = get_bucket()

    data_chunks = [Datasource.load_dataset(bucket=bucket, key=k) for k in d.data_keys()]

    # Now read it out and make sure it's what we expect
    combined = xr.concat(data_chunks, dim="time")

    assert combined.equals(ch4_data)

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
        "latest_version": "v1",
    }

    assert d.metadata() == expected_metadata


def test_versioning(capfd):
    min_tac_filepath = get_surface_datapath(filename="tac.picarro.1minute.100m.min.dat", source_format="CRDS")
    detailed_tac_filepath = get_surface_datapath(filename="tac.picarro.1minute.100m.201407.dat", source_format="CRDS")

    min_data = parse_crds(data_filepath=min_tac_filepath, site="tac", inlet="100m", network="decc")

    # Take head of data
    # Then add the full data, check versioning works correctly
    metadata = {"foo": "bar"}

    d = Datasource()
    # Fix the UUID for the tests
    d._uuid = "4b91f73e-3d57-47e4-aa13-cb28c35d3b3d"

    min_ch4_data = min_data["ch4"]["data"]

    d.add_data(metadata=metadata, data=min_ch4_data, data_type="surface")

    d.save()

    min_keys = d.versions()

    expected_v1 = {
        "2012-07-26-13:51:30+00:00_2012-07-28-02:45:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v1/2012-07-26-13:51:30+00:00_2012-07-28-02:45:30+00:00",
        "2013-07-26-13:51:30+00:00_2013-07-28-13:02:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v1/2013-07-26-13:51:30+00:00_2013-07-28-13:02:30+00:00",
        "2014-07-26-13:51:30+00:00_2014-07-28-13:02:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v1/2014-07-26-13:51:30+00:00_2014-07-28-13:02:30+00:00",
        "2015-07-26-13:51:30+00:00_2015-07-28-13:02:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v1/2015-07-26-13:51:30+00:00_2015-07-28-13:02:30+00:00",
        "2016-07-26-13:51:30+00:00_2016-07-28-13:02:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v1/2016-07-26-13:51:30+00:00_2016-07-28-13:02:30+00:00",
        "2017-07-26-13:51:30+00:00_2017-07-28-13:02:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v1/2017-07-26-13:51:30+00:00_2017-07-28-13:02:30+00:00",
        "2018-07-26-13:51:30+00:00_2018-07-28-13:02:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v1/2018-07-26-13:51:30+00:00_2018-07-28-13:02:30+00:00",
        "2019-06-01-07:54:30+00:00_2019-06-21-07:13:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v1/2019-06-01-07:54:30+00:00_2019-06-21-07:13:30+00:00",
        "2020-06-21-17:53:30+00:00_2020-07-04-09:58:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v1/2020-06-21-17:53:30+00:00_2020-07-04-09:58:30+00:00",
    }

    assert min_keys["v1"]["keys"] == expected_v1

    detailed_data = parse_crds(data_filepath=detailed_tac_filepath, site="tac", inlet="100m", network="decc")

    detailed_ch4_data = detailed_data["ch4"]["data"]

    # Check new version can be created and stored (using appropriate flag)
    d.add_data(metadata=metadata, data=detailed_ch4_data, data_type="surface", if_exists="new")

    d.save(new_version=True)

    detailed_keys = d.versions()

    expected_v2 = {
        # "2012-07-26-13:51:30+00:00_2012-07-28-02:45:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v2/2012-07-26-13:51:30+00:00_2012-07-28-02:45:30+00:00",
        # "2013-07-26-13:51:30+00:00_2013-07-28-13:02:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v2/2013-07-26-13:51:30+00:00_2013-07-28-13:02:30+00:00",
        # "2015-07-26-13:51:30+00:00_2015-07-28-13:02:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v2/2015-07-26-13:51:30+00:00_2015-07-28-13:02:30+00:00",
        # "2016-07-26-13:51:30+00:00_2016-07-28-13:02:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v2/2016-07-26-13:51:30+00:00_2016-07-28-13:02:30+00:00",
        # "2017-07-26-13:51:30+00:00_2017-07-28-13:02:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v2/2017-07-26-13:51:30+00:00_2017-07-28-13:02:30+00:00",
        # "2018-07-26-13:51:30+00:00_2018-07-28-13:02:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v2/2018-07-26-13:51:30+00:00_2018-07-28-13:02:30+00:00",
        # "2019-06-01-07:54:30+00:00_2019-06-21-07:13:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v2/2019-06-01-07:54:30+00:00_2019-06-21-07:13:30+00:00",
        # "2020-06-21-17:53:30+00:00_2020-07-04-09:58:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v2/2020-06-21-17:53:30+00:00_2020-07-04-09:58:30+00:00",
        "2014-06-30-00:06:30+00:00_2014-08-01-23:49:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v2/2014-06-30-00:06:30+00:00_2014-08-01-23:49:30+00:00",
    }

    assert detailed_keys["v2"]["keys"] == expected_v2

    # TODO: Add case for if_exists="replace" which should look more like original case above after updates


def test_replace_version():
    """Test that new data can replace previous data. This involves deleting the previous version
    data and copying across the new data.
    """
    min_tac_filepath = get_surface_datapath(filename="tac.picarro.1minute.100m.min.dat", source_format="CRDS")
    detailed_tac_filepath = get_surface_datapath(filename="tac.picarro.1minute.100m.201407.dat", source_format="CRDS")

    min_data = parse_crds(data_filepath=min_tac_filepath, site="tac", inlet="100m", network="decc")

    min_ch4_data = min_data["ch4"]["data"]
    metadata = {"foo": "bar"}

    d = Datasource()
    # Fix the UUID for the tests
    d._uuid = "4b91f73e-3d57-47e4-aa13-cb28c35d3b3d"
    d.add_data(metadata=metadata, data=min_ch4_data, data_type="surface")

    # Save initial data
    d.save()

    detailed_data = parse_crds(data_filepath=detailed_tac_filepath, site="tac", inlet="100m", network="decc")

    detailed_ch4_data = detailed_data["ch4"]["data"]

    # Check new version can be created and stored (using appropriate flag)
    d.add_data(metadata=metadata, data=detailed_ch4_data, data_type="surface", if_exists="new")

    # Save and overwrite with new data
    d.save(new_version=False)

    detailed_keys = d.versions()

    expected_new_v1 = {
        "2014-06-30-00:06:30+00:00_2014-08-01-23:49:30+00:00": "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v1/2014-06-30-00:06:30+00:00_2014-08-01-23:49:30+00:00",
    }

    assert detailed_keys["v1"]["keys"] == expected_new_v1

    # TODO: Add case for if_exists="replace" which should look more like original case above after updates


def test_get_dataframe_daterange():
    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)
    random_data = pd.DataFrame(
        data=np.random.randint(0, 100, size=(100, 4)),
        index=pd.date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
        columns=list("ABCD"),
    )

    d = Datasource()

    start, end = d.get_dataframe_daterange(random_data)

    assert start == pd.Timestamp("1970-01-01 01:01:00+0000")
    assert end == pd.Timestamp("1970-04-10 01:01:00+0000")


def test_save(mock_uuid2):
    bucket = get_bucket()

    datasource = Datasource()
    datasource.add_metadata_key(key="data_type", value="surface")
    datasource.save(bucket)

    prefix = f"{Datasource._datasource_root}/uuid/{datasource._uuid}"

    objs = get_object_names(bucket, prefix)

    assert objs[0].split("/")[-1] == mocked_uuid2


def test_save_footprint():
    bucket = get_bucket()

    metadata = {"test": "testing123", "start_date": "2013-06-02", "end_date": "2013-06-30"}

    dir_path = os.path.dirname(__file__)
    test_data = "../data/emissions"
    filename = "WAO-20magl_EUROPE_201306_downsampled.nc"
    filepath = os.path.join(dir_path, test_data, filename)

    data = xr.open_dataset(filepath)

    datasource = Datasource()
    datasource.add_data(data=data, metadata=metadata, data_type="footprints")
    datasource.save()

    prefix = f"{Datasource._datasource_root}/uuid/{datasource._uuid}"
    objs = get_object_names(bucket, prefix)

    datasource_2 = Datasource.load(bucket=bucket, key=objs[0])

    date_key = "2013-06-02-00:00:00+00:00_2013-06-30-00:00:00+00:00"

    data = datasource_2._data[date_key]

    assert float(data.pressure[0].values) == pytest.approx(1023.971)
    assert float(data.pressure[2].values) == pytest.approx(1009.940)
    assert float(data.pressure[-1].values) == pytest.approx(1021.303)

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


def test_exists():
    d = Datasource()
    d.save()

    exists = Datasource.exists(datasource_id=d.uuid())

    assert exists == True


def test_to_data(data):
    d = Datasource()

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")

    obj_data = d.to_data()

    metadata = obj_data["metadata"]
    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["sampling_period"] == "60.0"
    assert metadata["inlet"] == "248m"
    assert metadata["data_type"] == "surface"
    assert len(obj_data["data_keys"]) == 0


def test_from_data(data):
    d = Datasource()

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")
    d.save()

    obj_data = d.to_data()

    bucket = get_bucket()

    # Create a new object with the data from d
    d_2 = Datasource.from_data(bucket=bucket, data=obj_data, shallow=False)

    metadata = d_2.metadata()
    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["sampling_period"] == "60.0"
    assert metadata["inlet"] == "248m"

    assert sorted(d_2.data_keys()) == sorted(d.data_keys())
    assert d_2.metadata() == d.metadata()


def test_incorrect_datatype_raises(data):
    d = Datasource()

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    with pytest.raises(TypeError):
        d.add_data(metadata=metadata, data=ch4_data, data_type="CRDS")


def test_update_daterange_replacement(data):
    metadata = {"foo": "bar"}

    d = Datasource()

    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="surface")

    assert d._start_date == pd.Timestamp("2014-01-30 11:12:30+00:00")
    assert d._end_date == pd.Timestamp("2020-12-01 22:31:30+00:00")

    ch4_short = ch4_data.head(40)

    d._data = None

    d.add_data(metadata=metadata, data=ch4_short, data_type="surface")

    assert d._start_date == pd.Timestamp("2014-01-30 11:12:30+00:00")
    assert d._end_date == pd.Timestamp("2016-04-02 06:55:30+00:00")


def test_load_dataset():
    filename = "WAO-20magl_EUROPE_201306_small.nc"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/emissions"
    filepath = os.path.join(dir_path, test_data, filename)

    ds = xr.load_dataset(filepath)

    metadata = {"test": "testing123", "start_date": "2013-06-02", "end_date": "2013-06-30"}

    d = Datasource()

    d.add_data(metadata=metadata, data=ds, data_type="footprints")

    d.save()

    keys = d._data_keys["latest"]["keys"]

    key = list(keys.values())[0]

    bucket = get_bucket()

    loaded_ds = Datasource.load_dataset(bucket=bucket, key=key)

    assert loaded_ds.equals(ds)


def test_search_metadata():
    d = Datasource()

    d._metadata = {"unladen": "swallow", "spam": "eggs"}

    assert d.search_metadata(unladen="swallow") == True
    assert d.search_metadata(spam="eggs") == True
    assert d.search_metadata(unladen="Swallow") == True

    assert d.search_metadata(giraffe="beans") == False
    assert d.search_metadata(bird="flamingo") == False


def test_dated_metadata_search():
    d = Datasource()

    start = pd.Timestamp("2001-01-01-00:00:00", tz="UTC")
    end = pd.Timestamp("2001-03-01-00:00:00", tz="UTC")

    d._start_date = start
    d._end_date = end

    d._metadata = {"inlet": "100m", "instrument": "violin", "site": "timbuktu"}

    assert d.search_metadata(inlet="100m", instrument="violin") == True

    assert (
        d.search_metadata(
            search_terms=["100m", "violin"],
            start_date=pd.Timestamp("2015-01-01"),
            end_date=pd.Timestamp("2021-01-01"),
        )
        == False
    )
    assert (
        d.search_metadata(
            inlet="100m",
            instrument="violin",
            start_date=pd.Timestamp("2001-01-01"),
            end_date=pd.Timestamp("2002-01-01"),
        )
        == True
    )


def test_search_metadata_find_all():
    d = Datasource()

    d._metadata = {"inlet": "100m", "instrument": "violin", "car": "toyota"}

    result = d.search_metadata(inlet="100m", instrument="violin", car="toyota", find_all=True)

    assert result is True

    result = d.search_metadata(inlet="100m", instrument="violin", car="subaru", find_all=True)

    assert result is False


@pytest.mark.skip(reason="We don't currently have recursive search functionality")
def test_search_metadata_finds_recursively():
    d = Datasource()

    d._metadata = {"car": "toyota", "inlets": {"inlet_a": "45m", "inlet_b": "3580m"}}

    result = d.search_metadata(search_terms=["45m", "3580m", "toyota"], find_all=True)

    assert result is True

    result = d.search_metadata(search_terms=["100m", "violin", "toyota", "swallow"], find_all=True)

    assert result is False

    result = d.search_metadata(search_terms=["100m", "violin", "toyota", "swallow"], find_all=False)

    assert result is True


def test_in_daterange(data):
    metadata = data["ch4"]["metadata"]
    data = data["ch4"]["data"]

    d = Datasource()
    d._uuid = "test-id-123"
    d.add_data(metadata=metadata, data=data, data_type="surface")
    d.save()

    expected_keys = [
        "data/uuid/test-id-123/v1/2014-01-30-11:12:30+00:00_2014-11-30-11:24:29+00:00",
        "data/uuid/test-id-123/v1/2015-01-30-11:12:30+00:00_2015-11-30-11:24:29+00:00",
        "data/uuid/test-id-123/v1/2016-04-02-06:52:30+00:00_2016-11-02-12:55:29+00:00",
        "data/uuid/test-id-123/v1/2017-02-18-06:36:30+00:00_2017-12-18-15:42:29+00:00",
        "data/uuid/test-id-123/v1/2018-02-18-15:42:30+00:00_2018-12-18-15:43:29+00:00",
        "data/uuid/test-id-123/v1/2019-02-03-17:38:30+00:00_2019-12-09-10:48:29+00:00",
        "data/uuid/test-id-123/v1/2020-02-01-18:08:30+00:00_2020-12-01-22:32:29+00:00",
    ]

    assert d.data_keys() == expected_keys

    start = pd.Timestamp("2014-1-1")
    end = pd.Timestamp("2014-2-1")
    daterange = create_daterange_str(start=start, end=end)

    dated_keys = d.keys_in_daterange_str(daterange=daterange)

    assert dated_keys[0].split("/")[-1] == "2014-01-30-11:12:30+00:00_2014-11-30-11:24:29+00:00"


def test_shallow_then_load_data(data):
    metadata = data["ch4"]["metadata"]
    data = data["ch4"]["data"]

    d = Datasource()
    d.add_data(metadata=metadata, data=data, data_type="surface")
    d.save()

    new_d = Datasource.load(uuid=d.uuid(), shallow=True)

    assert not new_d._data

    ds_data = new_d.data()

    assert ds_data

    ch4_data = ds_data["2014-01-30-11:12:30+00:00_2014-11-30-11:24:29+00:00"]

    assert ch4_data.time[0] == pd.Timestamp("2014-01-30-11:12:30")


def test_key_date_compare():
    d = Datasource()

    keys = {
        "2014-01-30-11:12:30+00:00_2014-11-30-11:23:30+00:00": "data/uuid/test-uid/v1/2014-01-30-11:12:30+00:00_2014-11-30-11:23:30+00:00",
        "2015-01-30-11:12:30+00:00_2015-11-30-11:23:30+00:00": "data/uuid/test-uid/v1/2015-01-30-11:12:30+00:00_2015-11-30-11:23:30+00:00",
        "2016-04-02-06:52:30+00:00_2016-11-02-12:54:30+00:00": "data/uuid/test-uid/v1/2016-04-02-06:52:30+00:00_2016-11-02-12:54:30+00:00",
        "2017-02-18-06:36:30+00:00_2017-12-18-15:41:30+00:00": "data/uuid/test-uid/v1/2017-02-18-06:36:30+00:00_2017-12-18-15:41:30+00:00",
        "2018-02-18-15:42:30+00:00_2018-12-18-15:42:30+00:00": "data/uuid/test-uid/v1/2018-02-18-15:42:30+00:00_2018-12-18-15:42:30+00:00",
        "2019-02-03-17:38:30+00:00_2019-12-09-10:47:30+00:00": "data/uuid/test-uid/v1/2019-02-03-17:38:30+00:00_2019-12-09-10:47:30+00:00",
        "2020-02-01-18:08:30+00:00_2020-12-01-22:31:30+00:00": "data/uuid/test-uid/v1/2020-02-01-18:08:30+00:00_2020-12-01-22:31:30+00:00",
    }

    start = timestamp_tzaware("2014-01-01")
    end = timestamp_tzaware("2018-01-01")

    in_date = d.key_date_compare(keys=keys, start_date=start, end_date=end)

    expected = [
        "data/uuid/test-uid/v1/2014-01-30-11:12:30+00:00_2014-11-30-11:23:30+00:00",
        "data/uuid/test-uid/v1/2015-01-30-11:12:30+00:00_2015-11-30-11:23:30+00:00",
        "data/uuid/test-uid/v1/2016-04-02-06:52:30+00:00_2016-11-02-12:54:30+00:00",
        "data/uuid/test-uid/v1/2017-02-18-06:36:30+00:00_2017-12-18-15:41:30+00:00",
    ]

    assert in_date == expected

    start = timestamp_tzaware("2053-01-01")
    end = timestamp_tzaware("2092-01-01")

    in_date = d.key_date_compare(keys=keys, start_date=start, end_date=end)

    assert not in_date

    error_key = {"2014-01-30-11:12:30+00:00_2014-11-30-11:23:30+00:00_2014-11-30-11:23:30+00:00": "broken"}

    with pytest.raises(ValueError):
        in_date = d.key_date_compare(keys=error_key, start_date=start, end_date=end)
