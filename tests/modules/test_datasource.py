import datetime
import os
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray

from openghg.modules import CRDS, Datasource
from openghg.objectstore import get_local_bucket, get_object_names
from openghg.util import create_daterange_str

mocked_uuid = "00000000-0000-0000-00000-000000000000"
mocked_uuid2 = "10000000-0000-0000-00000-000000000001"

# Disable this for long strings below - Line break occurred before a binary operator (W503)
# flake8: noqa: W503


def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")


@pytest.fixture(scope="session")
def data():
    crds = CRDS()

    filename = "bsd.picarro.1minute.248m.dat"
    filepath = get_datapath(filename=filename, data_type="CRDS")

    combined_data = crds.read_data(data_filepath=filepath, site="bsd", network="DECC")

    return combined_data


@pytest.fixture
def datasource():
    return Datasource(
        name="test_name",
    )


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
    d = Datasource(name="test")

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    assert ch4_data["ch4"][0] == pytest.approx(1960.24)
    assert ch4_data["ch4 stdev"][0] == pytest.approx(0.236)
    assert ch4_data["ch4 n_meas"][0] == pytest.approx(26.0)

    d.add_data(metadata=metadata, data=ch4_data)

    date_key = "2014-01-30-10:52:30+00:00_2014-01-30-14:20:30+00:00"

    assert d._data[date_key]["ch4"].equals(ch4_data["ch4"])
    assert d._data[date_key]["ch4 stdev"].equals(ch4_data["ch4 stdev"])
    assert d._data[date_key]["ch4 n_meas"].equals(ch4_data["ch4 n_meas"])

    datasource_metadata = d.metadata()

    assert datasource_metadata["data_type"] == "timeseries"
    assert datasource_metadata["inlet"] == "248m"
    assert datasource_metadata["instrument"] == "picarro"
    assert datasource_metadata["port"] == "8"
    assert datasource_metadata["site"] == "bsd"
    assert datasource_metadata["species"] == "ch4"


def test_versioning(data):
    # Take head of data
    # Then add the full data, check versioning works correctly
    metadata = {"foo": "bar"}

    d = Datasource(name="foo")
    # Fix the UUID for the tests
    d._uuid = "4b91f73e-3d57-47e4-aa13-cb28c35d3b3d"

    ch4_data = data["ch4"]["data"]

    v1 = ch4_data.head(20)
    v2 = ch4_data.head(30)
    v3 = ch4_data.head(40)

    d.add_data(metadata=metadata, data=v1)

    d.save()

    d.add_data(metadata=metadata, data=v2)

    d.save()

    d.add_data(metadata=metadata, data=v3)

    d.save()

    keys = d.versions()

    assert (
        keys["v1"]["keys"]["2014-01-30-10:52:30+00:00_2014-01-30-12:20:30+00:00"]
        == "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v1/2014-01-30-10:52:30+00:00_2014-01-30-12:20:30+00:00"
    )

    assert list(keys["v2"]["keys"].values()) == [
        "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v2/2014-01-30-10:52:30+00:00_2014-01-30-13:12:30+00:00"
    ]

    assert list(keys["v3"]["keys"].values()) == [
        "data/uuid/4b91f73e-3d57-47e4-aa13-cb28c35d3b3d/v3/2014-01-30-10:52:30+00:00_2014-01-30-13:22:30+00:00"
    ]

    assert keys["v3"]["keys"] == keys["latest"]["keys"]


def test_get_dataframe_daterange():
    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)
    random_data = pd.DataFrame(
        data=np.random.randint(0, 100, size=(100, 4)),
        index=pd.date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
        columns=list("ABCD"),
    )

    d = Datasource(name="test")

    start, end = d.get_dataframe_daterange(random_data)

    assert start == pd.Timestamp("1970-01-01 01:01:00+0000")
    assert end == pd.Timestamp("1970-04-10 01:01:00+0000")


def test_save(mock_uuid2):
    bucket = get_local_bucket()

    datasource = Datasource(name="test_name")
    datasource.add_metadata(key="data_type", value="timeseries")
    datasource.save(bucket)

    prefix = f"{Datasource._datasource_root}/uuid/{datasource._uuid}"

    objs = get_object_names(bucket, prefix)

    assert objs[0].split("/")[-1] == mocked_uuid2


def test_save_footprint():
    bucket = get_local_bucket(empty=True)

    metadata = {"test": "testing123"}

    dir_path = os.path.dirname(__file__)
    test_data = "../data/emissions"
    filename = "WAO-20magl_EUROPE_201306_downsampled.nc"
    filepath = os.path.join(dir_path, test_data, filename)

    data = xarray.open_dataset(filepath)

    datasource = Datasource(name="test_name")
    datasource.add_data(metadata=metadata, data=data, data_type="footprint")
    datasource.save()

    prefix = f"{Datasource._datasource_root}/uuid/{datasource._uuid}"
    objs = get_object_names(bucket, prefix)

    datasource_2 = Datasource.load(bucket=bucket, key=objs[0])

    date_key = "2013-06-02-00:00:00+00:00_2013-06-30-00:00:00+00:00"

    data = datasource_2._data[date_key]

    assert float(data.pressure[0].values) == pytest.approx(1023.971)
    assert float(data.pressure[2].values) == pytest.approx(1009.940)
    assert float(data.pressure[-1].values) == pytest.approx(1021.303)


def test_add_metadata_key(datasource):
    datasource.add_metadata_key(key="foo", value=123)
    datasource.add_metadata_key(key="bar", value=456)

    assert datasource._metadata["foo"] == "123"
    assert datasource._metadata["bar"] == "456"


def test_add_metadata_lowercases_correctly(datasource):
    metadata = {"AAA": {"INLETS": {"inlet_A": "158m", "inlet_b": "12m"}, "some_metadata": {"OWNER": "foo", "eMAIL": "this@that"}}}

    datasource.add_metadata(metadata=metadata)

    assert datasource.metadata() == {
        "aaa": {"inlets": {"inlet_a": "158m", "inlet_b": "12m"}, "some_metadata": {"owner": "foo", "email": "this@that"}}
    }


def test_exists():
    d = Datasource(name="testing")
    d.save()

    exists = Datasource.exists(datasource_id=d.uuid())

    assert exists == True


def test_to_data(data):
    d = Datasource(name="testing_123")

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    assert ch4_data["ch4"][0] == pytest.approx(1960.24)
    assert ch4_data["ch4 stdev"][0] == pytest.approx(0.236)
    assert ch4_data["ch4 n_meas"][0] == pytest.approx(26.0)

    d.add_data(metadata=metadata, data=ch4_data, data_type="timeseries")

    obj_data = d.to_data()

    metadata = obj_data["metadata"]
    assert obj_data["name"] == "testing_123"
    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["time_resolution"] == "1_minute"
    assert metadata["inlet"] == "248m"
    assert metadata["data_type"] == "timeseries"
    assert len(obj_data["data_keys"]) == 0


def test_from_data(data):
    d = Datasource(name="testing_123")

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data, data_type="timeseries")

    obj_data = d.to_data()

    bucket = get_local_bucket()

    # Create a new object with the data from d
    d_2 = Datasource.from_data(bucket=bucket, data=obj_data, shallow=False)

    metadata = d_2.metadata()
    assert metadata["site"] == "bsd"
    assert metadata["instrument"] == "picarro"
    assert metadata["time_resolution"] == "1_minute"
    assert metadata["inlet"] == "248m"

    assert d_2.to_data() == d.to_data()


def test_incorrect_datatype_raises(data):
    d = Datasource(name="testing_123")

    metadata = data["ch4"]["metadata"]
    ch4_data = data["ch4"]["data"]

    with pytest.raises(TypeError):
        d.add_data(metadata=metadata, data=ch4_data, data_type="CRDS")


def test_update_daterange_replacement(data):
    metadata = {"foo": "bar"}

    d = Datasource(name="foo")

    ch4_data = data["ch4"]["data"]

    d.add_data(metadata=metadata, data=ch4_data)

    assert d._start_datetime == pd.Timestamp("2014-01-30 10:52:30+00:00")
    assert d._end_datetime == pd.Timestamp("2014-01-30 14:20:30+00:00")

    ch4_short = ch4_data.head(40)

    d._data = None

    d.add_data(metadata=metadata, data=ch4_short, overwrite=True)

    assert d._start_datetime == pd.Timestamp("2014-01-30 10:52:30+00:00")
    assert d._end_datetime == pd.Timestamp("2014-01-30 13:22:30+00:00")


def test_load_dataset():
    filename = "WAO-20magl_EUROPE_201306_small.nc"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/emissions"
    filepath = os.path.join(dir_path, test_data, filename)

    ds = xarray.load_dataset(filepath)

    metadata = {"some": "metadata"}

    d = Datasource("dataset_test")

    d.add_data(metadata=metadata, data=ds, data_type="footprint")

    d.save()

    keys = d._data_keys["latest"]["keys"]

    key = list(keys.values())[0]

    bucket = get_local_bucket()

    loaded_ds = Datasource.load_dataset(bucket=bucket, key=key)

    assert loaded_ds.equals(ds)


def test_search_metadata():
    d = Datasource(name="test_search")

    d._metadata = {"unladen": "swallow", "spam": "eggs"}

    assert d.search_metadata("swallow") == True
    assert d.search_metadata("eggs") == True
    assert d.search_metadata("eggs") == True
    assert d.search_metadata("Swallow") == True

    assert d.search_metadata("beans") == False
    assert d.search_metadata("flamingo") == False


def test_search_metadata_find_all():
    d = Datasource(name="test_search")

    d._metadata = {"inlet": "100m", "instrument": "violin", "car": "toyota"}

    result = d.search_metadata(search_terms=["100m", "violin", "toyota"], find_all=True)

    assert result is True

    result = d.search_metadata(search_terms=["100m", "violin", "toyota", "swallow"], find_all=True)

    assert result is False

def test_search_metadata_finds_recursively():
    d = Datasource(name="test_search")

    d._metadata = {"car": "toyota", "inlets": {"inlet_a": "45m", "inlet_b": "3580m"}}

    result = d.search_metadata(search_terms=["45m", "3580m", "toyota"], find_all=True)

    assert result is True

    result = d.search_metadata(search_terms=["100m", "violin", "toyota", "swallow"], find_all=True)

    assert result is False

    result = d.search_metadata(search_terms=["100m", "violin", "toyota", "swallow"], find_all=False)

    assert result is True


def test_set_rank():
    d = Datasource()

    daterange = "2027-08-01-00:00:00_2027-12-01-00:00:00"

    d.set_rank(rank=1, daterange=daterange)

    assert d._rank[1] == ["2027-08-01-00:00:00_2027-12-01-00:00:00"]


def test_set_incorrect_rank_raises():
    d = Datasource()

    daterange = "2027-08-01-00:00:00_2027-12-01-00:00:00"

    with pytest.raises(ValueError):
        d.set_rank(rank=42, daterange=daterange)


def test_setting_overlapping_dateranges():
    d = Datasource()

    daterange = "2027-08-01-00:00:00_2027-12-01-00:00:00"

    d.set_rank(rank=1, daterange=daterange)

    assert d._rank[1] == ["2027-08-01-00:00:00_2027-12-01-00:00:00"]

    daterange_two = "2027-11-01-00:00:00_2028-06-01-00:00:00"

    d.set_rank(rank=1, daterange=daterange_two)

    assert d._rank[1] == ["2027-08-01-00:00:00+00:00_2028-06-01-00:00:00+00:00"]


def test_combining_single_dateranges_returns():
    d = Datasource()

    daterange = "2027-08-01-00:00:00_2027-12-01-00:00:00"

    combined = d.combine_dateranges(dateranges=[daterange])

    assert combined[0] == daterange


def test_combining_overlapping_dateranges():
    d = Datasource()

    daterange_1 = "2001-01-01-00:00:00_2001-03-01-00:00:00"
    daterange_2 = "2001-02-01-00:00:00_2001-06-01-00:00:00"

    dateranges = [daterange_1, daterange_2]

    combined = d.combine_dateranges(dateranges=dateranges)

    assert combined == ["2001-01-01-00:00:00+00:00_2001-06-01-00:00:00+00:00"]

    daterange_1 = "2001-01-01-00:00:00_2001-03-01-00:00:00"
    daterange_2 = "2001-02-01-00:00:00_2001-06-01-00:00:00"
    daterange_3 = "2001-05-01-00:00:00_2001-08-01-00:00:00"
    daterange_4 = "2004-05-01-00:00:00_2004-08-01-00:00:00"
    daterange_5 = "2004-04-01-00:00:00_2004-09-01-00:00:00"
    daterange_6 = "2007-04-01-00:00:00_2007-09-01-00:00:00"

    dateranges = [daterange_1, daterange_2, daterange_3, daterange_4, daterange_5, daterange_6]

    combined = d.combine_dateranges(dateranges=dateranges)

    assert combined == [
        "2001-01-01-00:00:00+00:00_2001-08-01-00:00:00+00:00",
        "2004-04-01-00:00:00+00:00_2004-09-01-00:00:00+00:00",
        "2007-04-01-00:00:00+00:00_2007-09-01-00:00:00+00:00",
    ]


def test_combining_no_overlap():
    d = Datasource()
    daterange_1 = "2001-01-01-00:00:00_2001-03-01-00:00:00"
    daterange_2 = "2011-02-01-00:00:00_2011-06-01-00:00:00"

    dateranges = [daterange_1, daterange_2]

    combined = d.combine_dateranges(dateranges=dateranges)

    assert combined == [
        "2001-01-01-00:00:00+00:00_2001-03-01-00:00:00+00:00",
        "2011-02-01-00:00:00+00:00_2011-06-01-00:00:00+00:00",
    ]


def test_split_daterange_str():
    d = Datasource()

    start_true = pd.Timestamp("2001-01-01-00:00:00", tz="UTC")
    end_true = pd.Timestamp("2001-03-01-00:00:00", tz="UTC")

    daterange_1 = "2001-01-01-00:00:00_2001-03-01-00:00:00"

    start, end = d.split_datrange_str(daterange_str=daterange_1)

    assert start_true == start
    assert end_true == end


def test_in_daterange(data):
    metadata = data["ch4"]["metadata"]
    data = data["ch4"]["data"]

    d = Datasource()
    d.add_data(metadata=metadata, data=data)
    d.save()

    start = pd.Timestamp("2014-1-1")
    end = pd.Timestamp("2014-2-1")

    daterange = create_daterange_str(start=start, end=end)

    d._data_keys["latest"]["2014-01-30-10:52:30+00:00_2014-01-30-14:20:30+00:00"] = [
        "data/uuid/ace2bb89-7618-4104-9404-a329c2bcd318/v1/2014-01-30-10:52:30+00:00_2014-01-30-14:20:30+00:00"
    ]
    d._data_keys["latest"]["2015-01-30-10:52:30+00:00_2016-01-30-14:20:30+00:00"] = [
        "data/uuid/ace2bb89-7618-4104-9404-a329c2bcd318/v1/2015-01-30-10:52:30+00:00_2016-01-30-14:20:30+00:00"
    ]
    d._data_keys["latest"]["2016-01-31-10:52:30+00:00_2017-01-30-14:20:30+00:00"] = [
        "data/uuid/ace2bb89-7618-4104-9404-a329c2bcd318/v1/2016-01-31-10:52:30+00:00_2017-01-30-14:20:30+00:00"
    ]

    keys = d.keys_in_daterange(daterange=daterange)

    assert keys[0].split("/")[-1] == "2014-01-30-10:52:30+00:00_2014-01-30-14:20:30+00:00"


def test_shallow_then_load_data(data):
    metadata = data["ch4"]["metadata"]
    data = data["ch4"]["data"]

    d = Datasource()
    d.add_data(metadata=metadata, data=data)
    d.save()

    new_d = Datasource.load(uuid=d.uuid(), shallow=True)

    assert not new_d._data

    ds_data = new_d.data()

    assert ds_data

    ch4_data = ds_data["2014-01-30-10:52:30+00:00_2014-01-30-14:20:30+00:00"]

    assert ch4_data.time[0] == pd.Timestamp("2014-01-30T10:52:30")
