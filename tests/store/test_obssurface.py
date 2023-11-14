import os
import json
import pytest
import xarray as xr
from pandas import Timestamp
from helpers import attributes_checker_obssurface, get_surface_datapath, clear_test_stores
from pathlib import Path
from openghg.objectstore import (
    exists,
    get_bucket,
    get_writable_bucket,
    set_object_from_json,
    get_object_from_json,
)
from openghg.store import ObsSurface
from openghg.store.base import Datasource
from openghg.objectstore.metastore import open_metastore
from openghg.retrieve import search_surface
from openghg.standardise import standardise_surface, standardise_from_binary_data
from openghg.util import create_daterange_str
from pandas import Timestamp


@pytest.fixture
def bucket():
    return get_bucket()


def test_raising_error_doesnt_save_to_store(mocker, bucket):
    clear_test_stores()
    bucket = get_writable_bucket(name="user")

    key = ""
    with pytest.raises(ValueError):
        with open_metastore(data_type="surface", bucket=bucket) as obs:
            key = "abc123"
            assert not exists(bucket=bucket, key=key)
            # Here we're testing to see what happens if a user does something
            # with obs that results in an exception being raised that isn't internal
            # to our processing functions
            raise ValueError("Oops")

    assert not exists(bucket=bucket, key=key)

    mocker.patch("openghg.store.base._base.BaseStore.assign_data", side_effect=ValueError("Read error."))

    one_min = get_surface_datapath("tac.picarro.1minute.100m.test.dat", source_format="CRDS")

    with pytest.raises(ValueError):
        standardise_surface(store="user", filepath=one_min, site="tac", network="decc", source_format="CRDS")

    assert not exists(bucket=bucket, key=key)


def test_different_sampling_periods_diff_datasources():
    clear_test_stores()

    one_min = get_surface_datapath("tac.picarro.1minute.100m.test.dat", source_format="CRDS")
    one_min_res = standardise_surface(
        store="user", filepath=one_min, site="tac", network="decc", source_format="CRDS"
    )

    min_uuids = one_min_res["processed"]["tac.picarro.1minute.100m.test.dat"]
    for sp, data in min_uuids.items():
        assert data["new"] is True

    one_hour = get_surface_datapath("tac.picarro.hourly.100m.test.dat", source_format="CRDS")
    one_hour_res = standardise_surface(
        store="user", filepath=one_hour, site="tac", network="decc", source_format="CRDS"
    )

    hour_uuids = one_hour_res["processed"]["tac.picarro.hourly.100m.test.dat"]
    for sp, data in hour_uuids.items():
        assert data["new"] is True


def test_same_source_data_same_datasource():
    site = "tac"
    network = "DECC"
    source_format = "CRDS"

    tac_path1 = get_surface_datapath(filename="tac.picarro.1minute.100m.201208.dat", source_format="CRDS")
    tac_path2 = get_surface_datapath(filename="tac.picarro.1minute.100m.201407.dat", source_format="CRDS")

    res = standardise_surface(
        store="user",
        filepath=tac_path1,
        source_format=source_format,
        site=site,
        network=network,
        overwrite=True,
    )

    res_2 = standardise_surface(
        store="user",
        filepath=tac_path2,
        source_format=source_format,
        site=site,
        network=network,
        overwrite=True,
    )

    proc_data = res["processed"]["tac.picarro.1minute.100m.201208.dat"]
    proc_data_2 = res_2["processed"]["tac.picarro.1minute.100m.201407.dat"]

    assert proc_data["ch4"]["uuid"] == proc_data_2["ch4"]["uuid"]
    assert proc_data["ch4"]["uuid"] == proc_data_2["ch4"]["uuid"]

    assert proc_data["co2"]["uuid"] == proc_data_2["co2"]["uuid"]
    assert proc_data["co2"]["uuid"] == proc_data_2["co2"]["uuid"]


def test_read_data(mocker):
    clear_test_stores()

    # Get some bytes
    filepath = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")
    binary_bsd = filepath.read_bytes()

    metadata = {
        "source_format": "CRDS",
        "site": "bsd",
        "network": "DECC",
    }

    file_metadata = {"filename": "bsd.picarro.1minute.248m.min.dat"}

    result = standardise_from_binary_data(
        store="user",
        data_type="surface",
        binary_data=binary_bsd,
        metadata=metadata,
        file_metadata=file_metadata,
    )

    species = ["ch4", "co2", "co"]
    for k, v in result["processed"]["bsd.picarro.1minute.248m.min.dat"].items():
        assert k in species
        assert v["new"] is True

    with pytest.raises(ValueError):
        metadata = {}
        standardise_from_binary_data(
            store="user",
            data_type="surface",
            binary_data=binary_bsd,
            metadata=metadata,
            file_metadata=file_metadata,
        )

    with pytest.raises(KeyError):
        file_metadata = {}
        standardise_from_binary_data(
            store="user",
            data_type="surface",
            binary_data=binary_bsd,
            metadata=metadata,
            file_metadata=file_metadata,
        )


@pytest.mark.parametrize("sampling_period", ["60", 60, "60000000000", "twelve-thousand"])
def test_read_CRDS_incorrect_sampling_period_raises(sampling_period):
    clear_test_stores()

    filepath = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")

    with pytest.raises(ValueError) as exec_info:
        standardise_surface(
            store="user",
            filepath=filepath,
            source_format="CRDS",
            site="bsd",
            network="DECC",
            sampling_period=sampling_period,
        )
        assert "Invalid sampling period" in str(exec_info) or "Could not evaluate sampling period" in str(
            exec_info
        )


def test_read_CRDS(bucket):
    filepath = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")
    results = standardise_surface(
        store="user", filepath=filepath, source_format="CRDS", site="bsd", network="DECC"
    )

    keys = results["processed"]["bsd.picarro.1minute.248m.min.dat"].keys()

    assert sorted(keys) == ["ch4", "co", "co2"]

    # Load up the assigned Datasources and check they contain the correct data
    data = results["processed"]["bsd.picarro.1minute.248m.min.dat"]

    uid = data["ch4"]["uuid"]

    datasource = Datasource(bucket=bucket, uuid=uid)

    assert datasource.data_keys() == ["2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00"]

    # Values here have changed from the previous test as we're now looking at the whole
    # dataset rather than the first year
    with datasource.get_data(version="latest") as ch4_data:
        assert ch4_data.time[0] == Timestamp("2014-01-30T11:12:30")
        assert ch4_data["ch4"][0] == 1959.55
        assert ch4_data["ch4"][-1] == 1955.93
        assert ch4_data["ch4_variability"][-1] == 0.232
        assert ch4_data["ch4_number_of_observations"][-1] == 23.0

    filepath = get_surface_datapath(filename="bsd.picarro.1minute.248m.future.dat", source_format="CRDS")

    results = standardise_surface(
        store="user", filepath=filepath, source_format="CRDS", site="bsd", network="DECC"
    )

    # Let's load the same Datasource again and check that the data has been updated
    datasource = Datasource(bucket=bucket, uuid=uid)

    assert datasource.data_keys() == [
        "2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00",
        "2023-01-30-13:56:30+00:00_2023-01-30-14:21:29+00:00",
    ]


def test_read_GC(bucket):
    clear_test_stores()

    data_filepath = get_surface_datapath(filename="capegrim-medusa.18.C", source_format="GC")
    precision_filepath = get_surface_datapath(filename="capegrim-medusa.18.precisions.C", source_format="GC")

    results = standardise_surface(
        store="user",
        filepath=(data_filepath, precision_filepath),
        source_format="GCWERKS",
        site="CGO",
        network="AGAGE",
    )

    # 30/11/2021: Species labels were updated to be standardised in line with variable naming
    # This list of expected labels was updated.
    expected_keys = [
        "c2cl4_70m",
        "c2f6_70m",
        "c2h2_70m",
        "c2h6_70m",
        "c2hcl3_70m",
        "c3f8_70m",
        "c3h8_70m",
        "c4f10_70m",
        "c4f8_70m",
        "c6f14_70m",
        "c6h5ch3_70m",
        "c6h6_70m",
        "cc3h8_70m",
        "ccl4_70m",
        "cf4_70m",
        "cfc112_70m",
        "cfc113_70m",
        "cfc114_70m",
        "cfc115_70m",
        "cfc11_70m",
        "cfc12_70m",
        "cfc13_70m",
        "ch2br2_70m",
        "ch2cl2_70m",
        "ch3br_70m",
        "ch3ccl3_70m",
        "ch3cl_70m",
        "ch3i_70m",
        "chbr3_70m",
        "chcl3_70m",
        "cos_70m",
        "desflurane_70m",
        "halon1211_70m",
        "halon1301_70m",
        "halon2402_70m",
        "hcfc124_70m",
        "hcfc132b_70m",
        "hcfc133a_70m",
        "hcfc141b_70m",
        "hcfc142b_70m",
        "hcfc22_70m",
        "hfc125_70m",
        "hfc134a_70m",
        "hfc143a_70m",
        "hfc152a_70m",
        "hfc227ea_70m",
        "hfc236fa_70m",
        "hfc23_70m",
        "hfc245fa_70m",
        "hfc32_70m",
        "hfc365mfc_70m",
        "hfc4310mee_70m",
        "nf3_70m",
        "sf5cf3_70m",
        "sf6_70m",
        "so2f2_70m",
    ]

    assert sorted(list(results["processed"]["capegrim-medusa.18.C"].keys())) == expected_keys

    # Load in some data
    uuid = results["processed"]["capegrim-medusa.18.C"]["hfc152a_70m"]["uuid"]

    hfc_datasource = Datasource(bucket=bucket, uuid=uuid)

    with hfc_datasource.get_data(version="latest") as ds:
        # hfc152a_data = hfc152a_data["2018-01-01-02:24:00+00:00_2018-01-31-23:52:59+00:00"]

        assert ds.time[0] == Timestamp("2018-01-01T02:24:00")
        assert ds["hfc152a"][0] == 4.409
        assert ds["hfc152a_repeatability"][0] == 0.03557
        assert ds["hfc152a_status_flag"][0] == 0
        assert ds["hfc152a_integration_flag"][0] == 0

        assert ds.time[-1] == Timestamp("2018-01-31T23:33:00")
        assert ds["hfc152a"][-1] == 4.262
        assert ds["hfc152a_repeatability"][-1] == 0.03271
        assert ds["hfc152a_status_flag"][-1] == 0
        assert ds["hfc152a_integration_flag"][-1] == 0

        hfc152a_attrs = ds.attrs

    # Check we have the Datasource info saved
    with open_metastore(data_type="surface", bucket=bucket) as metastore:
        uuids = metastore.select("uuid")

        attrs = hfc152a_attrs

        assert attributes_checker_obssurface(attrs=attrs, species="hfc152a")

        # # Now test that if we add more data it adds it to the same Datasource
        uuid_one = uuids[0]  # metastore.search()[0]['uuid']

    datasource = Datasource(bucket=bucket, uuid=uuid_one)

    assert datasource.data_keys() == ["2018-01-01-02:24:00+00:00_2018-01-31-23:52:59+00:00"]

    data_filepath = get_surface_datapath(filename="capegrim-medusa.future.C", source_format="GC")
    precision_filepath = get_surface_datapath(
        filename="capegrim-medusa.future.precisions.C", source_format="GC"
    )

    results = standardise_surface(
        store="user",
        filepath=(data_filepath, precision_filepath),
        source_format="GCWERKS",
        site="CGO",
        network="AGAGE",
    )

    datasource = Datasource(bucket=bucket, uuid=uuid_one)

    assert datasource.data_keys() == [
        "2018-01-01-02:24:00+00:00_2018-01-31-23:52:59+00:00",
        "2023-01-01-02:24:00+00:00_2023-01-31-23:52:59+00:00",
    ]


@pytest.mark.skip(reason="Cranfield data processing will be removed.")
def test_read_cranfield():
    clear_test_stores()

    data_filepath = get_surface_datapath(filename="THB_hourly_means_test.csv", source_format="Cranfield_CRDS")
    results = standardise_surface(
        store="user", filepath=data_filepath, source_format="CRANFIELD", site="TMB", network="CRANFIELD"
    )

    expected_keys = ["ch4", "co", "co2"]

    assert sorted(results["processed"]["THB_hourly_means_test.csv"].keys()) == expected_keys

    uuid = results["processed"]["THB_hourly_means_test.csv"]["ch4"]["uuid"]

    ch4_data = Datasource(bucket=get_bucket(), uuid=uuid, shallow=False).data()
    ch4_data = ch4_data["2018-05-05-00:00:00+00:00_2018-05-13-16:00:00+00:00"]

    assert ch4_data.time[0] == Timestamp("2018-05-05")
    assert ch4_data.time[-1] == Timestamp("2018-05-13T16:00:00")

    assert ch4_data["ch4"][0] == pytest.approx(2585.651)
    assert ch4_data["ch4"][-1] == pytest.approx(1999.018)

    assert ch4_data["ch4 variability"][0] == pytest.approx(75.50218)
    assert ch4_data["ch4 variability"][-1] == pytest.approx(6.48413)


def test_read_openghg_format(bucket):
    """
    Test that files already in OpenGHG format can be read. This file includes:
     - appropriate variable names and types
     - necessary attributes
       - match to site and network supplied
       - additional attributes needed for OpenGHG format
    """
    datafile = get_surface_datapath(filename="tac_co2_openghg.nc", source_format="OPENGHG")

    results = standardise_surface(
        store="user", filepath=datafile, source_format="OPENGHG", site="TAC", network="DECC"
    )

    uuid = results["processed"]["tac_co2_openghg.nc"]["co2"]["uuid"]

    co2_data = Datasource(bucket=bucket, uuid=uuid)

    with co2_data.get_data(version="latest") as co2_data:
        assert co2_data.time[0] == Timestamp("2012-07-30-17:03:08")
        assert co2_data["co2"][0] == 385.25
        assert co2_data["co2_variability"][0] == 0.843


def test_read_noaa_raw(bucket):
    clear_test_stores()

    data_filepath = get_surface_datapath(
        filename="co_pocn25_surface-flask_1_ccgg_event.txt", source_format="NOAA"
    )

    results = standardise_surface(
        store="user",
        filepath=data_filepath,
        source_format="NOAA",
        site="POCN25",
        network="NOAA",
        inlet="flask",
    )

    uuid = results["processed"]["co_pocn25_surface-flask_1_ccgg_event.txt"]["co"]["uuid"]

    co_datasource = Datasource(bucket=bucket, uuid=uuid)

    with co_datasource.get_data(version="latest") as co_data:
        assert co_data["co"][0] == pytest.approx(94.9)
        assert co_data["co_repeatability"][0] == pytest.approx(-999.99)
        assert co_data["co_selection_flag"][0] == 0

        assert co_data["co"][-1] == pytest.approx(73.16)
        assert co_data["co_repeatability"][-1] == pytest.approx(-999.99)
        assert co_data["co_selection_flag"][-1] == pytest.approx(0)


def test_read_noaa_metastorepack(bucket):
    data_filepath = get_surface_datapath(
        filename="ch4_esp_surface-flask_2_representative.nc", source_format="NOAA"
    )

    results = standardise_surface(
        store="user",
        filepath=data_filepath,
        inlet="flask",
        source_format="NOAA",
        site="esp",
        network="NOAA",
        overwrite=True,
    )

    uuid = results["processed"]["ch4_esp_surface-flask_2_representative.nc"]["ch4"]["uuid"]

    ch4_datasource = Datasource(bucket=bucket, uuid=uuid)

    assert ch4_datasource.data_keys() == ["1993-06-17-00:12:30+00:00_2002-01-12-12:00:00+00:00"]

    with ch4_datasource.get_data(version="latest") as ch4_data:
        ch4_data.time[0] == Timestamp("1993-06-17T00:12:30.000000000")
        ch4_data["ch4"][0] == pytest.approx(1.76763e-06)
        ch4_data["ch4_number_of_observations"][0] == 2.0
        ch4_data["ch4_variability"][0] == pytest.approx(1.668772e-09)


@pytest.mark.skip(reason="Thames Barrier data read to be removed.")
def test_read_thames_barrier(bucket):
    clear_test_stores()

    data_filepath = get_surface_datapath(filename="thames_test_20190707.csv", source_format="THAMESBARRIER")

    results = standardise_surface(
        store="user",
        filepath=data_filepath,
        source_format="THAMESBARRIER",
        site="TMB",
        network="LGHG",
        sampling_period="3600s",
    )

    expected_keys = sorted(["ch4", "co2", "co"])

    assert sorted(list(results["processed"]["thames_test_20190707.csv"].keys())) == expected_keys

    uuid = results["processed"]["thames_test_20190707.csv"]["co2"]["uuid"]

    data = Datasource.load(bucket=bucket, uuid=uuid, shallow=False).data()
    data = data["2019-07-01-00:39:55+00:00_2019-08-01-01:10:29+00:00"]

    assert data.time[0] == Timestamp("2019-07-01T00:39:55")
    assert data.time[-1] == Timestamp("2019-08-01T00:10:30")
    assert data["co2"][0] == pytest.approx(417.97344761)
    assert data["co2"][-1] == pytest.approx(417.80000653)
    assert data["co2_variability"][0] == 0
    assert data["co2_variability"][-1] == 0


@pytest.mark.xfail(reason="Deleting datasources will be handled by ObjectStore objects - links to issue #727")
def test_delete_Datasource(bucket):  # TODO: revive/move this test when `ObjectStore` class created
    data_filepath = get_surface_datapath(filename="thames_test_20190707.csv", source_format="THAMESBARRIER")

    standardise_surface(
        store="user",
        filepath=data_filepath,
        source_format="THAMESBARRIER",
        site="tmb",
        network="LGHG",
        sampling_period="1m",
    )

    with open_metastore(data_type="surface", bucket=bucket) as metastore:
        uuid = metastore.select("uuid")[0]
        datasource = Datasource.load(bucket=bucket, uuid=uuid)
        data_keys = datasource.data_keys()
        key = data_keys[0]

        assert exists(bucket=bucket, key=key)

        metastore.delete({"uuid": uuid})

        assert uuid not in metastore.select("uuid")
        assert not exists(bucket=bucket, key=key)


def test_add_new_data_correct_datasource():
    clear_test_stores()

    data_filepath = get_surface_datapath(filename="capegrim-medusa.05.C", source_format="GC")
    precision_filepath = get_surface_datapath(filename="capegrim-medusa.05.precisions.C", source_format="GC")

    results = standardise_surface(
        store="user",
        filepath=(data_filepath, precision_filepath),
        source_format="GCWERKS",
        site="CGO",
        network="AGAGE",
    )

    first_results = results["processed"]["capegrim-medusa.05.C"]

    sorted_keys = sorted(list(results["processed"]["capegrim-medusa.05.C"].keys()))

    assert sorted_keys[:4] == ["c2cl4_10m", "c2cl4_70m", "c2f6_10m", "c2f6_70m"]
    assert sorted_keys[-4:] == ["hfc32_70m", "sf6_70m", "so2f2_10m", "so2f2_70m"]
    assert len(sorted_keys) == 69

    data_filepath = get_surface_datapath(filename="capegrim-medusa.06.C", source_format="GC")
    precision_filepath = get_surface_datapath(filename="capegrim-medusa.06.precisions.C", source_format="GC")

    new_results = standardise_surface(
        store="user",
        filepath=(data_filepath, precision_filepath),
        source_format="GCWERKS",
        site="CGO",
        network="AGAGE",
    )

    second_results = new_results["processed"]["capegrim-medusa.06.C"]

    shared_keys = [key for key in first_results if key in second_results]

    assert len(shared_keys) == 67

    for key in shared_keys:
        assert first_results[key]["uuid"] == second_results[key]["uuid"]
        assert first_results[key]["new"] is True
        assert second_results[key]["new"] is False


@pytest.mark.skip(reason="Ranking being completely reworked")
def test_set_rank():
    o = ObsSurface.load()

    o._rank_data.clear()

    test_uid = "test-uid-123"

    daterange_str = create_daterange_str(start="2001-01-01", end="2005-01-01")
    o.set_rank(uuid=test_uid, rank=1, date_range=daterange_str)

    assert o._rank_data == {"test-uid-123": {"2001-01-01-00:00:00+00:00_2005-01-01-00:00:00+00:00": 1}}

    daterange_str = create_daterange_str(start="2007-01-01", end="2009-01-01")
    o.set_rank(uuid=test_uid, rank=1, date_range=daterange_str)

    assert o._rank_data["test-uid-123"] == {
        "2001-01-01-00:00:00+00:00_2005-01-01-00:00:00+00:00": 1,
        "2007-01-01-00:00:00+00:00_2009-01-01-00:00:00+00:00": 1,
    }

    # Make sure we can't set another rank for the same daterange
    with pytest.raises(ValueError):
        o.set_rank(uuid=test_uid, rank=2, date_range=daterange_str)

    daterange_str = create_daterange_str(start="2008-01-01", end="2009-01-01")

    with pytest.raises(ValueError):
        o.set_rank(uuid=test_uid, rank=3, date_range=daterange_str)

    daterange_str = create_daterange_str(start="2007-01-01", end="2015-01-01")
    o.set_rank(uuid=test_uid, rank=1, date_range=daterange_str)

    assert o._rank_data["test-uid-123"] == {
        "2001-01-01-00:00:00+00:00_2005-01-01-00:00:00+00:00": 1,
        "2007-01-01-00:00:00+00:00_2015-01-01-00:00:00+00:00": 1,
    }


@pytest.mark.skip(reason="Ranking being completely reworked")
def test_set_rank_overwrite():
    o = ObsSurface.load()

    o._rank_data.clear()

    test_uid = "test-uid-123"

    daterange_str = create_daterange_str(start="2007-01-01", end="2015-01-01")
    o.set_rank(uuid=test_uid, rank=1, date_range=daterange_str)
    assert o._rank_data["test-uid-123"] == {"2007-01-01-00:00:00+00:00_2015-01-01-00:00:00+00:00": 1}

    daterange_str = create_daterange_str(start="2008-01-01", end="2009-01-01")
    o.set_rank(uuid=test_uid, rank=2, date_range=daterange_str, overwrite=True)

    expected_ranking = {
        "2007-01-01-00:00:00+00:00_2007-12-31-23:59:59+00:00": 1,
        "2008-01-01-00:00:00+00:00_2008-12-31-23:59:59+00:00": 2,
        "2009-01-01-00:00:01+00:00_2015-01-01-00:00:00+00:00": 1,
    }

    assert o._rank_data["test-uid-123"] == expected_ranking

    daterange_str = create_daterange_str(start="1994-01-01", end="2023-01-01")
    o.set_rank(uuid=test_uid, rank=2, date_range=daterange_str, overwrite=True)

    assert o._rank_data["test-uid-123"] == {"1994-01-01-00:00:00+00:00_2023-01-01-00:00:00+00:00": 2}

    o._rank_data.clear()

    daterange_str = create_daterange_str(start="2001-01-01", end="2021-01-01")
    o.set_rank(uuid=test_uid, rank=1, date_range=daterange_str)

    assert o._rank_data["test-uid-123"] == {"2001-01-01-00:00:00+00:00_2021-01-01-00:00:00+00:00": 1}

    daterange_str = create_daterange_str(start="2007-01-01", end="2009-01-01")
    o.set_rank(uuid=test_uid, rank=2, date_range=daterange_str, overwrite=True)

    daterange_str = create_daterange_str(start="2015-01-01", end="2016-01-01")
    o.set_rank(uuid=test_uid, rank=2, date_range=daterange_str, overwrite=True)

    expected = {
        "2001-01-01-00:00:00+00:00_2006-12-31-23:59:59+00:00": 1,
        "2007-01-01-00:00:00+00:00_2008-12-31-23:59:59+00:00": 2,
        "2009-01-01-00:00:01+00:00_2014-12-31-23:59:59+00:00": 1,
        "2015-01-01-00:00:00+00:00_2015-12-31-23:59:59+00:00": 2,
        "2016-01-01-00:00:01+00:00_2021-01-01-00:00:00+00:00": 1,
    }

    assert o._rank_data["test-uid-123"] == expected


@pytest.mark.skip(reason="Ranking being completely reworked")
def test_rank_overlapping_dateranges():
    dateranges = ["2014-01-01_2099-06-06", "2014-06-07_2015-09-09", "2015-09-10_2019-01-06"]

    o = ObsSurface.load()
    o._rank_data.clear()

    test_uid = "test-uid-123"

    o.set_rank(uuid=test_uid, rank=1, date_range=dateranges)

    with pytest.raises(ValueError):
        o.set_rank(uuid=test_uid, rank=2, date_range=dateranges)


@pytest.mark.skip(reason="Ranking being completely reworked")
def test_rank_same_daterange_doesnt_change():
    o = ObsSurface.load()
    o._rank_data.clear()

    test_uid = "test-uid-123"

    o.set_rank(uuid=test_uid, rank=1, date_range="2012-01-01_2012-06-01")

    assert o._rank_data == {"test-uid-123": {"2012-01-01-00:00:00+00:00_2012-06-01-00:00:00+00:00": 1}}

    o.set_rank(uuid=test_uid, rank=1, date_range="2012-01-01_2012-06-01")

    assert o._rank_data == {"test-uid-123": {"2012-01-01-00:00:00+00:00_2012-06-01-00:00:00+00:00": 1}}


@pytest.mark.skip(reason="Ranking being completely reworked")
def test_rank_daterange_start_overlap_overwrite():
    o = ObsSurface.load()
    o._rank_data.clear()

    test_uid = "test-uid-123"

    o.set_rank(uuid=test_uid, rank=1, date_range="2012-01-01_2013-01-01")

    assert o._rank_data == {"test-uid-123": {"2012-01-01-00:00:00+00:00_2013-01-01-00:00:00+00:00": 1}}

    o.set_rank(uuid=test_uid, rank=2, date_range="2012-01-01_2012-06-01", overwrite=True)

    assert o._rank_data == {
        "test-uid-123": {
            "2012-06-01-00:00:01+00:00_2013-01-01-00:00:00+00:00": 1,
            "2012-01-01-00:00:00+00:00_2012-06-01-00:00:00+00:00": 2,
        }
    }

    o.set_rank(uuid=test_uid, rank=1, date_range="2012-01-01_2013-01-01", overwrite=True)

    expected = {"test-uid-123": {"2012-01-01-00:00:00+00:00_2013-01-01-00:00:00+00:00": 1}}

    assert o._rank_data == expected


@pytest.mark.skip(reason="Function needs refactor or removing.")
def test_read_multiside_aqmesh():
    bucket = get_bucket()
    datafile = get_surface_datapath(filename="co2_data.csv", source_format="AQMESH")
    metafile = get_surface_datapath(filename="co2_metadata.csv", source_format="AQMESH")

    with ObsSurface(bucket=bucket) as metastore:
        result = metastore.read_multisite_aqmesh(
            data_filepath=datafile, metadata_filepath=metafile, overwrite=True
        )

    # This crazy structure will be fixed when add_datsources is updated
    raith_uuid = result["raith"]["raith"]["uuid"]

    d = Datasource.load(bucket=bucket, uuid=raith_uuid, shallow=False)

    data = d.data()["2021-06-18-05:00:00+00:00_2021-06-21-13:00:00+00:00"]

    data.time[0] == Timestamp("2021-06-18T05:00:00")
    data.co2[0] == 442.64
    data.time[-1] == Timestamp("2021-06-21T13:00:00")
    data.co2[-1] == 404.84

    expected_attrs = {
        "site": "raith",
        "pod_id": 39245,
        "start_date": "2021-06-15 01:00:00",
        "end_date": "2021-10-04 00:59:00",
        "relocate_date": "NA",
        "long_name": "Raith",
        "borough": "Glasgow",
        "site_type": "Roadside",
        "in_ulez": "No",
        "latitude": 55.798813,
        "longitude": -4.058363,
        "inlet": "1m",
        "network": "aqmesh_glasgow",
        "sampling_period": "NOT_SET",
        "species": "co2",
        "units": "ppm",
        "data_type": "surface",
        "source_format": "aqmesh",
    }

    assert data.attrs.items() >= expected_attrs.items()


def test_store_icos_carbonportal_data(bucket):
    # First we need to jump through some hoops to get the correct data dict
    # I feel like there must be a simpler way of doing this but xarray.to_json
    # doesn't convert datetimes correctly
    test_data_nc = get_surface_datapath(filename="test_toh_co2_147m.nc", source_format="ICOS")
    ds = xr.open_dataset(test_data_nc)

    metadata_path = get_surface_datapath(filename="toh_metadata.json", source_format="ICOS")

    with open(metadata_path, "r") as f:
        data = json.load(f)

    data["co2"]["data"] = ds

    with ObsSurface(bucket=bucket) as metastore:
        first_result = metastore.store_data(data=data)
        second_result = metastore.store_data(data=data)

    assert first_result["co2"]["new"] is True

    with ObsSurface(bucket=bucket) as obs:
        second_result = obs.store_data(data=data)

    assert second_result is None


@pytest.mark.parametrize(
    "species,obs_variable",
    [
        ("carbon dioxide", "co2"),  # Known species (convert using synonyms)
        ("radon", "rn"),  # Previous issues (added check)
        ("c2f6", "c2f6"),  # Previous issues (added check)
        ("CFC-11", "cfc11"),  # Known CFC (convert using synonyms)
        ("CFC-999", "cfc999"),  # Unknown CFC (remove '-' during cleaning)
        ("SF5CF3", "sf5cf3"),  # Unknown species (convert to lower case)
    ],
)
def test_obs_schema(species, obs_variable):
    """
    Check expected expected data variables (based on species) are being
    included for default ObsSurface schema.

    Conversion to variable name from species is based on
    'acrg_species_info.json' data.

    Note: at the moment this doesn't include any optional variables but
    this may be incorporated in future.
    """
    data_schema = ObsSurface.schema(species=species)

    data_vars = data_schema.data_vars
    assert obs_variable in data_vars

    # TODO: Could also add checks for dims and dtypes?


def test_check_obssurface_same_file_skips():
    filepath = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")

    results = standardise_surface(
        store="user", filepath=filepath, source_format="CRDS", site="bsd", network="DECC"
    )

    assert results

    results = standardise_surface(
        store="user", filepath=filepath, source_format="CRDS", site="bsd", network="DECC"
    )

    assert not results


def test_gcwerks_fp_not_a_tuple_raises():
    filepath = "/tmp/test_filepath.txt"

    with pytest.raises(TypeError):
        standardise_surface(
            store="user", filepath=filepath, source_format="GCWERKS", site="cgo", network="agage"
        )

    with pytest.raises(TypeError):
        standardise_surface(
            store="user", filepath=filepath, source_format="gcwerks", site="cgo", network="agage"
        )


def test_object_loads_if_invalid_objectstore_path_in_json(tmpdir):
    """This was added due to an issue found where in versions of OpenGHG < 0.6.2
    the _bucket variable was written to JSON. If this _bucket variable was updated to
    a path that another user couldn't access (such a symlink in a user's home directory
    the group object store) then subsequent instances of the class would fail due to that bucket
    path being invalid. See https://github.com/openghg/openghg/issues/740
    """
    bucket = get_writable_bucket(name="group")

    filepath = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")

    standardise_surface(store="group", filepath=filepath, source_format="CRDS", site="bsd", network="DECC")

    key = ObsSurface.key()

    no_permissions = Path(tmpdir).joinpath("invalid_path")
    no_permissions.mkdir()
    os.chmod(no_permissions, 0o444)

    # Someone else comes along and changes the value
    stored_obj = get_object_from_json(bucket=bucket, key=key)
    stored_obj.update({"_bucket": str(no_permissions)})
    set_object_from_json(bucket=bucket, key=key, data=stored_obj)

    # Now we search for the object, in versions before 0.6.2 this would cause a PermissionError
    search_surface(site="bsd", species="ch4")
