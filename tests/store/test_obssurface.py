import json
import os
from pathlib import Path

import numpy as np
import pytest
import xarray as xr
from helpers import (
    attributes_checker_obssurface,
    clear_test_stores,
    get_surface_datapath,
    metadata_checker_obssurface,
    filt,
    select,
)
from openghg.objectstore import (
    exists,
    get_bucket,
    get_object_from_json,
    get_writable_bucket,
    set_object_from_json,
)
from openghg.objectstore.metastore import open_metastore
from openghg.retrieve import get_obs_surface, search_surface
from openghg.standardise import standardise_from_binary_data, standardise_surface
from openghg.store import ObsSurface
from openghg.store.base import Datasource
from openghg.types import MetadataAndData
from openghg.util import create_daterange_str, clean_string
from pandas import Timestamp


@pytest.fixture
def bucket():
    return get_bucket()


@pytest.fixture
def min_uuids_fixture():
    clear_test_stores()

    one_min = get_surface_datapath("tac.picarro.1minute.100m.test.dat", source_format="CRDS")
    one_min_res = standardise_surface(
        store="user", filepath=one_min, site="tac", network="decc", source_format="CRDS"
    )

    min_uuids = filt(one_min_res, file="tac.picarro.1minute.100m.test.dat")

    return min_uuids


@pytest.fixture
def hourly_uuids_fixture():

    one_hour = get_surface_datapath("tac.picarro.hourly.100m.test.dat", source_format="CRDS")
    one_hour_res = standardise_surface(
        store="user", filepath=one_hour, site="tac", network="decc", source_format="CRDS"
    )

    hour_uuids = filt(one_hour_res, file="tac.picarro.hourly.100m.test.dat")

    return hour_uuids


def test_different_sampling_periods_diff_datasources(min_uuids_fixture, hourly_uuids_fixture):

    min_uuids = min_uuids_fixture
    for data in min_uuids:
        assert data["new"] is True

    hour_uuids = hourly_uuids_fixture
    for data in hour_uuids:
        assert data["new"] is True


def test_metadata_tac_crds(min_uuids_fixture, hourly_uuids_fixture, bucket):
    """
    Tests metadata and attributes are as expected, applied after sync_surface_metadata shifted to store level
    """
    bucket = get_writable_bucket(name="user")
    min_uuids = min_uuids_fixture
    for result in min_uuids:
        species = result["species"]
        datasource = Datasource(bucket=bucket, uuid=result["uuid"])
        assert metadata_checker_obssurface(datasource.metadata(), species=species)

        with datasource.get_data(version="latest") as data:
            assert attributes_checker_obssurface(data.attrs, species=species)


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

    proc_data = filt(res, file="tac.picarro.1minute.100m.201208.dat")
    proc_data_2 = filt(res_2, file="tac.picarro.1minute.100m.201407.dat")

    for species in ["ch4", "co2"]:
        uuid1 = filt(proc_data, species=species)[0]["uuid"]
        uuid2 = filt(proc_data_2, species=species)[0]["uuid"]
        assert uuid1 == uuid2


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

    assert result is not None

    result = filt(result, file="bsd.picarro.1minute.248m.min.dat")
    for species in ["ch4", "co2", "co"]:
        res = filt(result, species=species)
        assert res  # some result has species
        assert res[0]["new"] is True

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


def test_read_CRDS(bucket, tmpdir):
    filepath = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")
    results = standardise_surface(
        store="user", filepath=filepath, source_format="CRDS", site="bsd", network="DECC"
    )

    results = filt(results, file="bsd.picarro.1minute.248m.min.dat")

    assert {res["species"] for res in results} == {"ch4", "co", "co2"}

    # Load up the assigned Datasources and check they contain the correct data
    uid = [res["uuid"] for res in results if res["species"] == "ch4"][0]

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

    # Now let's make sure we can write out this retrieved data to NetCDF
    tmppath = Path(tmpdir).joinpath("test.nc")

    with datasource.get_data(version="latest") as ch4_data:
        ch4_data.to_netcdf(tmppath)


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
    expected_results = [
        {"species": "c2cl4", "inlet": "70m"},
        {"species": "c2f6", "inlet": "70m"},
        {"species": "c2h2", "inlet": "70m"},
        {"species": "c2h6", "inlet": "70m"},
        {"species": "c2hcl3", "inlet": "70m"},
        {"species": "c3f8", "inlet": "70m"},
        {"species": "c3h8", "inlet": "70m"},
        {"species": "c4f10", "inlet": "70m"},
        {"species": "c4f8", "inlet": "70m"},
        {"species": "c6f14", "inlet": "70m"},
        {"species": "c6h5ch3", "inlet": "70m"},
        {"species": "c6h6", "inlet": "70m"},
        {"species": "cc3h8", "inlet": "70m"},
        {"species": "ccl4", "inlet": "70m"},
        {"species": "cf4", "inlet": "70m"},
        {"species": "cfc112", "inlet": "70m"},
        {"species": "cfc113", "inlet": "70m"},
        {"species": "cfc114", "inlet": "70m"},
        {"species": "cfc115", "inlet": "70m"},
        {"species": "cfc11", "inlet": "70m"},
        {"species": "cfc12", "inlet": "70m"},
        {"species": "cfc13", "inlet": "70m"},
        {"species": "ch2br2", "inlet": "70m"},
        {"species": "ch2cl2", "inlet": "70m"},
        {"species": "ch3br", "inlet": "70m"},
        {"species": "ch3ccl3", "inlet": "70m"},
        {"species": "ch3cl", "inlet": "70m"},
        {"species": "ch3i", "inlet": "70m"},
        {"species": "chbr3", "inlet": "70m"},
        {"species": "chcl3", "inlet": "70m"},
        {"species": "cos", "inlet": "70m"},
        {"species": "desflurane", "inlet": "70m"},
        {"species": "halon1211", "inlet": "70m"},
        {"species": "halon1301", "inlet": "70m"},
        {"species": "halon2402", "inlet": "70m"},
        {"species": "hcfc124", "inlet": "70m"},
        {"species": "hcfc132b", "inlet": "70m"},
        {"species": "hcfc133a", "inlet": "70m"},
        {"species": "hcfc141b", "inlet": "70m"},
        {"species": "hcfc142b", "inlet": "70m"},
        {"species": "hcfc22", "inlet": "70m"},
        {"species": "hfc125", "inlet": "70m"},
        {"species": "hfc134a", "inlet": "70m"},
        {"species": "hfc143a", "inlet": "70m"},
        {"species": "hfc152a", "inlet": "70m"},
        {"species": "hfc227ea", "inlet": "70m"},
        {"species": "hfc236fa", "inlet": "70m"},
        {"species": "hfc23", "inlet": "70m"},
        {"species": "hfc245fa", "inlet": "70m"},
        {"species": "hfc32", "inlet": "70m"},
        {"species": "hfc365mfc", "inlet": "70m"},
        {"species": "hfc4310mee", "inlet": "70m"},
        {"species": "nf3", "inlet": "70m"},
        {"species": "sf5cf3", "inlet": "70m"},
        {"species": "sf6", "inlet": "70m"},
        {"species": "so2f2", "inlet": "70m"},
    ]

    results = filt(results, file="capegrim-medusa.18.C")

    # select species and inlet, then sort by species
    found_results = sorted(select(results, "species", "inlet"), key=lambda x: x["species"])
    assert sorted(expected_results, key=lambda x: x["species"]) == found_results

    # Load in some data
    uuid = select(filt(results, species="hfc152a", inlet="70m"), "uuid")[0]["uuid"]

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
        store="user", filepath=datafile, source_format="OPENGHG", site="TAC", network="DECC", update_mismatch="metadata",
    )

    uuid = filt(results, file="tac_co2_openghg.nc", species="co2")[0]["uuid"]

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
        measurement_type="flask",
        inlet="flask",
    )

    uuid = filt(results, file="co_pocn25_surface-flask_1_ccgg_event.txt", species="co")[0]["uuid"]

    co_datasource = Datasource(bucket=bucket, uuid=uuid)

    with co_datasource.get_data(version="latest") as co_data:
        assert co_data["co"][0] == pytest.approx(94.9)
        assert co_data["co_repeatability"][0] == pytest.approx(-999.99)
        assert co_data["co_selection_flag"][0] == 0

        assert co_data["co"][-1] == pytest.approx(73.16)
        assert co_data["co_repeatability"][-1] == pytest.approx(-999.99)
        assert co_data["co_selection_flag"][-1] == pytest.approx(0)

        attributes_checker_obssurface(attrs=co_data.attrs, species="co")


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
        measurement_type="flask",
        network="NOAA",
        overwrite=True,
    )

    uuid = filt(results, file="ch4_esp_surface-flask_2_representative.nc", species="ch4")[0]["uuid"]

    ch4_datasource = Datasource(bucket=bucket, uuid=uuid)

    assert ch4_datasource.data_keys() == ["1993-06-17-00:12:30+00:00_2002-01-12-12:00:00+00:00"]

    with ch4_datasource.get_data(version="latest") as ch4_data:
        ch4_data.time[0] == Timestamp("1993-06-17T00:12:30.000000000")
        ch4_data["ch4"][0] == pytest.approx(1.76763e-06)
        ch4_data["ch4_number_of_observations"][0] == 2.0
        ch4_data["ch4_variability"][0] == pytest.approx(1.668772e-09)


@pytest.mark.xfail(reason="Deleting datasources will be handled by ObjectStore objects - links to issue #727")
def test_delete_Datasource(bucket):  # TODO: revive/move this test when `ObjectStore` class created
    data_filepath = get_surface_datapath(
        filename="DECC-picarro_TAC_20130131_co2-185m-20220928.nc", source_format="OPENGHG"
    )

    standardise_surface(
        store="user",
        filepath=data_filepath,
        source_format="OPENGHG",
        site="tac",
        network="DECC",
        instrument="picarro",
        sampling_period="1h",
        update_mismatch="attributes",
        if_exists="new",
        sort_files=True,
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

    first_results = filt(results, file="capegrim-medusa.05.C")

    sorted_pairs = sorted(tuple(res.values()) for res in select(first_results, "species", "inlet"))

    assert sorted_pairs[:4] == [("c2cl4", "10m"), ("c2cl4", "70m"), ("c2f6", "10m"), ("c2f6", "70m")]
    assert sorted_pairs[-4:] == [("hfc32", "70m"), ("sf6", "70m"), ("so2f2", "10m"), ("so2f2", "70m")]

    assert len(sorted_pairs) == 69

    data_filepath = get_surface_datapath(filename="capegrim-medusa.06.C", source_format="GC")
    precision_filepath = get_surface_datapath(filename="capegrim-medusa.06.precisions.C", source_format="GC")

    new_results = standardise_surface(
        store="user",
        filepath=(data_filepath, precision_filepath),
        source_format="GCWERKS",
        site="CGO",
        network="AGAGE",
    )

    second_results = filt(new_results, file="capegrim-medusa.06.C")
    second_pairs = sorted(tuple(res.values()) for res in select(second_results, "species", "inlet"))

    shared_pairs = set(sorted_pairs) & set(second_pairs)

    assert len(shared_pairs) == 67

    for pair in shared_pairs:
        species, inlet = pair
        first_res = filt(first_results, species=species, inlet=inlet)[0]
        second_res = filt(second_results, species=species, inlet=inlet)[0]
        assert first_res["uuid"] == second_res["uuid"]
        assert first_res["new"] is True
        assert second_res["new"] is False


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

    data = [MetadataAndData(metadata=data["co2"]["metadata"], data=ds)]

    with ObsSurface(bucket=bucket) as metastore:
        result = metastore.store_data(data=data)

    assert result is not None
    assert filt(result, species="co2")[0]["new"] is True



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


def test_check_obssurface_multi_file_same_skip():
    """
    BUGFIX: Previously only the last file in the filepath list was saved
    as a hash. This is to check that when multiple files are passed to
    standardise_surface, check that the first file
    """

    clear_test_stores()

    filepaths = [
        get_surface_datapath("DECC-picarro_TAC_20130131_co2-185m-20220929.nc", source_format="openghg"),
        get_surface_datapath("DECC-picarro_TAC_20130131_co2-185m-20220928.nc", source_format="openghg"),
    ]

    results = standardise_surface(
        store="user",
        filepath=filepaths,
        source_format="OPENGHG",
        site="tac",
        network="DECC",
        instrument="picarro",
        sampling_period="1h",
        if_exists="new",
        update_mismatch="metadata",
    )

    assert results

    filepath_repeat = get_surface_datapath(
        "DECC-picarro_TAC_20130131_co2-185m-20220929.nc", source_format="openghg"
    )

    results = standardise_surface(
        store="user",
        filepath=filepath_repeat,
        source_format="OPENGHG",
        site="tac",
        network="DECC",
        instrument="picarro",
        sampling_period="1h",
        if_exists="new",
        update_mismatch="metadata",
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


def test_drop_only_correct_nan():
    """
    Create a test for Issue #826 where all columns were being dropped from CRDS data even if
    only column contains NaN values.

    Example to demonstrate this:
     -      -         -    -       ch4     ch4   ch4       co2     co2   co2
    date   time      type port         C   stdev     N         C   stdev     N
    ...
    140616 033330       air   10       nan     nan   nan       nan     nan   nan
    140616 033430       air   10   1906.27   1.697    17       nan     nan   nan
    140616 033530       air   10   1907.20   0.792    17    405.30   0.382    17
    """

    rgl_filepath = get_surface_datapath(filename="rgl.picarro.1minute.90m.minimum.dat", source_format="CRDS")

    standardise_surface(
        filepath=rgl_filepath, source_format="CRDS", network="DECC", site="RGL", store="group"
    )

    # Compare output to GCWerks - there should be a valid CH4 data point at 2014-06-16 03:34

    rgl_ch4 = get_obs_surface(site="rgl", species="ch4", inlet="90m")
    rgl_ch4_data = rgl_ch4.data

    time_str1 = "2014-06-16T03:34:30"
    time_str2 = "2014-06-16T03:35:30"

    assert len(rgl_ch4_data["time"]) == 2
    assert np.isclose(rgl_ch4_data.sel(time=time_str1)["mf"].values, 1906.27)
    assert np.isclose(rgl_ch4_data.sel(time=time_str2)["mf"].values, 1907.20)

    rgl_co2 = get_obs_surface(site="rgl", species="co2", inlet="90m")
    rgl_co2_data = rgl_co2.data

    assert len(rgl_co2_data["time"]) == 1
    assert np.isclose(rgl_co2_data.sel(time=time_str2)["mf"].values, 405.30)


@pytest.mark.parametrize(
    "data_keyword,data_value_1,data_value_2",
    [
        ("data_level", "1", "2"),
        ("data_sublevel", "1.1", "1.2"),
        ("dataset_source", "InGOS", "European ObsPack"),
    ],
)
def test_obs_data_param_split(data_keyword, data_value_1, data_value_2):
    """
    Test to check keywords can be used to split data into multiple datasources and be retrieved.
    """

    clear_test_stores()
    data_filepath_1 = get_surface_datapath(filename="tac_co2_openghg_dummy-ones.nc", source_format="OPENGHG")
    data_filepath_2 = get_surface_datapath(filename="tac_co2_openghg.nc", source_format="OPENGHG")

    data_labels_1 = {data_keyword: data_value_1}
    data_labels_2 = {data_keyword: data_value_2}

    standardise_surface(
        filepath=data_filepath_1,
        source_format="OPENGHG",
        site="TAC",
        network="DECC",
        store="group",
        update_mismatch="metadata",
        **data_labels_1
    )

    standardise_surface(
        filepath=data_filepath_2,
        source_format="OPENGHG",
        site="TAC",
        network="DECC",
        store="group",
        update_mismatch="metadata",
        **data_labels_2
    )

    tac_1 = get_obs_surface(site="tac", species="co2", **data_labels_1)
    tac_2 = get_obs_surface(site="tac", species="co2", **data_labels_2)

    # assert tac_1.metadata[data_keyword] == data_value_1.lower()
    # assert tac_2.metadata[data_keyword] == data_value_2.lower()
    assert tac_1.metadata[data_keyword] == clean_string(data_value_1)
    assert tac_2.metadata[data_keyword] == clean_string(data_value_2)

    # All values within "tac_co2_openghg_dummy-ones.nc" have been set to a value of 1, so check
    # this data has been retrieved.
    np.testing.assert_equal(tac_1.data["mf"].values, 1)


def test_optional_parameters():
    """Test if ValueError is raised for invalid input value to calibration_scale."""

    clear_test_stores()
    data_filepath = get_surface_datapath(filename="tac_co2_openghg.nc", source_format="OPENGHG")

    with pytest.raises(
        ValueError,
        match="Input for 'calibration_scale': unknown does not match value in file attributes: WMO-X2007",
    ):
        standardise_surface(
            filepath=data_filepath,
            source_format="OPENGHG",
            site="TAC",
            network="DECC",
            calibration_scale="unknown",
            instrument="picarro",
            store="group",
        )


def test_optional_metadata_raise_error():
    """
    Test to verify required keys present in optional metadata supplied as dictionary raise ValueError
    """

    clear_test_stores()
    rgl_filepath = get_surface_datapath(filename="rgl.picarro.1minute.90m.minimum.dat", source_format="CRDS")

    with pytest.raises(ValueError):
        standardise_surface(
            filepath=rgl_filepath,
            source_format="CRDS",
            network="DECC",
            site="RGL",
            store="group",
            optional_metadata={"species": "openghg_tests"},
        )


def test_optional_metadata():
    """
    Test to verify optional metadata supplied as dictionary gets stored as metadata
    """

    rgl_filepath = get_surface_datapath(filename="rgl.picarro.1minute.90m.minimum.dat", source_format="CRDS")

    standardise_surface(
        filepath=rgl_filepath,
        source_format="CRDS",
        network="DECC",
        site="RGL",
        store="group",
        optional_metadata={"project": "openghg_tests"},
    )

    rgl_ch4 = get_obs_surface(site="rgl", species="ch4", inlet="90m")
    rgl_ch4_metadata = rgl_ch4.metadata

    assert "project" in rgl_ch4_metadata


@pytest.mark.parametrize(
    "filepath, site, instrument, sampling_period, network, inlet, measurement_type, source_format, update_mismatch",
    [
        (
            "DECC-picarro_TAC_20130131_co2-185m-20220928.nc",
            "tac",
            "picarro",
            "1h",
            "decc",
            "185m",
            None,
            "openghg",
            "from_definition",
        ),
        ("ch4_bao_tower-insitu_1_ccgg_all.nc", "bao", None, None, "noaa", None, "insitu", "noaa", "from_source"),
        ("ICOS_ATC_L2_L2-2024.1_RGL_90.0_CTS.CH4", "rgl", "g2301", None, "icos", None, None, "icos", "never"),
    ],
)
def test_sync_surface_metadata_store_level(
    filepath, site, instrument, sampling_period, network, inlet, measurement_type, source_format, update_mismatch, caplog
):
    clear_test_stores()
    bucket = get_writable_bucket(name="user")

    filepath = get_surface_datapath(filepath, source_format=source_format)
    standardised_data = standardise_surface(
        filepath=filepath,
        site=site,
        instrument=instrument,
        sampling_period=sampling_period,
        network=network,
        inlet=inlet,
        measurement_type=measurement_type,
        store="user",
        source_format=source_format,
        update_mismatch=update_mismatch,
    )

    standardised_data = filt(standardised_data, file=filepath.name)

    for res in standardised_data:
        datasource = Datasource(bucket=bucket, uuid=res["uuid"])
        assert metadata_checker_obssurface(datasource.metadata(), species=res["species"])

        with datasource.get_data(version="latest") as data:
            assert attributes_checker_obssurface(data.attrs, species=res["species"])


def test_co2_games():

    co2_games_data = get_surface_datapath(
        filename="co2_bsd_tower-insitu_160_allvalid-108magl.nc", source_format="co2_games"
    )

    standardise_surface(
        source_format="co2_games",
        network="paris_simulation",
        site="bsd",
        filepath=co2_games_data,
        store="user",
    )
