import pytest
from openghg.retrieve import (
    search,
    search_bc,
    search_column,
    search_eulerian,
    search_flux,
    search_footprints,
    search_surface,
)
from openghg.dataobjects import data_manager
from pandas import Timestamp


@pytest.mark.parametrize(
    "inlet_keyword,inlet_value",
    [
        ("inlet", "50m"),
        ("height", "50m"),
        ("inlet", "50magl"),
        ("inlet", "50"),
        ("inlet", 50),  # May remove this later as really we expect a string here
    ],
)
def test_search_surface(inlet_keyword, inlet_value):
    res = search_surface(site="hfd")

    assert len(res.metadata) == 6

    if inlet_keyword == "inlet":
        res = search_surface(site="hfd", inlet=inlet_value, species="co2")
    elif inlet_keyword == "height":
        res = search_surface(site="hfd", height=inlet_value, species="co2")

    key = next(iter(res.metadata))

    partial_metdata = {
        "site": "hfd",
        "instrument": "picarro",
        "sampling_period": "60.0",
        "inlet": "50m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "calibration_scale": "wmo-x2019",
        "long_name": "heathfield",
        "data_type": "surface",
        "inlet_height_magl": "50",
    }

    assert partial_metdata.items() <= res.metadata[key].items()

    assert not search_surface(site="hfd", species="co2", inlet="888m")


def test_search_surface_selects_dates():
    res = search_surface(site="hfd", species="co2", inlet="50m")

    data = res.retrieve_all().data

    assert data.time[0] == Timestamp("2013-11-23T12:28:30")
    assert data.time[-1] == Timestamp("2020-06-24T09:41:30")

    res = search_surface(
        site="hfd", species="co2", inlet="50m", start_date="2014-01-01", end_date="2014-12-31"
    )

    data_sliced = res.retrieve_all().data

    assert data_sliced.time[0] == Timestamp("2014-01-01T22:36:30")
    assert data_sliced.time[-1] == Timestamp("2014-01-07T09:17:30")


def test_search_surface_range():
    res = search_surface(
        site="TAC",
        species="co2",
        inlet="185",
        start_date="2013-02-01",
        end_date="2013-03-01",
    )

    assert res is not None

    key = next(iter(res.metadata))

    partial_metdata = {
        "site": "tac",
        "instrument": "picarro",
        "sampling_period": "3600.0",
        "inlet": "185m",
        "network": "decc",
        "species": "co2",
        "data_type": "surface",
        "inlet_height_magl": "185",
    }

    assert res.metadata[key].items() >= partial_metdata.items()


def test_search_site():
    res = search(site="bsd", species="co2", inlet="42m")

    expected = {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "60.0",
        "inlet": "42m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "calibration_scale": "wmo-x2019",
        "long_name": "bilsdale",
        "inlet_height_magl": "42",
        "data_owner": "simon o'doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "station_longitude": -1.15033,
        "station_latitude": 54.35858,
        "station_long_name": "bilsdale, uk",
        "station_height_masl": 380.0,
    }

    key = next(iter(res.metadata))
    metadata = res.metadata[key]

    assert expected.items() <= metadata.items()

    res = search(
        site="bsd",
        species="co2",
        inlet="108m",
        instrument="picarro",
        calibration_scale="wmo-x2019",
    )

    expected = {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "60.0",
        "inlet": "108m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "calibration_scale": "wmo-x2019",
        "long_name": "bilsdale",
        "inlet_height_magl": "108",
        "data_owner": "simon o'doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "station_longitude": -1.15033,
        "station_latitude": 54.35858,
        "station_long_name": "bilsdale, uk",
        "station_height_masl": 380.0,
    }

    key = next(iter(res.metadata))
    metadata = res.metadata[key]

    assert expected.items() <= metadata.items()

    res = search(site="atlantis")

    assert not res


def test_search_site_data_version():
    """Test that latest version value is added to metadata"""

    res = search(site="bsd", species="co2", inlet="42m")
    key = next(iter(res.metadata))
    metadata = res.metadata[key]

    assert "latest_version" in metadata
    assert metadata["latest_version"] == "v1"


def test_multi_type_search():
    res = search(species="ch4")

    data_types = set([m["data_type"] for m in res.metadata.values()])

    assert data_types == {"surface", "eulerian_model", "column"}

    res = search(species="co2")
    data_types = set([m["data_type"] for m in res.metadata.values()])

    assert data_types == {"flux", "surface"}

    obs = res.retrieve_all()

    # Make sure the retrieval works correctly
    data_types = set([ob.metadata["data_type"] for ob in obs])

    assert data_types == {"flux", "surface"}

    res = search(species="ch4", data_type=["surface"])

    assert len(res.metadata) == 7

    res = search(species="co2", data_type=["surface", "flux"])

    assert len(res.metadata) == 8


def test_many_term_search():
    """Test search using list inputs. This should create an OR search between the terms in the arguments with lists."""
    res = search(site=["bsd", "tac"], species=["co2", "ch4"], inlet=["42m", "100m"])

    assert len(res.metadata) == 4
    assert res.metadata

    sites = set([x["site"] for x in res.metadata.values()])
    assert sites == {"bsd", "tac"}

    species = set([x["species"] for x in res.metadata.values()])
    assert species == {"co2", "ch4"}

    inlets = set([x["inlet"] for x in res.metadata.values()])
    assert inlets == {"100m", "42m"}


def test_optional_term_search():
    """Test search using dict inputs. This should create an OR search between the key, value pairs in the dictionaries"""
    # Note: had to be careful to not create duplicates as this currently raises an error.
    res = search(
        site="bsd",
        inlet_option={"inlet": "42m", "height": "42m"},
        name_option={"station_long_name": "bilsdale, uk", "long_name": "bilsdale"},
    )

    assert len(res.metadata) == 3
    assert res.metadata

    inlets = set([x["inlet"] for x in res.metadata.values()])
    assert inlets == {"42m"}


def test_nonsense_terms():
    res = search(site="london", species="ch4")

    assert not res

    res = search(site="bsd", species="sparrow")

    assert not res


@pytest.mark.parametrize(
    "inlet_keyword,inlet_value",
    [
        ("inlet", "10m"),
        ("height", "10m"),
        ("inlet", "10magl"),
        ("inlet", "10"),
    ],
)
def test_search_footprints(inlet_keyword, inlet_value):
    """
    Test search for footprint data which has been added to the object store.
    This has been stored using one footprint file which represents a year of data.
    """

    if inlet_keyword == "inlet":
        res = search_footprints(
            site="TMB",
            network="LGHG",
            inlet=inlet_value,
            domain="EUROPE",
            model="test_model",
        )
    elif inlet_keyword == "height":
        res = search_footprints(
            site="TMB",
            network="LGHG",
            height=inlet_value,
            domain="EUROPE",
            model="test_model",
        )

    key = next(iter(res.metadata))
    partial_metadata = {
        "data_type": "footprints",
        "site": "tmb",
        "inlet": "10m",
        "domain": "europe",
        "model": "test_model",
        "network": "lghg",
        "start_date": "2020-08-01 00:00:00+00:00",
        "end_date": "2021-07-31 23:59:59+00:00",
        "time_period": "1 year",
    }

    assert partial_metadata.items() <= res.metadata[key].items()


def test_search_footprints_multiple():
    """
    Test search for footprint source which is comprised of multiple uploaded files.
    Each file contains cutdown hourly data and covers 1 month:
        - 2016-07-01 (3 time points)
        - 2016-08-01 (3 time points)
    """
    res = search_footprints(
        site="TAC", network="DECC", height="100m", domain="TEST", model="NAME", time_resolved=False
    )

    key = next(iter(res.metadata))
    partial_metadata = {
        "data_type": "footprints",
        "site": "tac",
        "height": "100m",
        "domain": "test",
        "model": "name",
        "met_model": "ukv",
        "network": "decc",
        "time_period": "1 hour",
    }

    assert partial_metadata.items() <= res.metadata[key].items()

    # Test retrieved footprint data found from the search contains data spanning
    # the whole range.
    footprint_data = res.retrieve()

    data = footprint_data.data
    time = data["time"]
    assert time[0] == Timestamp("2016-07-01T00:00:00")
    assert time[-1] == Timestamp("2016-08-01T02:00:00")


def test_search_footprints_select():
    """
    Test limited date range can be searched for footprint source.
    (Same data as previous test)
    """
    res = search_footprints(
        site="TAC",
        network="DECC",
        height="100m",
        domain="TEST",
        model="NAME",
        start_date="2016-01-01",
        end_date="2016-07-31",
    )

    # Test retrieved footprint data found from the search contains data
    # spanning the reduced date range
    footprint_data = res.retrieve()
    data = footprint_data.data
    time = data["time"]

    assert time[0] == Timestamp("2016-07-01T00:00:00")
    assert time[-1] == Timestamp("2016-07-01T02:00:00")


@pytest.mark.parametrize(
    "time_resolved_keyword,value",
    [
        ("time_resolved", True),
        ("high_time_resolution", True),
    ],
)
def test_search_footprints_time_resolved(time_resolved_keyword, value):
    """Test search for time resolved footprints

    Expected behaviour: searching for footprints with
    keyword argument `time_resolved = True` should only
    return results for time resolved footprints.
    """

    # Check search for footprints returns multiple entries
    res_all = search_footprints(
        site="TAC",
    )

    # Based on loaded data in conftest.py,
    # more than one footprint for TAC should be found (standard, time_resolved)
    assert res_all.results.shape[0] > 1

    # Check searching using the time_resolved keyword, finds only the time resolved footprint.
    res = search_footprints(site="TAC", **{time_resolved_keyword: value})

    # results dataframes should have exactly one row (only time resolved footprint)
    assert res.results.shape[0] == 1

    # check attributes include time_resolved
    metadata = res.retrieve().metadata
    assert metadata["time_resolved"] == "true"


@pytest.fixture()
def previous_htr_footprint_setup():
    """
    Mimic the previous setup when adding "high_time_resolution" (now
    termed "time_resolved") footprints so this has the previous
    high_time_resolution="true" key rather than the new time_resolved="true".
    """
    from helpers import get_footprint_datapath
    from openghg.standardise import standardise_footprint

    # Add high time resolution footprint
    hitres_fp_datapath = get_footprint_datapath("TAC-185magl_UKV_co2_EUROPE_TEST_201405.nc")
    standardise_footprint(
        store="user",
        filepath=hitres_fp_datapath,
        site="TAC",
        model="NAME",
        network="DECC",
        height="185m",
        domain="TEST",
        met_model="UKV",
        time_resolved=True,
    )

    # Find this footprint and update the metadata
    dm = data_manager(data_type="footprints", site="TAC", inlet="185m", time_resolved=True, store="user")
    uuid = next(iter(dm.metadata))

    # Removed time_resolved key
    to_delete = "time_resolved"
    value = dm.metadata[uuid][to_delete]
    dm.update_metadata(uuid=uuid, to_delete=to_delete)

    # Add high_time_resolution key
    to_add = {"high_time_resolution": value}
    dm.update_metadata(uuid=uuid, to_update=to_add)

    yield

    # Remove temporary datasource from the object store
    dm = data_manager(
        data_type="footprints", site="TAC", inlet="185m", high_time_resolution=True, store="user"
    )
    uuid = next(iter(dm.metadata))
    dm.delete_datasource(uuid=uuid)


def test_search_high_time_resolution(previous_htr_footprint_setup):
    """
    Check backwards comptability for footprints added to an object store
    prior to the use of time_resolved keyword in preference to
    high_time_resolution.
    """

    # Check search for footprints returns expected footprint using high_time_resolution
    res1 = search_footprints(
        site="TAC",
        inlet="185m",
        high_time_resolution=True,
    )

    # Check results to ensure footprint labeled as high_time_resolution is found
    assert res1.results.shape[0] == 1

    # Check attributes
    metadata1 = res1.retrieve().metadata
    assert metadata1["high_time_resolution"] == "true"

    # Check search for footprints returns expected footprint using time_resolved
    res2 = search_footprints(
        site="TAC",
        inlet="185m",
        time_resolved=True,
    )

    # Check results to ensure footprint labeled as high_time_resolution is found
    assert res2.results.shape[0] == 1

    # check attributes
    metadata2 = res2.retrieve().metadata
    assert metadata2["high_time_resolution"] == "true"


def test_search_flux():
    """
    Test search for flux which is comprised of multiple uploaded files.
    Each file contains "yearly" data:
        - 2012-01-01 - 2012-12-31
        - 2013-01-01 - 2013-12-31
    """
    res = search_flux(
        species="co2",
        source="gpp-cardamom",
        domain="europe",
    )

    key = next(iter(res.metadata))

    partial_metadata = {
        "title": "gross primary productivity co2",
        "author": "openghg cloud",
        "regridder_used": "acrg_grid.regrid.regrid_3d",
        "species": "co2",
        "domain": "europe",
        "source": "gpp-cardamom",
        "data_type": "flux",
    }

    assert partial_metadata.items() <= res.metadata[key].items()

    # Test retrieved flux data found from the search contains data spanning
    # the whole range.
    flux_data = res.retrieve()
    data = flux_data.data
    time = data["time"]
    assert time[0] == Timestamp("2012-01-01T00:00:00")
    assert time[-1] == Timestamp("2013-01-01T00:00:00")


def test_search_flux_retrieve_original_files():
    """Ensure we can retrieve the original files from the data
    returned by the search result.
    """
    res = search_flux(
        species="co2",
        source="gpp-cardamom",
        domain="europe",
    )

    uid = next(iter(res.metadata))

    hashes = res.metadata[uid]["original_file_hashes"]["v1"]
    assert "9ff6de082836e1735d2b2dea2dbbc69b2dc89229" in hashes
    assert "9554a94b439317770b99c3877a1b17941bb19255" in hashes


def test_search_flux_select():
    """
    Test limited date range can be searched for footprint source.
    (Same data as previous test)
    """
    res = search_flux(
        species="co2",
        source="gpp-cardamom",
        domain="europe",
        start_date="2012-01-01",
        end_date="2013-01-01",
    )

    key = next(iter(res.metadata))

    partial_metadata = {
        "title": "gross primary productivity co2",
        "author": "openghg cloud",
        "regridder_used": "acrg_grid.regrid.regrid_3d",
        "species": "co2",
        "domain": "europe",
        "source": "gpp-cardamom",
        "data_type": "flux",
    }

    assert partial_metadata.items() <= res.metadata[key].items()

    # Test retrieved flux data found from the search contains data
    # spanning the reduced date range
    flux_data = res.retrieve()
    data = flux_data.data
    time = data["time"]
    assert len(time) == 1
    assert time[0] == Timestamp("2012-01-01T00:00:00")


def test_search_column():
    res = search_column(
        satellite="GOSAT",
        domain="BRAZIL",
        species="methane",
    )

    key = next(iter(res.metadata))

    partial_metadata = {
        "satellite": "gosat",
        "instrument": "tanso-fts",
        "species": "ch4",
        "domain": "brazil",
        "network": "gosat",
        "platform": "satellite",
        "selection": "brazil",
        "data_type": "column",
    }

    assert partial_metadata.items() <= res.metadata[key].items()


def test_search_bc():
    res = search_bc(species="n2o", bc_input="MOZART", domain="EUROPE")

    key = next(iter(res.metadata))

    partial_metadata = {
        "date_created": "2018-04-30 09:15:29.472284",
        "author": "openghg cloud",
        "run name": "newedgar",
        "species": "n2o",
        "title": "mozart volume mixing ratios at domain edges",
        "time period": "climatology from 200901-201407 mozart output",
        "copied from": "2000",
        "domain": "europe",
        "bc_input": "mozart",
        "start_date": "2012-01-01 00:00:00+00:00",
        "end_date": "2012-12-31 23:59:59+00:00",
    }

    assert partial_metadata.items() <= res.metadata[key].items()


def test_search_eulerian_model():
    res = search_eulerian(model="GEOSChem", species="ch4")

    key = next(iter(res.metadata))

    partial_metadata = {
        "title": "geos-chem diagnostic collection: speciesconc",
        "format": "cfio",
        "conventions": "coards",
        "simulation_start_date_and_time": "2015-01-01 00:00:00z",
        "simulation_end_date_and_time": "2016-01-01 00:00:00z",
        "model": "geoschem",
        "species": "ch4",
    }

    assert partial_metadata.items() <= res.metadata[key].items()
