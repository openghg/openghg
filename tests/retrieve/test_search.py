import pytest
from openghg.retrieve import (
    search,
    search_bc,
    search_column,
    search_flux,
    search_eulerian,
    search_footprints,
    search_surface,
)
from pandas import Timestamp
from openghg.store import data_manager


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


def test_search_surface_range():
    res = search_surface(
        site="TAC", species="co2", inlet="185", start_date="2013-02-01", end_date="2013-03-01"
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

    res = search(site="bsd", species="co2", inlet="108m", instrument="picarro", calibration_scale="wmo-x2019")

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

    assert data_types == {"emissions", "surface"}

    obs = res.retrieve_all()

    # Make sure the retrieval works correctly
    data_types = set([ob.metadata["data_type"] for ob in obs])

    assert data_types == {"emissions", "surface"}

    res = search(species="ch4", data_type=["surface"])

    assert len(res.metadata) == 7

    res = search(species="co2", data_type=["surface", "emissions"])

    assert len(res.metadata) == 8


def test_many_term_search():
    res = search(site=["bsd", "tac"], species=["co2", "ch4"], inlet=["42m", "100m"])

    assert len(res.metadata) == 4
    assert res.metadata

    sites = set([x["site"] for x in res.metadata.values()])
    assert sites == {"bsd", "tac"}

    species = set([x["species"] for x in res.metadata.values()])
    assert species == {"co2", "ch4"}

    inlets = set([x["inlet"] for x in res.metadata.values()])
    assert inlets == {"100m", "42m"}


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
            site="TMB", network="LGHG", inlet=inlet_value, domain="EUROPE", model="test_model"
        )
    elif inlet_keyword == "height":
        res = search_footprints(
            site="TMB", network="LGHG", height=inlet_value, domain="EUROPE", model="test_model"
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
    res = search_footprints(site="TAC", network="DECC", height="100m", domain="TEST", model="NAME")

    key = next(iter(res.metadata))
    partial_metadata = {
        "data_type": "footprints",
        "site": "tac",
        "height": "100m",
        "domain": "test",
        "model": "name",
        "metmodel": "ukv",
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
        end_date="2016-08-01",
    )

    # Test retrieved footprint data found from the search contains data
    # spanning the reduced date range
    footprint_data = res.retrieve()
    data = footprint_data.data
    time = data["time"]
    assert time[0] == Timestamp("2016-07-01T00:00:00")
    assert time[-1] == Timestamp("2016-07-01T02:00:00")


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
        "data_type": "emissions",
    }

    assert partial_metadata.items() <= res.metadata[key].items()

    # Test retrieved flux data found from the search contains data spanning
    # the whole range.
    flux_data = res.retrieve()
    data = flux_data.data
    time = data["time"]
    assert time[0] == Timestamp("2012-01-01T00:00:00")
    assert time[-1] == Timestamp("2013-01-01T00:00:00")


def test_search_flux_select():
    """
    Test limited date range can be searched for footprint source.
    (Same data as previous test)
    """
    res = search_flux(
        species="co2", source="gpp-cardamom", domain="europe", start_date="2012-01-01", end_date="2013-01-01"
    )

    key = next(iter(res.metadata))

    partial_metadata = {
        "title": "gross primary productivity co2",
        "author": "openghg cloud",
        "regridder_used": "acrg_grid.regrid.regrid_3d",
        "species": "co2",
        "domain": "europe",
        "source": "gpp-cardamom",
        "data_type": "emissions",
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


def test_search_v5_obj_store():
    """This mocks an old object store by deleting the "object_store" key from the Datasource and the
    metastore and then retrieving the data.
    """
    # We'll delete some of the object store keys for Tacolneston
    tac_100m_dm = data_manager(
        data_type="surface", store="user", site="tac", inlet="100m", species="ch4", instrument="picarro"
    )
    uuid = next(iter(tac_100m_dm.metadata))
    tac_100m_dm.update_metadata(uuid, to_delete="object_store")

    tac_100m_dm.refresh()

    assert "object_store" not in tac_100m_dm.metadata[uuid]

    tac_100m_res = search_surface(species="ch4", inlet="100m", site="tacolneston", instrument="picarro")

    data = tac_100m_res.retrieve_all()

    print(data)
