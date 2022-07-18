import pytest
from openghg.store import Emissions
from openghg.retrieve import search
from openghg.store import recombine_datasets, metastore_manager, datasource_lookup
from xarray import open_dataset
from helpers import get_emissions_datapath
from openghg.util import hash_bytes


def test_read_binary_data(mocker):
    fake_uuids = ["test-uuid-1", "test-uuid-2", "test-uuid-3"]
    mocker.patch("uuid.uuid4", side_effect=fake_uuids)

    test_datapath = get_emissions_datapath("co2-gpp-cardamom_EUROPE_2012.nc")

    binary_data = test_datapath.read_bytes()

    metadata = {
        "species": "co2",
        "source": "gpp-cardamom",
        "date": "2012",
        "domain": "europe",
        "high_time_resolution": False,
    }

    sha1_hash = hash_bytes(data=binary_data)
    filename = test_datapath.name

    file_metadata = {"filename": filename, "sha1_hash": sha1_hash, "compressed": False}

    results = Emissions.read_data(binary_data=binary_data, metadata=metadata, file_metadata=file_metadata)

    assert results == {"co2_gppcardamom_europe_2012": {"uuid": "test-uuid-1", "new": True}}


def test_read_file():
    test_datapath = get_emissions_datapath("co2-gpp-cardamom_EUROPE_2012.nc")

    proc_results = Emissions.read_file(
        filepath=test_datapath,
        species="co2",
        source="gpp-cardamom",
        date="2012",
        domain="europe",
        high_time_resolution=False,
        overwrite=True
    )

    assert "co2_gppcardamom_europe_2012" in proc_results

    search_results = search(
        species="co2", source="gpp-cardamom", date="2012", domain="europe", data_type="emissions"
    )

    key = list(search_results.keys())[0]

    data_keys = search_results[key]["keys"]
    emissions_data = recombine_datasets(keys=data_keys, sort=False)

    metadata = search_results[key]["metadata"]

    orig_data = open_dataset(test_datapath)

    assert orig_data.lat.equals(emissions_data.lat)
    assert orig_data.lon.equals(emissions_data.lon)
    assert orig_data.time.equals(emissions_data.time)
    assert orig_data.flux.equals(emissions_data.flux)

    expected_metadata = {
        "title": "gross primary productivity co2",
        "author": "openghg cloud",
        "date_created": "2018-05-20 19:44:14.968710",
        "number_of_prior_files_used": 1,
        "prior_file_1": "cardamom gpp",
        "prior_file_1_raw_resolution": "25x25km",
        "prior_file_1_reference": "t.l. smallman, jgr biogeosciences, 2017",
        "regridder_used": "acrg_grid.regrid.regrid_3d",
        "comments": "fluxes copied from year 2013. december 2012 values copied from january 2013 values.",
        "species": "co2",
        "domain": "europe",
        "source": "gppcardamom",
        "date": "2012",
        "start_date": "2012-01-01 00:00:00+00:00",
        "end_date": "2012-12-31 23:59:59+00:00",
        "max_longitude": 39.38,
        "min_longitude": -97.9,
        "max_latitude": 79.057,
        "min_latitude": 10.729,
        "time_resolution": "standard",
        "data_type": "emissions",
        "time_period": "1 year",
    }

    del metadata["processed"]
    del metadata["prior_file_1_version"]

    assert metadata == expected_metadata


def test_add_edgar_database():
    """Test edgar can be added to object store (default domain)"""
    folder = "v6.0_CH4"
    test_datapath = get_emissions_datapath(f"EDGAR/yearly/{folder}")

    database = "EDGAR"
    date = "2015"

    proc_results = Emissions.transform_data(
        datapath=test_datapath,
        database=database,
        date=date,
    )

    default_domain = "globaledgar"

    version = "v6.0"
    species = "ch4"
    default_source = "anthro"

    output_key = f"{species}_{default_source}_{default_domain}_{date}"
    assert output_key in proc_results

    search_results = search(
        species=species,
        date=date,
        database=database,  # would searching for lowercase not work?
        database_version=version,
        data_type="emissions",
    )

    key = list(search_results.keys())[0]

    # TODO: Add tests for data as well?
    # data_keys = search_results[key]["keys"]

    metadata = search_results[key]["metadata"]

    expected_metadata = {
        "species": species,
        "domain": default_domain,
        "source": default_source,
        "database": database.lower(),
        "database_version": version.replace('.',''),
        "date": "2015",
        "author": "OpenGHG Cloud".lower(),
        "start_date": "2015-01-01 00:00:00+00:00",
        "end_date": "2015-12-31 23:59:59+00:00",
        "min_longitude": -174.85857,
        "max_longitude": 180.0,
        "min_latitude": -89.95,
        "max_latitude": 89.95,
        "time_resolution": "standard",
        "time_period": "1 year",
    }

    assert metadata.items() >= expected_metadata.items()


def test_transform_and_add_edgar_database():
    """
    Test EDGAR database can be transformed (regridded) and added to the object store.
    """
    # Regridding to a new domain will use the xesmf importer - so skip this test
    # if module is not present.
    xesmf = pytest.importorskip("xesmf")

    folder = "v6.0_CH4"
    test_datapath = get_emissions_datapath(f"EDGAR/yearly/{folder}")

    database = "EDGAR"
    date = "2015"
    domain = "EUROPE"

    proc_results = Emissions.transform_data(
        datapath=test_datapath,
        database=database,
        date=date,
        domain=domain,
    )

    version = "v6.0"
    species = "ch4"
    default_source = "anthro"

    output_key = f"{species}_{default_source}_{domain}_{date}"
    assert output_key in proc_results

    search_results = search(
        species=species,
        date=date,
        domain=domain,
        database=database,  # would searching for lowercase not work?
        database_version=version,
        data_type="emissions",
    )

    key = list(search_results.keys())[0]

    # TODO: Add tests for data as well?
    # data_keys = search_results[key]["keys"]

    metadata = search_results[key]["metadata"]

    expected_metadata = {
        "species": species,
        "domain": domain.lower(),
        "source": "anthro",
        "database": "edgar",
        "database_version": version.replace('.',''),
        "date": "2015",
        "author": "openghg cloud",
        "start_date": "2015-01-01 00:00:00+00:00",
        "end_date": "2015-12-31 23:59:59+00:00",
        "min_longitude": -97.9,
        "max_longitude": 39.38,
        "min_latitude": 10.729,
        "max_latitude": 79.057,
        "time_resolution": "standard",
        "time_period": "1 year",
    }

    assert metadata.items() >= expected_metadata.items()


def test_datasource_add_lookup():
    e = Emissions()

    fake_datasource = {"co2_gppcardamom_europe_2012": {"uuid": "mock-uuid-123456", "new": True}}

    mock_data = {
        "co2_gppcardamom_europe_2012": {
            "metadata": {
                "species": "co2",
                "domain": "europe",
                "source": "gppcardamom",
                "date": "2012",
            }
        }
    }

    with metastore_manager(key="test-key-123") as metastore:
        e.add_datasources(uuids=fake_datasource, data=mock_data, metastore=metastore)

        assert e.datasources() == ["mock-uuid-123456"]

        required = ["species", "domain", "source", "date"]
        lookup = datasource_lookup(metastore=metastore, data=mock_data, required_keys=required)

        assert lookup["co2_gppcardamom_europe_2012"] == fake_datasource["co2_gppcardamom_europe_2012"]["uuid"]


def test_flux_schema():
    """Check expected data variables are being included for default Emissions schema"""
    data_schema = Emissions.schema()

    data_vars = data_schema.data_vars
    assert "flux" in data_vars

    assert "time" in data_vars["flux"]
    assert "lat" in data_vars["flux"]
    assert "lon" in data_vars["flux"]

    # TODO: Could also add checks for dims and dtypes?
