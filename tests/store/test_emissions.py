import pytest
from helpers import get_emissions_datapath
from typing import Any, Union
from openghg.retrieve import search, search_flux
from openghg.store import Flux
from openghg.standardise import standardise_flux, standardise_from_binary_data
from openghg.transform import transform_flux_data
from openghg.util import hash_bytes
from xarray import open_dataset
from pandas import Timestamp

from helpers import clear_test_stores


@pytest.fixture
def clear_stores():
    clear_test_stores()


def test_read_binary_data(mocker, clear_stores):
    clear_test_stores()
    fake_uuids = [f"test-uuid-{n}" for n in range(1, 101)]
    mocker.patch("uuid.uuid4", side_effect=fake_uuids)

    test_datapath = get_emissions_datapath("co2-gpp-cardamom_EUROPE_2012.nc")

    binary_data = test_datapath.read_bytes()

    metadata = {
        "species": "co2",
        "source": "gpp-cardamom",
        "domain": "europe",
        "high_time_resolution": False,
    }

    sha1_hash = hash_bytes(data=binary_data)
    filename = test_datapath.name

    file_metadata = {"filename": filename, "sha1_hash": sha1_hash, "compressed": False}

    results = standardise_from_binary_data(
        store="user",
        data_type="flux",
        binary_data=binary_data,
        metadata=metadata,
        file_metadata=file_metadata)

    expected_results = {"co2_gpp-cardamom_europe": {"uuid": "test-uuid-2",
                                                        "new": True}}

    assert results == expected_results


def test_read_file():
    test_datapath = get_emissions_datapath("co2-gpp-cardamom_EUROPE_2012.nc")

    proc_results = standardise_flux(store="user",
                                    filepath=test_datapath,
                                    species="co2",
                                    source="gpp-cardamom",
                                    domain="europe",
                                    high_time_resolution=False,
                                    force=True,  # For ease, make sure we can add the same data.
                                    )

    assert "co2_gpp-cardamom_europe" in proc_results

    search_results = search(
        species="co2",
        source="gpp-cardamom",
        domain="europe",
        data_type="flux",
        start_date="2012",
        end_date="2013",
    )

    emissions_obs = search_results.retrieve_all()
    emissions_data = emissions_obs.data
    metadata = emissions_obs.metadata

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
        "source": "gpp-cardamom",
        "start_date": "2012-01-01 00:00:00+00:00",
        "end_date": "2012-12-31 23:59:59+00:00",
        "max_longitude": 39.38,
        "min_longitude": -97.9,
        "max_latitude": 79.057,
        "min_latitude": 10.729,
        "time_resolution": "standard",
        "data_type": "flux",
        "time_period": "1 year",
    }

    del metadata["processed"]
    del metadata["prior_file_1_version"]

    assert metadata.items() >= expected_metadata.items()
    # TODO: Add test for adding additional years data - 2013 gpp cardomom


@pytest.fixture
def load_edgar():
    def _load_edgar(species: str, version: str, year: Union[str, int], database_version: bool = True):
        """Load globaledgar data for given species, version, and year.

        Currently, there is only data available for version v5.0, v6.0, and years 2014, 2015, 2016,
        all with species CH_4.

        Args:
            species: species of gas
            version: e.g. v5.0, v6.0, v4.3.2
            year: e.g. 2014, 2015, 2016
            database_version: if True, `database_version` argument passed to `read_file`
        """
        file_name = f'ch4-anthro_globaledgar_{version.replace(".", "-")}_{str(year)}.nc'
        datapath = get_emissions_datapath(file_name)
        kwargs: Any = dict(
            filepath=datapath,
            species=species,
            source="anthro",
            domain="globaledgar",
            database="EDGAR",
        )
        if database_version:
            kwargs["database_version"] = version.replace(".", "")
        return standardise_flux(store="user", **kwargs)
    return _load_edgar


def test_read_file_additional_keys(clear_stores, load_edgar):
    """
    Test multiple files can be added using additional keywords
    ('database' and 'database_version' tested below).
    and still be stored and retrieved separately.

    Data used:
     - EDGAR - ch4, anthro, globaledgar domain, 2014, version v5.0 (v50)
     - EDGAR - ch4, anthro, globaledgar domain, 2015, version v6.0 (v60)

    Should produce 2 search results.
    """
    # load 2014, v5
    proc_results1 = load_edgar("ch4", "v5.0", 2014)
    assert "ch4_anthro_globaledgar" in proc_results1

    # load 2015, v6
    proc_results2 = load_edgar("ch4", "v6.0", 2015)
    assert "ch4_anthro_globaledgar" in proc_results2

    search_results_all = search_flux(species="ch4", source="anthro", domain="globaledgar", database="EDGAR")

    # Should still produce 2 search results - one for each added file (database_version)
    assert len(search_results_all) == 2

    # Check these can be distinguished by searching by database_version
    search_results_1 = search_flux(
        species="ch4", source="anthro", domain="globaledgar", database="EDGAR", database_version="v50"
    )
    search_results_2 = search_flux(
        species="ch4", source="anthro", domain="globaledgar", database="EDGAR", database_version="v60"
    )

    assert len(search_results_1) == 1
    assert len(search_results_2) == 1

    assert search_results_1.results.iloc[0]["database_version"] == "v50"
    assert search_results_1.results.iloc[0]["start_date"] == "2014-01-01 00:00:00+00:00"
    assert search_results_2.results.iloc[0]["database_version"] == "v60"
    assert search_results_2.results.iloc[0]["start_date"] == "2015-01-01 00:00:00+00:00"


def test_read_file_align_correct_datasource(clear_stores, load_edgar):
    """
    Test datasources can be correctly aligned for additional keywords.
    ('database' and 'database_version' tested below).

    Data used:
     - EDGAR v5.0 (v50)
       - ch4, anthro, globaledgar domain, 2014
       - ch4, anthro, globaledgar domain, 2015
     - EDGAR v6.0 (v60)
       - ch4, anthro, globaledgar domain, 2015

    Should produce 2 search results.
    """
    load_edgar("ch4", "v5.0", 2014)
    load_edgar("ch4", "v6.0", 2015)
    load_edgar("ch4", "v5.0", 2015)

    search_results_all = search_flux(species="ch4", source="anthro", domain="globaledgar", database="EDGAR")

    # Should still produce 2 search results as 2014, 2015 v5.0 should be associated in a data source.
    assert len(search_results_all) == 2

    # Check these can be distinguished by searching by database_version
    search_results_1 = search_flux(
        species="ch4", source="anthro", domain="globaledgar", database="EDGAR", database_version="v50"
    )
    search_results_2 = search_flux(
        species="ch4", source="anthro", domain="globaledgar", database="EDGAR", database_version="v60"
    )

    assert len(search_results_1) == 1
    assert len(search_results_2) == 1

    # Check both time points are found within the retrieved data for v5.0
    # and date range has been extended.
    edgar_v5_retrieve = search_results_1.retrieve()
    edgar_v5_data = edgar_v5_retrieve.data
    edgar_v5_metadata = edgar_v5_retrieve.metadata

    assert edgar_v5_data.dims["time"] == 2
    assert edgar_v5_data["time"][0] == Timestamp("2014-01-01")
    assert edgar_v5_data["time"][1] == Timestamp("2015-01-01")

    assert edgar_v5_metadata["start_date"] == "2014-01-01 00:00:00+00:00"
    assert edgar_v5_metadata["end_date"] == "2015-12-31 23:59:59+00:00"


def test_read_file_fails_ambiguous(clear_stores, load_edgar):
    """
    Test helpful error message is raised if read_file is unable to disambiguiate
    between multiple datasources based on provided keywords when using
    additional keywords ('database' and 'database_version' tested).

    Data used:
     - same as test_read_file_align_correct_datasource() but doesn't pass
     `database_version` keyword at all for final file.
    """
    load_edgar("ch4", "v5.0", 2014)
    load_edgar("ch4", "v6.0", 2015)

    try:
        load_edgar("ch4", "v5.0", 2015, database_version=False)  # do not pass `database_version="v50"`
    except Exception as e:
        assert "More than once Datasource found for metadata" in e.args[0]
    else:
        raise AssertionError("This test should throw/catch a DatasourceLookupError with a useful message!")


def test_add_edgar_database(clear_stores):
    """Test edgar can be added to object store (default domain)"""
    folder = "v6.0_CH4"
    test_datapath = get_emissions_datapath(f"EDGAR/yearly/{folder}")

    database = "EDGAR"
    date = "2015"

    proc_results = transform_flux_data(store="user", datapath=test_datapath, database=database, date=date)

    default_domain = "globaledgar"

    version = "v6.0"
    species = "ch4"
    default_source = "anthro"

    output_key = f"{species}_{default_source}_{default_domain}_{date}"
    assert output_key in proc_results

    search_results = search_flux(
        species=species,
        date=date,
        database=database,  # would searching for lowercase not work?
        database_version=version,
    )

    assert search_results

    edgar_obs = search_results.retrieve_all()
    metadata = edgar_obs.metadata

    expected_metadata = {
        "species": species,
        "domain": default_domain,
        "source": default_source,
        "database": database.lower(),
        "database_version": version.replace(".", ""),
        "date": "2015",
        "author": "OpenGHG Cloud".lower(),
        "start_date": "2015-01-01 00:00:00+00:00",
        "end_date": "2015-12-31 23:59:59+00:00",
        # "min_longitude": -174.85857,
        # "max_longitude": 180.0,
        "min_longitude": -180.0,
        "max_longitude": 174.85858,
        "min_latitude": -89.95,
        "max_latitude": 89.95,
        "time_resolution": "standard",
        "time_period": "1 year",
    }

    assert metadata.items() >= expected_metadata.items()


def test_transform_and_add_edgar_database(clear_stores):
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

    proc_results = transform_flux_data(store="user", datapath=test_datapath, database=database, date=date, domain=domain)

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
        data_type="flux",
    )

    edgar_data = search_results.retrieve_all()
    metadata = edgar_data.metadata

    # TODO: Add tests for data as well?
    # data_keys = search_results[key]["keys"]

    expected_metadata = {
        "species": species,
        "domain": domain.lower(),
        "source": "anthro",
        "database": "edgar",
        "database_version": version.replace(".", ""),
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


def test_flux_schema():
    """Check expected data variables are being included for default Flux schema"""
    data_schema = Flux.schema()

    data_vars = data_schema.data_vars
    assert "flux" in data_vars

    assert "time" in data_vars["flux"]
    assert "lat" in data_vars["flux"]
    assert "lon" in data_vars["flux"]

    # TODO: Could also add checks for dims and dtypes?
