from helpers import get_bc_datapath
from openghg.retrieve import search
from openghg.store import BoundaryConditions, load_metastore, recombine_datasets
from openghg.util import hash_bytes
from xarray import open_dataset
import numpy as np


def test_read_data_monthly(mocker):
    fake_uuids = ["test-uuid-1", "test-uuid-2", "test-uuid-3"]
    # mocker.patch("uuid.uuid4", side_effect=fake_uuids)

    test_datapath = get_bc_datapath("ch4_EUROPE_201208.nc")

    binary_data = test_datapath.read_bytes()
    sha1_hash = hash_bytes(data=binary_data)

    metadata = {
        "species": "ch4",
        "bc_input": "MOZART",
        "domain": "EUROPE",
        "period": "monthly",
    }

    filename = test_datapath.name

    file_metadata = {"sha1_hash": sha1_hash, "filename": filename, "compressed": False}

    proc_results = BoundaryConditions.read_data(
        binary_data=binary_data, metadata=metadata, file_metadata=file_metadata
    )

    # assert proc_results == {"ch4_mozart_europe": {"uuid": "test-uuid-1", "new": True}}
    assert proc_results["ch4_mozart_europe"]["new"] is True


def test_read_file_monthly():
    test_datapath = get_bc_datapath("ch4_EUROPE_201208.nc")

    proc_results = BoundaryConditions.read_file(
        filepath=test_datapath,
        species="ch4",
        bc_input="MOZART",
        domain="EUROPE",
        period="monthly",
        overwrite=True,
    )

    assert "ch4_mozart_europe" in proc_results

    search_results = search(
        species="ch4", bc_input="MOZART", domain="europe", data_type="boundary_conditions"
    )

    bc_data = search_results.retrieve_all()

    orig_data = open_dataset(test_datapath)

    assert orig_data.lat.equals(bc_data.data.lat)
    assert orig_data.lon.equals(bc_data.data.lon)
    assert orig_data.time.equals(bc_data.data.time)

    data_vars = ["vmr_n", "vmr_e", "vmr_s", "vmr_w"]
    for dv in data_vars:
        assert orig_data[dv].equals(bc_data.data[dv])

    expected_metadata = {
        "title": "mozart volume mixing ratios at domain edges",
        "author": "openghg cloud",
        "date_created": "2018-05-18 15:39:53.392826",
        "species": "ch4",
        "domain": "europe",
        "bc_input": "mozart",
        "start_date": "2012-08-01 00:00:00+00:00",
        "end_date": "2012-08-31 23:59:59+00:00",
        "max_longitude": 39.38,
        "min_longitude": -97.9,
        "max_latitude": 79.057,
        "min_latitude": 10.729,
        "data_type": "boundary_conditions",
        "time_period": "1 month",
    }

    assert expected_metadata.items() <= bc_data.metadata.items()


def test_read_file_yearly():
    test_datapath = get_bc_datapath("n2o_EUROPE_2012.nc")

    species = "n2o"
    bc_input = "MOZART"
    domain = "EUROPE"

    BoundaryConditions.read_file(
        filepath=test_datapath,
        species=species,
        bc_input=bc_input,
        domain=domain,
    )

    search_results = search(
        species=species, bc_input=bc_input, domain=domain, data_type="boundary_conditions"
    )

    bc_obs = search_results.retrieve_all()
    bc_data = bc_obs.data
    metadata = bc_obs.metadata

    orig_data = open_dataset(test_datapath)

    assert orig_data.lat.equals(bc_data.lat)
    assert orig_data.lon.equals(bc_data.lon)
    assert orig_data.time.equals(bc_data.time)

    data_vars = ["vmr_n", "vmr_e", "vmr_s", "vmr_w"]
    for dv in data_vars:
        assert orig_data[dv].equals(bc_data[dv])

    expected_metadata = {
        "title": "mozart volume mixing ratios at domain edges",
        "author": "openghg cloud",
        "date_created": "2018-04-30 09:15:29.472284",
        "species": "n2o",
        "domain": "europe",
        "bc_input": "mozart",
        "start_date": "2012-01-01 00:00:00+00:00",
        "end_date": "2012-12-31 23:59:59+00:00",
        "max_longitude": 39.38,
        "min_longitude": -97.9,
        "max_latitude": 79.057,
        "min_latitude": 10.729,
        "data_type": "boundary_conditions",
        "time_period": "1 year",
        "time period": "climatology from 200901-201407 mozart output",
        "copied from": "2000",
    }

    assert expected_metadata.items() <= metadata.items()


def test_read_file_co2_no_time_dim():
    """
    Test monthly co2 file with with no time dimension can be read and intepreted
    correctly.
     - Input file contains "time" coordinate but this has a dimension of 0.
     - Saved version of this file will update "time" and data variables to include
     this with a 1D dimension.
    """
    test_datapath = get_bc_datapath("co2_EUROPE_201407.nc")

    species = "co2"
    bc_input = "CAMS"
    domain = "EUROPE"

    BoundaryConditions.read_file(
        filepath=test_datapath,
        species=species,
        bc_input=bc_input,
        domain=domain,
    )

    search_results = search(
        species=species, bc_input=bc_input, domain=domain, data_type="boundary_conditions"
    )

    bc_obs = search_results.retrieve_all()
    bc_data = bc_obs.data
    metadata = bc_obs.metadata

    orig_data = open_dataset(test_datapath)

    # Test search results against data extracted from original file
    np.testing.assert_allclose(bc_data.lat, orig_data.lat)
    np.testing.assert_allclose(bc_data.lon, orig_data.lon)

    # For time a new 1D dimension will have been added for this data
    # TODO: Including .astype(int) here as numpy complains about comparing
    # <class 'numpy._FloatAbstractDType'> and <class 'numpy.dtype[datetime64]'>.
    # May want to look into this further or accept this workaround.
    np.testing.assert_allclose(bc_data.time[0].astype(int), orig_data.time.astype(int))

    data_vars = ["vmr_n", "vmr_e", "vmr_s", "vmr_w"]
    for dv in data_vars:
        # Match stored 1D data to original 0D data by selecting on the time axis.
        bc_dv_data = bc_data[dv].isel({"time": 0})
        org_dv_data = orig_data[dv]
        np.testing.assert_allclose(bc_dv_data, org_dv_data)

    expected_metadata = {
        "title": "ecmwf cams co2 volume mixing ratios at domain edges",
        "species": "co2",
        "domain": "europe",
        "bc_input": "cams",
        "start_date": "2014-07-01 00:00:00+00:00",
        "end_date": "2014-07-31 23:59:59+00:00",
        "max_longitude": 39.38,
        "min_longitude": -97.9,
        "max_latitude": 79.057,
        "min_latitude": 10.729,
        "data_type": "boundary_conditions",
        "time_period": "1 month",
    }

    assert expected_metadata.items() <= metadata.items()


# TODO: Add test for multiple values within a file - continuous (maybe monthly)
# TODO: Add test around non-continuous data and key word?


def test_datasource_add_lookup():
    from openghg.store import datasource_lookup

    bc = BoundaryConditions()

    fake_datasource = {"ch4_mozart_europe_201208": {"uuid": "mock-uuid-123456", "new": True}}

    mock_data = {
        "ch4_mozart_europe_201208": {
            "metadata": {
                "species": "ch4",
                "domain": "europe",
                "bc_input": "mozart",
                "date": "201208",
            }
        }
    }

    with load_metastore(key="test-key-123") as metastore:
        bc.add_datasources(uuids=fake_datasource, data=mock_data, metastore=metastore)

        assert bc.datasources() == ["mock-uuid-123456"]

        required = ["species", "domain", "bc_input", "date"]

        lookup = datasource_lookup(metastore=metastore, data=mock_data, required_keys=required)

        assert lookup["ch4_mozart_europe_201208"] == fake_datasource["ch4_mozart_europe_201208"]["uuid"]


def test_bc_schema():
    """Check expected data variables are being included for default BoundaryConditions schema"""
    data_schema = BoundaryConditions.schema()

    data_vars = data_schema.data_vars
    assert "vmr_n" in data_vars
    assert "vmr_e" in data_vars
    assert "vmr_s" in data_vars
    assert "vmr_w" in data_vars

    # TODO: Could also add checks for dims and dtypes?

#test_emmisions

import pytest
from helpers import get_emissions_datapath
from openghg.retrieve import search, search_flux
from openghg.store import Emissions, datasource_lookup, load_metastore
from openghg.util import hash_bytes
from openghg.types import DatasourceLookupError
from xarray import open_dataset
from pandas import Timestamp

from helpers import clear_test_store


def test_read_binary_data(mocker):
    clear_test_store()

    # As well as uuid4() being called within this codebase, this is also called
    # within one of the dependencies (xarray) (e.g. through xr.load_dataset(io.BytesIO(...))).
    # - more fake_uuids may need to be added here to allow this to run successfully.
    fake_uuids = ["test-uuid-1", "test-uuid-2", "test-uuid-3", "test-uuid-4"]
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

    results = Emissions.read_data(binary_data=binary_data, metadata=metadata, file_metadata=file_metadata)

    expected_results = {"co2_gpp-cardamom_europe": {"uuid": "test-uuid-2",
                                                    "new": True}}

    assert results == expected_results


def test_read_file():
    test_datapath = get_emissions_datapath("co2-gpp-cardamom_EUROPE_2012.nc")

    proc_results = Emissions.read_file(
        filepath=test_datapath,
        species="co2",
        source="gpp-cardamom",
        domain="europe",
        high_time_resolution=False,
        overwrite=True,
    )

    assert "co2_gpp-cardamom_europe" in proc_results

    search_results = search(
        species="co2", source="gpp-cardamom", domain="europe", data_type="emissions",
        start_date="2012", end_date="2013",
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
        "data_type": "emissions",
        "time_period": "1 year",
    }

    del metadata["processed"]
    del metadata["prior_file_1_version"]

    assert metadata.items() >= expected_metadata.items()

# TODO: Add test for adding additional years data - 2013 gpp cardomom

def test_read_file_additional_keys():
    """
    Test multiple files can be added using additional keywords
    ('database' and 'database_version' tested below).
    and still be stored and retrieved separately.

    Data used:
     - EDGAR - ch4, anthro, globaledgar domain, 2014, version v5.0 (v50)
     - EDGAR - ch4, anthro, globaledgar domain, 2015, version v6.0 (v60)

    Should produce 2 search results.
    """
    clear_test_store()

    test_datapath1 = get_emissions_datapath("ch4-anthro_globaledgar_v5-0_2014.nc")

    proc_results1 = Emissions.read_file(
        filepath=test_datapath1,
        species="ch4",
        source="anthro",
        domain="globaledgar",
        database="EDGAR",
        database_version="v50",
    )

    assert "ch4_anthro_globaledgar" in proc_results1

    test_datapath2 = get_emissions_datapath("ch4-anthro_globaledgar_v6-0_2015.nc")

    proc_results2 = Emissions.read_file(
        filepath=test_datapath2,
        species="ch4",
        source="anthro",
        domain="globaledgar",
        database="EDGAR",
        database_version="v60",
    )

    assert "ch4_anthro_globaledgar" in proc_results2

    search_results_all = search_flux(species="ch4", source="anthro", domain="globaledgar", database="EDGAR")

    # Should still produce 2 search results - one for each added file (database_version)
    assert len(search_results_all) == 2

    # Check these can be distinguished by searching by database_version
    search_results_1 = search_flux(species="ch4", source="anthro", domain="globaledgar", database="EDGAR", database_version="v50")
    search_results_2 = search_flux(species="ch4", source="anthro", domain="globaledgar", database="EDGAR", database_version="v60")

    assert len(search_results_1) == 1
    assert len(search_results_2) == 1

    assert search_results_1.results.iloc[0]["database_version"] == "v50"
    assert search_results_1.results.iloc[0]["start_date"] == "2014-01-01 00:00:00+00:00"
    assert search_results_2.results.iloc[0]["database_version"] == "v60"
    assert search_results_2.results.iloc[0]["start_date"] == "2015-01-01 00:00:00+00:00"


def test_read_file_align_correct_datasource():
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
    clear_test_store()

    test_datapath1 = get_emissions_datapath("ch4-anthro_globaledgar_v5-0_2014.nc")

    Emissions.read_file(
        filepath=test_datapath1,
        species="ch4",
        source="anthro",
        domain="globaledgar",
        database="EDGAR",
        database_version="v50",
    )

    test_datapath2 = get_emissions_datapath("ch4-anthro_globaledgar_v6-0_2015.nc")

    Emissions.read_file(
        filepath=test_datapath2,
        species="ch4",
        source="anthro",
        domain="globaledgar",
        database="EDGAR",
        database_version="v60",
    )

    test_datapath3 = get_emissions_datapath("ch4-anthro_globaledgar_v5-0_2015.nc")

    Emissions.read_file(
        filepath=test_datapath3,
        species="ch4",
        source="anthro",
        domain="globaledgar",
        database="EDGAR",
        database_version="v50",
    )

    search_results_all = search_flux(species="ch4", source="anthro", domain="globaledgar", database="EDGAR")

    # Should still produce 2 search results as 2014, 2015 v5.0 should be associated in a data source.
    assert len(search_results_all) == 2

    # Check these can be distinguished by searching by database_version
    search_results_1 = search_flux(species="ch4", source="anthro", domain="globaledgar", database="EDGAR", database_version="v50")
    search_results_2 = search_flux(species="ch4", source="anthro", domain="globaledgar", database="EDGAR", database_version="v60")

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


def test_read_file_fails_ambiguous():
    """
    Test helpful error message is raised if read_file is unable to disambiguiate
    between multiple datasources based on provided keywords when using 
    additional keywords ('database' and 'database_version' tested).

    Data used:
     - same as test_read_file_align_correct_datasource() but doesn't pass 
     `database_version` keyword at all for final file.
    """
    clear_test_store()

    test_datapath1 = get_emissions_datapath("ch4-anthro_globaledgar_v5-0_2014.nc")

    Emissions.read_file(
        filepath=test_datapath1,
        species="ch4",
        source="anthro",
        domain="globaledgar",
        database="EDGAR",
        database_version="v50",
    )

    test_datapath2 = get_emissions_datapath("ch4-anthro_globaledgar_v6-0_2015.nc")

    Emissions.read_file(
        filepath=test_datapath2,
        species="ch4",
        source="anthro",
        domain="globaledgar",
        database="EDGAR",
        database_version="v60",
    )

    test_datapath3 = get_emissions_datapath("ch4-anthro_globaledgar_v5-0_2015.nc")

    # Doesn't include a database_version input which would be needed to distinguish
    # between the 2 previous datasources added.
    with pytest.raises(DatasourceLookupError) as exc_info:
        Emissions.read_file(
            filepath=test_datapath3,
            species="ch4",
            source="anthro",
            domain="globaledgar",
            database="EDGAR",
        )
    
    assert "More than once Datasource found for metadata" in exc_info.value.args[0]


def test_add_edgar_database():
    """Test edgar can be added to object store (default domain)"""
    clear_test_store()
    
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

    with load_metastore(key="test-key-123") as metastore:
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


    #test_footprints

    import pytest
from helpers import get_footprint_datapath
from openghg.retrieve import search
from openghg.store import Footprints, datasource_lookup, load_metastore, recombine_datasets
from openghg.util import hash_bytes


@pytest.mark.xfail(reason="Need to add a better way of passing in binary data to the read_file functions.")
def test_read_footprint_co2_from_data(mocker):
    fake_uuids = ["test-uuid-1", "test-uuid-2", "test-uuid-3"]
    mocker.patch("uuid.uuid4", side_effect=fake_uuids)

    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")

    metadata = {
        "site": "TAC",
        "inlet": "100m",
        "inlet": "100m",
        "domain": "TEST",
        "model": "NAME",
        "metmodel": "UKV",
        "species": "co2",
        "high_time_res": True,
    }

    binary_data = datapath.read_bytes()
    sha1_hash = hash_bytes(data=binary_data)
    filename = datapath.name

    file_metadata = {"filename": filename, "sha1_hash": sha1_hash, "compressed": True}

    # Expect co2 data to be high time resolution
    # - could include high_time_res=True but don't need to as this will be set automatically

    result = Footprints.read_data(binary_data=binary_data, metadata=metadata, file_metadata=file_metadata)

    assert result == {"tac_test_NAME_100m": {"uuid": "test-uuid-1", "new": True}}


@pytest.mark.parametrize(
    "keyword,value",
    [
        ("inlet", "100m"),
        ("height", "100m"),
        ("inlet", "100magl"),
        ("height", "100magl"),
        ("inlet", "100"),
    ],
)
def test_read_footprint_standard(keyword, value):
    """
    Test standard footprint which should contain (at least)
     - data variables: "fp"
     - coordinates: "height", "lat", "lev", "lon", "time"
    Check this for different variants of inlet and height inputs.
    """
    datapath = get_footprint_datapath("TAC-100magl_EUROPE_201208.nc")

    site = "TAC"
    domain = "EUROPE"
    model = "NAME"

    if keyword == "inlet":
        Footprints.read_file(
            filepath=datapath,
            site=site,
            model=model,
            inlet=value,
            domain=domain,
        )
    elif keyword == "height":
        Footprints.read_file(
            filepath=datapath,
            site=site,
            model=model,
            height=value,
            domain=domain,
        )

    # Get the footprints data
    footprint_results = search(site=site, domain=domain, data_type="footprints")

    footprint_obs = footprint_results.retrieve_all()
    footprint_data = footprint_obs.data

    footprint_coords = list(footprint_data.coords.keys())

    # Sorting to allow comparison - coords / dims can be stored in different orders
    # depending on how the Dataset has been manipulated
    footprint_coords.sort()
    assert footprint_coords == ["height", "lat", "lev", "lon", "time"]

    assert "fp" in footprint_data.data_vars

    expected_attrs = {
        "author": "OpenGHG Cloud",
        "data_type": "footprints",
        "site": "tac",
        "inlet": "100m",
        "height": "100m",  # Should always be the same as inlet
        "model": "NAME",
        "domain": "europe",
        "start_date": "2012-08-01 00:00:00+00:00",
        "end_date": "2012-08-31 23:59:59+00:00",
        "max_longitude": 39.38,
        "min_longitude": -97.9,
        "max_latitude": 79.057,
        "min_latitude": 10.729,
        "spatial_resolution": "standard_spatial_resolution",
        "time_resolution": "standard_time_resolution",
        "time_period": "2 hours",
    }

    for key in expected_attrs:
        assert footprint_data.attrs[key] == expected_attrs[key]


def test_read_footprint_high_spatial_res():
    """
    Test high spatial resolution footprint
     - expects additional parameters for `fp_low` and `fp_high`
     - expects additional coordinates for `lat_high`, `lon_high`
     - expects keyword attributes to be set
       - "spatial_resolution": "high_spatial_resolution"
    """
    datapath = get_footprint_datapath("footprint_test.nc")
    # model_params = {"simulation_params": "123"}

    site = "TMB"
    network = "LGHG"
    inlet = "10m"
    domain = "EUROPE"
    model = "test_model"

    Footprints.read_file(
        filepath=datapath,
        site=site,
        model=model,
        network=network,
        inlet=inlet,
        domain=domain,
        period="monthly",
        high_spatial_res=True,
    )

    # Get the footprints data
    footprint_results = search(site=site, domain=domain, data_type="footprints")

    footprint_obs = footprint_results.retrieve_all()
    footprint_data = footprint_obs.data

    footprint_coords = list(footprint_data.coords.keys())
    footprint_dims = list(footprint_data.dims)

    # Sorting to allow comparison - coords / dims can be stored in different orders
    # depending on how the Dataset has been manipulated
    footprint_coords.sort()
    footprint_dims.sort()

    assert footprint_coords == ["height", "lat", "lat_high", "lev", "lon", "lon_high", "time"]
    assert footprint_dims == ["height", "index", "lat", "lat_high", "lev", "lon", "lon_high", "time"]

    assert (
        footprint_data.attrs["heights"]
        == [
            500.0,
            1500.0,
            2500.0,
            3500.0,
            4500.0,
            5500.0,
            6500.0,
            7500.0,
            8500.0,
            9500.0,
            10500.0,
            11500.0,
            12500.0,
            13500.0,
            14500.0,
            15500.0,
            16500.0,
            17500.0,
            18500.0,
            19500.0,
        ]
    ).all()

    assert footprint_data.attrs["variables"] == [
        "fp",
        "temperature",
        "pressure",
        "wind_speed",
        "wind_direction",
        "PBLH",
        "release_lon",
        "release_lat",
        "particle_locations_n",
        "particle_locations_e",
        "particle_locations_s",
        "particle_locations_w",
        "mean_age_particles_n",
        "mean_age_particles_e",
        "mean_age_particles_s",
        "mean_age_particles_w",
        "fp_low",
        "fp_high",
        "index_lons",
        "index_lats",
    ]

    del footprint_data.attrs["processed"]
    del footprint_data.attrs["heights"]
    del footprint_data.attrs["variables"]

    expected_attrs = {
        "author": "OpenGHG Cloud",
        "data_type": "footprints",
        "site": "tmb",
        "network": "lghg",
        "inlet": "10m",
        "height": "10m",  # Should always be the same as inlet
        "model": "test_model",
        "domain": "europe",
        "start_date": "2020-08-01 00:00:00+00:00",
        "end_date": "2020-08-31 23:59:59+00:00",
        "time_period": "1 month",
        "max_longitude": 39.38,
        "min_longitude": -97.9,
        "max_latitude": 79.057,
        "min_latitude": 10.729,
        "spatial_resolution": "high_spatial_resolution",
        "max_latitude_high": 52.01937,
        "max_longitude_high": 0.468,
        "min_latitude_high": 50.87064,
        "min_longitude_high": -1.26,
        "time_resolution": "standard_time_resolution",
    }

    assert footprint_data.attrs == expected_attrs

    assert footprint_data["fp_low"].max().values == pytest.approx(0.43350983)
    assert footprint_data["fp_high"].max().values == pytest.approx(0.11853027)
    assert footprint_data["pressure"].max().values == pytest.approx(1011.92)
    assert footprint_data["fp_low"].min().values == 0.0
    assert footprint_data["fp_high"].min().values == 0.0
    assert footprint_data["pressure"].min().values == pytest.approx(1011.92)


@pytest.mark.parametrize(
    "site,inlet,metmodel,start,end,filename",
    [
        (
            "TAC",
            "100m",
            "UKV",
            "2014-07-01 00:00:00+00:00",
            "2014-07-04 00:59:59+00:00",
            "TAC-100magl_UKV_co2_TEST_201407.nc",
        ),
        (
            "RGL",
            "90m",
            "UKV",
            "2014-01-10 00:00:00+00:00",
            "2014-01-12 00:59:59+00:00",
            "RGL-90magl_UKV_co2_TEST_201401.nc",
        ),
    ],
)
def test_read_footprint_co2(site, inlet, metmodel, start, end, filename):
    """
    Test high spatial resolution footprint
     - expects additional parameter for `fp_HiTRes`
     - expects additional coordinate for `H_back`
     - expects keyword attributes to be set
       - "spatial_resolution": "high_time_resolution"

    Two tests included on same domain for CO2:
    - TAC data - includes H_back as an integer (older style footprint)
    - RGL data - includes H_back as a float (newer style footprint)
    """
    datapath = get_footprint_datapath(filename)

    domain = "TEST"
    model = "NAME"
    species = "co2"

    # Expect co2 data to be high time resolution
    # - could include high_time_res=True but don't need to as this will be set automatically

    Footprints.read_file(
        filepath=datapath,
        site=site,
        model=model,
        metmodel=metmodel,
        inlet=inlet,
        species=species,
        domain=domain,
    )

    # Get the footprints data
    footprint_results = search(site=site, domain=domain, species=species, data_type="footprints")

    footprint_obs = footprint_results.retrieve_all()
    footprint_data = footprint_obs.data

    footprint_coords = list(footprint_data.coords.keys())

    # Sorting to allow comparison - coords / dims can be stored in different orders
    # depending on how the Dataset has been manipulated
    footprint_coords.sort()
    assert footprint_coords == ["H_back", "height", "lat", "lev", "lon", "time"]

    assert "fp" in footprint_data.data_vars
    assert "fp_HiTRes" in footprint_data.data_vars

    expected_attrs = {
        "author": "OpenGHG Cloud",
        "data_type": "footprints",
        "site": site.lower(),
        "inlet": inlet,
        "height": inlet,  # Should always be the same as inlet
        "model": "NAME",
        "species": "co2",
        "metmodel": metmodel.lower(),
        "domain": domain.lower(),
        "start_date": start,
        "end_date": end,
        "max_longitude": 3.476,
        "min_longitude": -0.396,
        "max_latitude": 53.785,
        "min_latitude": 51.211,
        "spatial_resolution": "standard_spatial_resolution",
        "time_resolution": "high_time_resolution",
        "time_period": "1 hour",
    }

    for key in expected_attrs:
        assert footprint_data.attrs[key] == expected_attrs[key]


def test_read_footprint_short_lived():
    datapath = get_footprint_datapath("WAO-20magl_UKV_rn_TEST_201801.nc")

    site = "WAO"
    inlet = "20m"
    domain = "TEST"
    model = "NAME"
    metmodel = "UKV"
    species = "Rn"

    # Expect rn data to be short lived
    # - could include short_lifetime=True but shouldn't need to as this will be set automatically

    Footprints.read_file(
        filepath=datapath,
        site=site,
        model=model,
        metmodel=metmodel,
        inlet=inlet,
        species=species,
        domain=domain,
    )

    # Get the footprints data
    footprint_results = search(site=site, domain=domain, species=species, data_type="footprints")

    footprint_obs = footprint_results.retrieve_all()
    footprint_data = footprint_obs.data

    footprint_coords = list(footprint_data.coords.keys())

    # Sorting to allow comparison - coords / dims can be stored in different orders
    # depending on how the Dataset has been manipulated
    footprint_coords.sort()
    assert footprint_coords == ["height", "lat", "lev", "lon", "time"]

    assert "fp" in footprint_data.data_vars
    assert "mean_age_particles_n" in footprint_data.data_vars
    assert "mean_age_particles_e" in footprint_data.data_vars
    assert "mean_age_particles_s" in footprint_data.data_vars
    assert "mean_age_particles_w" in footprint_data.data_vars

    expected_attrs = {
        "author": "OpenGHG Cloud",
        "data_type": "footprints",
        "site": "wao",
        "inlet": inlet,
        "height": inlet,  # Should always be the same value as inlet
        "model": "NAME",
        "species": "rn",  # TODO: May want to see if we can keep this capitalised?
        "metmodel": "ukv",
        "domain": "test",
        "start_date": "2018-01-01 00:00:00+00:00",
        "end_date": "2018-01-02 23:59:59+00:00",
        "max_longitude": 3.476,
        "min_longitude": -0.396,
        "max_latitude": 53.785,
        "min_latitude": 51.211,
        "spatial_resolution": "standard_spatial_resolution",
        "time_resolution": "standard_time_resolution",
        "time_period": "1 hour",
    }

    for key in expected_attrs:
        assert footprint_data.attrs[key] == expected_attrs[key]


def test_datasource_add_lookup():
    f = Footprints()

    fake_datasource = {"tmb_lghg_10m_europe": {"uuid": "mock-uuid-123456", "new": True}}

    mock_data = {
        "tmb_lghg_10m_europe": {
            "metadata": {
                "data_type": "footprints",
                "site": "tmb",
                "inlet": "10m",
                "domain": "europe",
                "model": "test_model",
                "network": "lghg",
            }
        }
    }

    with load_metastore(key="test-metastore-123") as metastore:
        f.add_datasources(uuids=fake_datasource, data=mock_data, metastore=metastore)

        assert f.datasources() == ["mock-uuid-123456"]
        required = ["site", "inlet", "domain", "model"]
        lookup = datasource_lookup(data=mock_data, metastore=metastore, required_keys=required)

        assert lookup["tmb_lghg_10m_europe"] == fake_datasource["tmb_lghg_10m_europe"]["uuid"]


def test_footprint_schema():
    """Check expected data variables are being included for default Footprint schema"""
    data_schema = Footprints.schema()

    data_vars = data_schema.data_vars
    assert "fp" in data_vars
    assert "particle_locations_n" in data_vars
    assert "particle_locations_e" in data_vars
    assert "particle_locations_s" in data_vars
    assert "particle_locations_w" in data_vars

    # TODO: Could also add checks for dims and dtypes?


def test_footprint_schema_spatial():
    """
    Check expected data variables and extra dimensions
    are being included for high_spatial_res Footprint schema
    """

    data_schema = Footprints.schema(high_spatial_res=True)

    data_vars = data_schema.data_vars
    assert "fp" not in data_vars  # "fp" not required (but can be present in file)
    assert "fp_low" in data_vars
    assert "fp_high" in data_vars

    assert "particle_locations_n" in data_vars
    assert "particle_locations_e" in data_vars
    assert "particle_locations_s" in data_vars
    assert "particle_locations_w" in data_vars

    fp_low_dims = data_vars["fp_low"]
    assert "lat" in fp_low_dims
    assert "lon" in fp_low_dims

    fp_high_dims = data_vars["fp_high"]
    assert "lat_high" in fp_high_dims
    assert "lon_high" in fp_high_dims


def test_footprint_schema_temporal():
    """
    Check expected data variables and extra dimensions
    are being included for high_time_res Footprint schema
    """

    data_schema = Footprints.schema(high_time_res=True)

    data_vars = data_schema.data_vars
    assert "fp" not in data_vars  # "fp" not required (but can be present in file)
    assert "fp_HiTRes" in data_vars

    assert "particle_locations_n" in data_vars
    assert "particle_locations_e" in data_vars
    assert "particle_locations_s" in data_vars
    assert "particle_locations_w" in data_vars

    assert "H_back" in data_vars["fp_HiTRes"]


def test_footprint_schema_lifetime():
    """
    Check expected data variables
    are being included for short_lifetime Footprint schema
    """

    data_schema = Footprints.schema(short_lifetime=True)

    data_vars = data_schema.data_vars
    assert "fp" in data_vars

    assert "particle_locations_n" in data_vars
    assert "particle_locations_e" in data_vars
    assert "particle_locations_s" in data_vars
    assert "particle_locations_w" in data_vars

    assert "mean_age_particles_n" in data_vars
    assert "mean_age_particles_e" in data_vars
    assert "mean_age_particles_s" in data_vars
    assert "mean_age_particles_w" in data_vars