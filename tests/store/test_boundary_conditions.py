import pytest

from openghg.store import BoundaryConditions
from openghg.retrieve import search
from openghg.store import recombine_datasets, metastore_manager
from xarray import open_dataset
from helpers import get_bc_datapath


def test_read_file_monthly():
    test_datapath = get_bc_datapath("ch4_EUROPE_201208.nc")

    proc_results = BoundaryConditions.read_file(
        filepath=test_datapath,
        species="ch4",
        bc_input="MOZART",
        domain="EUROPE",
        period="monthly",
    )

    assert "ch4_mozart_europe_201208" in proc_results

    search_results = search(
        species="ch4", bc_input="MOZART", domain="europe", data_type="boundary_conditions"
    )

    key = list(search_results.keys())[0]

    data_keys = search_results[key]["keys"]
    bc_data = recombine_datasets(keys=data_keys, sort=False)

    metadata = search_results[key]["metadata"]

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

    for key in expected_metadata.keys():
        assert metadata[key] == expected_metadata[key]


def test_read_file_yearly():
    test_datapath = get_bc_datapath("n2o_EUROPE_2012.nc")

    species = "n2o"
    bc_input = "MOZART"
    domain = "EUROPE"

    proc_results = BoundaryConditions.read_file(
        filepath=test_datapath,
        species=species,
        bc_input=bc_input,
        domain=domain,
    )

    assert "n2o_mozart_europe_2012" in proc_results

    search_results = search(
        species=species, bc_input=bc_input, domain=domain, data_type="boundary_conditions"
    )

    key = list(search_results.keys())[0]

    data_keys = search_results[key]["keys"]
    bc_data = recombine_datasets(keys=data_keys, sort=False)

    metadata = search_results[key]["metadata"]

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
        'time period': 'climatology from 200901-201407 mozart output',
        'copied from': '2000',
    }

    for key in expected_metadata.keys():
        assert metadata[key] == expected_metadata[key]

# TODO: Add test for co2 data - need to create TEST region to match other data for this
# TODO: Add test for multiple values within a file - continuous (maybe monthly)
# TODO: Add test around non-continuous data and key word?

def test_datasource_add_lookup():
    bc = BoundaryConditions()

    fake_datasource = {"ch4_mozart_europe_201208": {"uuid": "mock-uuid-123456", "new": True}}

    fake_metadata = {
        "ch4_mozart_europe_201208": {
            "species": "ch4",
            "domain": "europe",
            "bc_input": "mozart",
            "date": "201208",
        }
    }

    with metastore_manager(key="test-key-123") as metastore:
        bc.add_datasources(uuids=fake_datasource, metadata=fake_metadata, metastore=metastore)

        assert bc.datasources() == ["mock-uuid-123456"]

        lookup = bc.datasource_lookup(fake_metadata, metastore=metastore)

        assert lookup["ch4_mozart_europe_201208"] == fake_datasource["ch4_mozart_europe_201208"]["uuid"]
