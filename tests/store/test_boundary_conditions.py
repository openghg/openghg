import numpy as np
import pytest
from helpers import get_bc_datapath,clear_test_store
from openghg.retrieve import search
from openghg.standardise import standardise_bc, standardise_from_binary_data
from openghg.store import BoundaryConditions
from openghg.util import hash_bytes
from xarray import open_dataset


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

    proc_results = standardise_from_binary_data(
        data_type="boundary_conditions",
        store="user",
        binary_data=binary_data,
        metadata=metadata,
        file_metadata=file_metadata,
    )

    # assert proc_results == {"ch4_mozart_europe": {"uuid": "test-uuid-1", "new": True}}
    assert proc_results["ch4_mozart_europe"]["new"] is True


def test_read_file_monthly():
    test_datapath = get_bc_datapath("ch4_EUROPE_201208.nc")

    proc_results = standardise_bc(
        store="user",
        filepath=test_datapath,
        species="ch4",
        bc_input="MOZART",
        domain="EUROPE",
        period="monthly",
        force=True,
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

    standardise_bc(
        store="user",
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

    standardise_bc(
        store="user",
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


def test_bc_schema():
    """Check expected data variables are being included for default BoundaryConditions schema"""
    data_schema = BoundaryConditions.schema()

    data_vars = data_schema.data_vars
    assert "vmr_n" in data_vars
    assert "vmr_e" in data_vars
    assert "vmr_s" in data_vars
    assert "vmr_w" in data_vars

    # TODO: Could also add checks for dims and dtypes?


def test_optional_metadata_raise_error():
    """
    Test to verify required keys present in optional metadata supplied as dictionary raise ValueError
    """

    clear_test_store("user")
    test_datapath = get_bc_datapath("co2_EUROPE_201407.nc")

    species = "co2"
    bc_input = "CAMS"
    domain = "EUROPE"

    with pytest.raises(ValueError):
        standardise_bc(
            store="user",
            filepath=test_datapath,
            species=species,
            bc_input=bc_input,
            domain=domain,
            optional_metadata={"purpose":"openghg_tests", "species":"co2"},
        )


def test_optional_metadata():
    """
    Test to verify optional metadata supplied as dictionary gets stored as metadata
    """
    test_datapath = get_bc_datapath("co2_EUROPE_201407.nc")

    species = "co2"
    bc_input = "CAMS"
    domain = "EUROPE"

    standardise_bc(
        store="user",
        filepath=test_datapath,
        species=species,
        bc_input=bc_input,
        domain=domain,
        optional_metadata={"project":"openghg_test", "tag":"tests"}
    )

    search_results = search(
        species=species, bc_input=bc_input, domain=domain, data_type="boundary_conditions"
    )

    bc_obs = search_results.retrieve_all()
    metadata = bc_obs.metadata

    assert "project" in metadata
    assert "tag" in metadata
