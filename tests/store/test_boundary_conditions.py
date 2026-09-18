import numpy as np
import pytest
from helpers import get_bc_datapath, clear_test_store
from openghg.dataobjects import data_manager
from openghg.retrieve import search
from openghg.standardise import standardise_bc, standardise_from_binary_data
from openghg.store import BoundaryConditions
from openghg.transform import transform_bc_data
from xarray import concat, open_dataset


@pytest.fixture(autouse=True)
def clear_store():
    """Start each boundary-condition test with an empty writable store."""
    clear_test_store("user")


def test_read_data_monthly(mocker):
    """Monthly boundary-condition binary data is stored with expected metadata."""

    class FakeUUID:
        """A class that mocks `uuid.uuid4`.

        It has a `hex` property, which is used by some of our code.
        The values returned by `hex` are 0, 1, 2, ... (as strings).

        It only returns one uuid, but could be changed to return a different UUID
        each time.
        """

        hex_num = 0
        uuid_num = 0

        def __init__(self) -> None:
            pass

        def __str__(self) -> str:
            return "test-uuid-1"

        @property
        def hex(self) -> str:
            self.hex_num += 1
            return str(self.hex_num)

    fake_uuid = FakeUUID()
    mocker.patch("uuid.uuid4", side_effect=lambda: fake_uuid)
    mocker.patch("openghg.objectstore._objectstore.uuid4", side_effect=lambda: fake_uuid)

    test_datapath = get_bc_datapath("ch4_EUROPE_201208.nc")

    binary_data = test_datapath.read_bytes()
    metadata = {
        "species": "ch4",
        "bc_input": "MOZART",
        "domain": "EUROPE",
        "period": "monthly",
    }

    filename = test_datapath.name

    file_metadata = {"filename": filename, "compressed": False}

    proc_results = standardise_from_binary_data(
        data_type="boundary_conditions",
        store="user",
        binary_data=binary_data,
        metadata=metadata,
        file_metadata=file_metadata,
        source_format="openghg",
    )

    assert proc_results is not None and len(proc_results) == 1

    expected_info = {
        "uuid": "test-uuid-1",
        "new": True,
        "species": "ch4",
        "bc_input": "mozart",
        "domain": "europe",
    }
    assert expected_info.items() <= proc_results[0].items()


def test_read_file_monthly():
    """A monthly boundary-condition file is standardised and retrieved unchanged."""
    test_datapath = get_bc_datapath("ch4_EUROPE_201208.nc")

    proc_results = standardise_bc(
        store="user",
        filepath=test_datapath,
        species="ch4",
        bc_input="MOZART",
        domain="EUROPE",
        period="monthly",
    )

    assert len(proc_results) == 1

    expected_info = {"species": "ch4", "bc_input": "mozart", "domain": "europe"}
    assert expected_info.items() <= proc_results[0].items()

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
        assert bc_data.data[dv].attrs["units"] == "mol/mol"

    assert bc_data.data.lat.attrs["units"] == "degrees_north"
    assert bc_data.data.lon.attrs["units"] == "degrees_east"
    assert bc_data.data.height.attrs["units"] == "m"

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


def test_restandardise_boundary_conditions_after_datasource_deletion():
    """Deleted multi-file boundary-condition data can be recreated in full."""
    clear_test_store("user")
    test_datapaths = [
        get_bc_datapath("ch4_EUROPE_201208.nc"),
        get_bc_datapath("ch4_EUROPE_201209.nc"),
    ]
    standardise_kwargs = {
        "store": "user",
        "filepath": test_datapaths,
        "species": "ch4",
        "bc_input": "MOZART",
        "domain": "EUROPE",
        "period": "monthly",
        "concat_nc_files": False,
    }

    initial_results = standardise_bc(**standardise_kwargs)
    data_manager(
        data_type="boundary_conditions",
        store="user",
        species="ch4",
        bc_input="mozart",
        domain="europe",
    ).delete_datasource(initial_results[0]["uuid"])

    repeated_results = standardise_bc(**standardise_kwargs)

    assert repeated_results and repeated_results[0].get("new") is True

    recreated_data = search(
        species="ch4",
        bc_input="MOZART",
        domain="europe",
        data_type="boundary_conditions",
        store="user",
    ).retrieve_all()
    with open_dataset(test_datapaths[0]) as august_data, open_dataset(test_datapaths[1]) as september_data:
        original_data = concat([august_data, september_data], dim="time").load()

    assert original_data.time.equals(recreated_data.data.time)
    for data_var in ["vmr_n", "vmr_e", "vmr_s", "vmr_w"]:
        assert original_data[data_var].equals(recreated_data.data[data_var])


def test_looped_combine_with_ignored_force_retains_all_boundary_condition_files():
    """Ignored force warns while looped combine retains every source month."""
    test_datapaths = [
        get_bc_datapath("ch4_EUROPE_201208.nc"),
        get_bc_datapath("ch4_EUROPE_201209.nc"),
    ]
    standardise_kwargs = {
        "store": "user",
        "species": "ch4",
        "bc_input": "MOZART",
        "domain": "EUROPE",
        "period": "monthly",
    }

    standardise_bc(filepath=test_datapaths, **standardise_kwargs)
    with pytest.warns(DeprecationWarning, match=r"force.*deprecated.*if_exists") as caught_warnings:
        standardise_bc(
            filepath=test_datapaths,
            concat_nc_files=False,
            force=True,
            if_exists="combine",
            **standardise_kwargs,
        )
    assert sum("force argument is deprecated" in str(warning.message) for warning in caught_warnings) == 1

    retrieved_data = search(
        species="ch4",
        bc_input="MOZART",
        domain="europe",
        data_type="boundary_conditions",
        store="user",
    ).retrieve_all()
    with open_dataset(test_datapaths[0]) as august_data, open_dataset(test_datapaths[1]) as september_data:
        expected_data = concat([august_data, september_data], dim="time").load()

    assert expected_data.time.equals(retrieved_data.data.time)
    for data_var in ["vmr_n", "vmr_e", "vmr_s", "vmr_w"]:
        assert expected_data[data_var].equals(retrieved_data.data[data_var])

    expected_metadata = {
        "species": "ch4",
        "bc_input": "mozart",
        "domain": "europe",
        "data_type": "boundary_conditions",
        "time_period": "1 month",
        "start_date": "2012-08-01 00:00:00+00:00",
        "end_date": "2012-09-30 23:59:59+00:00",
    }
    for key, value in expected_metadata.items():
        assert retrieved_data.metadata[key] == value


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
    assert set(data_schema.units_compatible) == {"vmr_n", "vmr_e", "vmr_s", "vmr_w"}
    assert data_schema.units == {"lat": "degrees_north", "lon": "degrees_east", "height": "m"}

    # TODO: Could also add checks for dims and dtypes?


def test_info_metadata_raise_error():
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
            info_metadata={"purpose": "openghg_tests", "species": "co2"},
        )


def test_info_metadata():
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
        info_metadata={"project": "openghg_test", "tag": "tests"},
    )

    search_results = search(
        species=species, bc_input=bc_input, domain=domain, data_type="boundary_conditions"
    )

    bc_obs = search_results.retrieve_all()
    metadata = bc_obs.metadata

    assert "project" in metadata
    assert "tag" in metadata


def test_transform_cams_n2o_bc():
    "Test CAMS parser for transform_boundary_conditions"
    bc_input = "cams_test"
    cams_version = "v22r1"
    domain = "europe"
    species = "n2o"
    period = "daily"
    filename = "cams73_v22r1_n2o_test_202201.nc"
    data_path = get_bc_datapath(filename=filename)

    results = transform_bc_data(
        datapath=data_path,
        database="CAMS",
        species=species,
        bc_input=bc_input,
        period=period,
        cams_version=cams_version,
        domain=domain,
        source_format="cams",
        store="user",
    )

    expected_metadata = {"species": species, "domain": domain, "bc_input": bc_input}

    for k, v in expected_metadata.items():
        assert results[0][k].lower() == v.lower()
