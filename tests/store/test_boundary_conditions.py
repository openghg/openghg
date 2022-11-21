from helpers import get_bc_datapath
from openghg.retrieve import search
from openghg.store import BoundaryConditions, load_metastore, recombine_datasets
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


# TODO: Add test for co2 data - need to create TEST region to match other data for this
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
