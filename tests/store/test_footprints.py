import pytest
from helpers import get_footprint_datapath, clear_test_store
from openghg.retrieve import search
from openghg.objectstore import get_writable_bucket
from openghg.standardise import standardise_footprint, standardise_from_binary_data
from openghg.store import Footprints
from openghg.util import hash_bytes, hash_file
import xarray as xr
from pathlib import Path


@pytest.mark.xfail(reason="Need to add a better way of passing in binary data to the read_file functions.")
def test_read_footprint_co2_from_data(mocker):
    # fake_uuids = ["test-uuid-1", "test-uuid-2", "test-uuid-3"]
    fake_uuids = [f"test-uuid-{n}" for n in range(100, 150)]
    mocker.patch("uuid.uuid4", side_effect=fake_uuids)

    datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")

    metadata = {
        "site": "TAC",
        "inlet": "100m",
        "inlet": "100m",
        "domain": "TEST",
        "model": "NAME",
        "met_model": "UKV",
        "species": "co2",
        "high_time_resolution": "True",
    }

    binary_data = datapath.read_bytes()
    sha1_hash = hash_bytes(data=binary_data)
    filename = datapath.name

    file_metadata = {"filename": filename, "sha1_hash": sha1_hash, "compressed": True}

    # Expect co2 data to be high time resolution
    # - could include high_time_resolution=True but don't need to as this will be set automatically
    result = standardise_from_binary_data(
        store="user",
        data_type="footprints",
        binary_data=binary_data,
        metadata=metadata,
        file_metadata=file_metadata,
    )

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
     - coordinates: "height", "lat", "lon", "time"
    Check this for different variants of inlet and height inputs.
    """
    site = "TAC"
    domain = "EUROPE"
    model = "NAME"
    kwargs = {keyword: value}  # can't pass `keyword=value` as argument to standardise_footprint

    standardise_footprint(
        filepath=get_footprint_datapath("TAC-100magl_EUROPE_201208.nc"),
        site=site,
        domain=domain,
        model=model,
        store="user",
        **kwargs,
    )

    # Get the footprints data
    footprint_results = search(site=site, domain=domain, data_type="footprints")

    footprint_obs = footprint_results.retrieve_all()
    footprint_data = footprint_obs.data

    footprint_coords = list(footprint_data.coords.keys())

    # Sorting to allow comparison - coords / dims can be stored in different orders
    # depending on how the Dataset has been manipulated
    footprint_coords.sort()
    assert footprint_coords == ["height", "lat", "lon", "time"]

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
        "high_spatial_resolution": "False",
        "high_time_resolution": "False",
        "time_period": "2 hours",
    }

    for key in expected_attrs:
        assert footprint_data.attrs[key] == expected_attrs[key]


def test_read_footprint_short_lifetime_no_species_raises():
    with pytest.raises(ValueError):
        standardise_footprint(
            store="user",
            filepath=get_footprint_datapath("footprint_test.nc"),
            site="TAC",
            network="LGHG",
            inlet="10m",
            domain="EUROPE",
            short_lifetime=True,
            model="test_model",
        )

    with pytest.raises(ValueError):
        standardise_footprint(
            species="inert",
            store="user",
            filepath=get_footprint_datapath("footprint_test.nc"),
            site="TAC",
            network="LGHG",
            inlet="10m",
            domain="EUROPE",
            short_lifetime=True,
            model="test_model",
        )


def test_read_footprint_high_spatial_resolution(tmpdir):
    """
    Test high spatial resolution footprint
     - expects additional parameters for `fp_low` and `fp_high`
     - expects additional coordinates for `lat_high`, `lon_high`
     - expects keyword attributes to be set
       - "high_spatial_resolution": "True"
    """
    site = "TMB"
    domain = "EUROPE"
    standardise_footprint(
        store="user",
        filepath=get_footprint_datapath("footprint_test.nc"),
        site=site,
        network="LGHG",
        inlet="10m",
        domain=domain,
        model="test_model",
        period="monthly",
        high_spatial_resolution=True,
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

    assert footprint_coords == ["height", "lat", "lat_high", "lon", "lon_high", "time"]
    assert footprint_dims == ["height", "index", "lat", "lat_high", "lon", "lon_high", "time"]

    assert footprint_data.attrs["heights"] == [
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

    assert footprint_data.attrs["variables"] == [
        "fp",
        "air_temperature",
        "air_pressure",
        "wind_speed",
        "wind_from_direction",
        "atmosphere_boundary_layer_thickness",
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
        "species": "inert",
        "start_date": "2020-08-01 00:00:00+00:00",
        "end_date": "2020-08-31 23:59:59+00:00",
        "time_period": "1 month",
        "max_longitude": 39.38,
        "min_longitude": -97.9,
        "max_latitude": 79.057,
        "min_latitude": 10.729,
        "high_spatial_resolution": "True",
        "max_latitude_high": 52.01937,
        "max_longitude_high": 0.468,
        "min_latitude_high": 50.87064,
        "min_longitude_high": -1.26,
        "high_time_resolution": "False",
        "short_lifetime": "False",
    }

    assert footprint_data.attrs == expected_attrs

    assert footprint_data["fp_low"].max().values == pytest.approx(0.43350983)
    assert footprint_data["fp_high"].max().values == pytest.approx(0.11853027)
    assert footprint_data["air_pressure"].max().values == pytest.approx(1011.92)
    assert footprint_data["fp_low"].min().values == 0.0
    assert footprint_data["fp_high"].min().values == 0.0
    assert footprint_data["air_pressure"].min().values == pytest.approx(1011.92)

    # Make sure we can write out a NetCDF
    tmppath = Path(tmpdir).joinpath("footprint_test.nc")
    footprint_data.to_netcdf(tmppath)


@pytest.mark.parametrize(
    "site,inlet,met_model,start,end,filename",
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
def test_read_footprint_co2(site, inlet, met_model, start, end, filename):
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
    # - could include high_time_resolution=True but don't need to as this will be set automatically
    standardise_footprint(
        store="user",
        filepath=datapath,
        site=site,
        model=model,
        met_model=met_model,
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
    assert footprint_coords == ["H_back", "height", "lat", "lon", "time"]

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
        "met_model": met_model.lower(),
        "domain": domain.lower(),
        "start_date": start,
        "end_date": end,
        "max_longitude": 3.476,
        "min_longitude": -0.396,
        "max_latitude": 53.785,
        "min_latitude": 51.211,
        "high_spatial_resolution": "False",
        "high_time_resolution": "True",
        "short_lifetime": "False",
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
    met_model = "UKV"
    species = "Rn"

    # Expect rn data to be short lived
    # - could include short_lifetime=True but shouldn't need to as this will be set automatically
    standardise_footprint(
        store="user",
        filepath=datapath,
        site=site,
        model=model,
        met_model=met_model,
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
    assert footprint_coords == ["height", "lat", "lon", "time"]

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
        "met_model": "ukv",
        "domain": "test",
        "start_date": "2018-01-01 00:00:00+00:00",
        "end_date": "2018-01-02 23:59:59+00:00",
        "max_longitude": 3.476,
        "min_longitude": -0.396,
        "max_latitude": 53.785,
        "min_latitude": 51.211,
        "high_spatial_resolution": "False",
        "high_time_resolution": "False",
        "short_lifetime": "True",
        "time_period": "1 hour",
    }

    for key in expected_attrs:
        assert footprint_data.attrs[key] == expected_attrs[key]


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
    are being included for high_spatial_resolution Footprint schema
    """

    data_schema = Footprints.schema(high_spatial_resolution=True)

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
    are being included for high_time_resolution Footprint schema
    """

    data_schema = Footprints.schema(high_time_resolution=True)

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


def test_process_footprints():
    file1 = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    file2 = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")

    for fp in (file1, file2):
        standardise_footprint(
            filepath=fp,
            site="TAC",
            inlet="100m",
            domain="TEST_DOMAIN_MULTIFILE",
            model="UKV",
            store="user",
            chunks={"time": 4},
        )

    # Get the footprints data
    fp_res = search(site="TAC", domain="TEST_DOMAIN_MULTIFILE", data_type="footprints")

    fp_obs = fp_res.retrieve_all()

    with xr.open_dataset(file1) as ds, xr.open_dataset(file2) as ds2:
        xr.concat([ds, ds2], dim="time").identical(fp_obs.data)


def test_passing_in_different_chunks_to_same_store_works():
    file1 = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    file2 = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")

    standardise_footprint(
        filepath=file1,
        site="TAC",
        inlet="100m",
        domain="TEST_CHUNK_DOMAIN",
        model="UKV",
        store="user",
        chunks={"time": 4},
        force=True,
    )
    standardise_footprint(
        filepath=file2,
        site="TAC",
        inlet="100m",
        domain="TEST_CHUNK_DOMAIN",
        model="UKV",
        store="user",
        chunks={"time": 2},
        force=True,
    )

    # Get the footprints data
    fp_res = search(site="TAC", domain="TEST_CHUNK_DOMAIN", data_type="footprints")

    fp_obs = fp_res.retrieve_all()

    with xr.open_dataset(file1) as ds, xr.open_dataset(file2) as ds2:
        xr.concat([ds, ds2], dim="time").identical(fp_obs.data)


def test_pass_empty_dict_means_full_dimension_chunks():
    file1 = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    file2 = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")

    bucket = get_writable_bucket(name="user")

    f = Footprints(bucket=bucket)

    # Start with no chunks passed
    checked_chunks = f.check_chunks(
        filepaths=[file1, file2],
        chunks={},
        high_spatial_resolution=False,
        high_time_resolution=False,
        short_lifetime=False,
    )

    assert checked_chunks == {"lat": 12, "lon": 12, "time": 3}


def test_footprints_chunking_schema():
    file1 = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    file2 = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")

    bucket = get_writable_bucket(name="user")

    f = Footprints(bucket=bucket)

    # Start with no chunks passed
    checked_chunks = f.check_chunks(
        filepaths=[file1, file2],
        high_spatial_resolution=False,
        high_time_resolution=False,
        short_lifetime=False,
    )

    assert checked_chunks == {"lat": 12, "lon": 12, "time": 480}

    checked_chunks = f.check_chunks(
        filepaths=[file1, file2],
        chunks={"time": 4},
        high_spatial_resolution=False,
        high_time_resolution=False,
        short_lifetime=False,
    )

    # If we set a chunk size then it should be used and we'll get back the sizes of the other chunks
    assert checked_chunks == {"lat": 12, "lon": 12, "time": 4}

    # Let's set a huge chunk size and make sure we get an error
    with pytest.raises(ValueError):
        f.check_chunks(
            filepaths=[file1, file2],
            chunks={"time": int(1e9)},
            high_spatial_resolution=False,
            high_time_resolution=False,
            short_lifetime=False,
        )


def test_store_and_retrieve_original_files(tmp_path):
    clear_test_store(name="group")

    file1 = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    file2 = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")

    bucket = get_writable_bucket(name="group")

    f = Footprints(bucket=bucket)

    seen, unseen = f.check_hashes(filepaths=[file1, file2], force=False)

    assert not seen

    f.store_original_files(hash_data=unseen)

    output_folder = tmp_path / "original_files"
    output_folder.mkdir()

    filenames_only = {h: f.name for h, f in unseen.items()}

    f.get_original_files(hash_data=filenames_only, output_folder=output_folder)

    original_files = list(output_folder.iterdir())
    assert len(original_files) == 2

    # Let's make sure they're exactly the same files
    for filepath in original_files:
        assert hash_file(filepath) in unseen
