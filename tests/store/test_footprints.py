import pytest
from helpers import get_footprint_datapath
from openghg.retrieve import search
from openghg.objectstore import get_bucket
from openghg.store import Footprints, datasource_lookup, load_metastore
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
    bucket = get_bucket()
    with Footprints(bucket=bucket) as fps:
        result = fps.read_data(binary_data=binary_data, metadata=metadata, file_metadata=file_metadata)

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

    bucket = get_bucket()
    if keyword == "inlet":
        with Footprints(bucket=bucket) as fps:
            fps.read_file(
                filepath=datapath,
                site=site,
                model=model,
                inlet=value,
                domain=domain,
            )
    elif keyword == "height":
        with Footprints(bucket=bucket) as fps:
            fps.read_file(
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

    bucket = get_bucket()
    with Footprints(bucket=bucket) as fps:
        fps.read_file(
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

    bucket = get_bucket()
    with Footprints(bucket=bucket) as fps:
        fps.read_file(
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

    bucket = get_bucket()
    with Footprints(bucket=bucket) as fps:
        fps.read_file(
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
    bucket = get_bucket()
    f = Footprints(bucket=bucket)

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
