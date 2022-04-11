import pytest

from openghg.store import Footprints, recombine_datasets, metastore_manager
from openghg.retrieve import search
from openghg.objectstore import get_local_bucket
from helpers import get_footprint_datapath


def test_read_footprint():
    get_local_bucket()

    datapath = get_footprint_datapath("footprint_test.nc")
    # model_params = {"simulation_params": "123"}

    site = "TMB"
    network = "LGHG"
    height = "10m"
    domain = "EUROPE"
    model = "test_model"

    Footprints.read_file(
        filepath=datapath, 
        site=site, 
        model=model, 
        network=network, 
        height=height, 
        domain=domain,
        period="monthly",
    )

    # Get the footprints data
    footprint_results = search(site=site, domain=domain, data_type="footprints")

    fp_site_key = list(footprint_results.keys())[0]

    footprint_keys = footprint_results[fp_site_key]["keys"]
    footprint_data = recombine_datasets(keys=footprint_keys, sort=False)

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
        "height": "10m",
        "model": "test_model",
        "domain": "europe",
        "start_date": "2020-08-01 00:00:00+00:00",
        "end_date": "2020-08-31 23:59:59+00:00",
        "time_period": "1 month",
        "max_longitude": 39.38,
        "min_longitude": -97.9,
        "max_latitude": 79.057,
        "min_latitude": 10.729,
        "time_resolution": "standard_time_resolution",
    }

    assert footprint_data.attrs == expected_attrs

    footprint_data["fp_low"].max().values == pytest.approx(0.43350983)
    footprint_data["fp_high"].max().values == pytest.approx(0.11853027)
    footprint_data["pressure"].max().values == pytest.approx(1011.92)
    footprint_data["fp_low"].min().values == 0.0
    footprint_data["fp_high"].min().values == 0.0
    footprint_data["pressure"].min().values == pytest.approx(1011.92)


# def test_read_footprint_co2():
#     get_local_bucket()

#     datapath = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")

#     site = "TAC"
#     height = "100m"
#     domain = "TEST"
#     model = "NAME"
#     metmodel = "UKV"
#     species = "co2"

#     Footprints.read_file(
#         filepath=datapath, 
#         site=site, 
#         model=model,
#         metmodel=metmodel,
#         height=height,
#         species=species,
#         domain=domain,
#         # high_res=True,
#     )

#     # Get the footprints data
#     footprint_results = search(site=site, domain=domain, species=species, data_type="footprints")

#     fp_site_key = list(footprint_results.keys())[0]

#     footprint_keys = footprint_results[fp_site_key]["keys"]
#     footprint_data = recombine_datasets(keys=footprint_keys, sort=False)

#     footprint_coords = list(footprint_data.coords.keys())

#     # Sorting to allow comparison - coords / dims can be stored in different orders
#     # depending on how the Dataset has been manipulated
#     footprint_coords.sort()
#     assert footprint_coords == ["H_back", "height", "lat", "lev", "lon", "time"]

#     assert "fp" in footprint_data.data_vars
#     assert "fp_HiTRes" in footprint_data.data_vars

#     expected_attrs = {
#         "author": "OpenGHG Cloud",
#         "data_type": "footprints",
#         "site": "tac",
#         "height": "100m",
#         "model": "NAME",
#         "species": "co2",
#         "metmodel": "ukv",
#         "domain": "test",
#         "start_date": "2014-07-01 00:00:00+00:00",
#         "end_date": "2014-07-04 00:59:59+00:00",
#         "time_period": "1 hour",
#         "max_longitude": 3.476,
#         "min_longitude": -0.396,
#         "max_latitude": 53.785,
#         "min_latitude": 51.211,
#         # "time_resolution": "high_time_resolution",
#     }

#     for key in expected_attrs:
#         assert footprint_data.attrs[key] == expected_attrs[key]


def test_datasource_add_lookup():
    f = Footprints()

    fake_datasource = {"tmb_lghg_10m_europe": {"uuid": "mock-uuid-123456", "new": True}}

    fake_metadata = {
        "tmb_lghg_10m_europe": {
            "data_type": "footprints",
            "site": "tmb",
            "height": "10m",
            "domain": "europe",
            "model": "test_model",
            "network": "lghg",
        }
    }

    with metastore_manager(key="test-metastore-123") as metastore:
        f.add_datasources(uuids=fake_datasource, metadata=fake_metadata, metastore=metastore)

        assert f.datasources() == ["mock-uuid-123456"]

        lookup = f.datasource_lookup(metadata=fake_metadata, metastore=metastore)

        assert lookup["tmb_lghg_10m_europe"] == fake_datasource["tmb_lghg_10m_europe"]["uuid"]


