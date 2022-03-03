import pytest

from openghg.store import Footprints, recombine_datasets
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
        filepath=datapath, site=site, model=model, network=network, height=height, domain=domain
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
        "end_date": "2020-08-01 00:00:00+00:00",
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


def test_set_lookup_uuids():
    f = Footprints()

    fake_uuid = "123456789"

    site = "test_site"
    domain = "test_domain"
    model = "test_model"
    height = "test_height"

    f.set_uuid(site=site, domain=domain, model=model, height=height, uuid=fake_uuid)

    found_uid = f.lookup_uuid(site=site, domain=domain, model=model, height=height)

    assert f._datasource_table[site][domain][model][height] == found_uid == fake_uuid


def test_datasource_add_lookup():
    f = Footprints()

    fake_datasource = {"tmb_lghg_10m_europe": "mock-uuid-123456"}

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

    f.add_datasources(datasource_uuids=fake_datasource, metadata=fake_metadata)

    assert f.datasources() == ["mock-uuid-123456"]

    lookup = f.datasource_lookup(fake_metadata)

    assert lookup == fake_datasource


def test_wrong_uuid_raises():
    f = Footprints()

    fake_datasource = {"tmb_lghg_10m_europe": "mock-uuid-123456"}

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

    f.add_datasources(datasource_uuids=fake_datasource, metadata=fake_metadata)

    assert f.datasources() == ["mock-uuid-123456"]

    changed_datasource = {"tmb_lghg_10m_europe": "mock-uuid-8888888"}

    with pytest.raises(ValueError):
        f.add_datasources(datasource_uuids=changed_datasource, metadata=fake_metadata)
