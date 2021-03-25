from pathlib import Path
import pytest

from openghg.modules import FOOTPRINTS
from openghg.processing import search_footprints, recombine_datasets
from openghg.objectstore import get_local_bucket

def get_datapath(filename):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/footprints/{filename}")


def test_read_footprint():
    _ = get_local_bucket(empty=True)

    datapath = get_datapath("footprint_test.nc")
    model_params = {"simulation_params": "123"}

    site = "TMB"
    network = "LGHG"
    height = "10m"
    domain = "EUROPE"

    start_date = "2010-01-01"
    end_date = "2022-01-01"

    FOOTPRINTS.read_file(filepath=datapath, site=site, network=network, height=height, domain=domain, model_params=model_params)

    # Get the footprint data
    footprint_results = search_footprints(sites=site, domains=domain, inlet=height, start_date=start_date, end_date=end_date)

    fp_site_key = list(footprint_results.keys())[0]

    footprint_keys = footprint_results[fp_site_key]["keys"]
    footprint_data = recombine_datasets(data_keys=footprint_keys, sort=False)

    assert list(footprint_data.coords.keys()) == ["time", "lon", "lat", "lev", "height", "lat_high", "lon_high"]
    assert list(footprint_data.dims) == ["height", "index", "lat", "lat_high", "lev", "lon", "lon_high", "time"]

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

    assert (
        footprint_data.attrs["variables"]
        == [
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
    )

    del footprint_data.attrs["processed"]
    del footprint_data.attrs["heights"]
    del footprint_data.attrs["variables"]

    expected_attrs = {
        "author": "OpenGHG Cloud",
        "data_type": "footprint",
        "site": "TMB",
        "network": "LGHG",
        "height": "10m",
        "domain": "EUROPE",
        "start_date": "2020-08-01 00:00:00+00:00",
        "end_date": "2020-08-01 00:00:00+00:00",
        "max_longitude": 39.38,
        "min_longitude": -97.9,
        "max_latitude": 79.057,
        "min_latitude": 10.729,
        "time_resolution": "standard_time_resolution",
    }

    assert footprint_data.attrs == expected_attrs

    footprint_data["fp_low"].max().values == 0.43350983
    footprint_data["fp_high"].max().values == 0.11853027
    footprint_data["pressure"].max().values == 1011.92
    footprint_data["fp_low"].min().values == 0.0
    footprint_data["fp_high"].min().values == 0.0
    footprint_data["pressure"].min().values == 1011.92


def test_read_same_footprint_twice_raises():
    datapath = get_datapath("footprint_test.nc")
    model_params = {"simulation_params": "123"}

    with pytest.raises(ValueError):
        FOOTPRINTS.read_file(filepath=datapath, site="TMB", network="LGHG", domain="EUROPE", height="10magl", model_params=model_params)
