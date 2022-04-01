import pytest
from openghg.client import search


def test_search_and_download(process_crds):
    results = search(species="co2", site="hfd")

    keys = results.keys(site="hfd", species="co2", inlet="100m")

    # We don't have any ranked data
    unranked_keys = keys["unranked"]
    assert len(unranked_keys) == 6

    metadata = results.metadata(site="hfd", species="co2", inlet="100m")

    expected_metadata = {
        "site": "hfd",
        "instrument": "picarro",
        "sampling_period": "60.0",
        "inlet": "100m",
        "port": "10",
        "type": "air",
        "network": "decc",
        "species": "co2",
        "calibration_scale": "wmo-x2007",
        "long_name": "heathfield",
        "data_owner": "simon o'doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "station_longitude": 0.23048,
        "station_latitude": 50.97675,
        "station_long_name": "heathfield, uk",
        "station_height_masl": 150.0,
        "inlet_height_magl": "100m",
        "data_type": "timeseries",
    }

    assert metadata == expected_metadata

    data = results.retrieve(site="hfd", species="co2", inlet="100m")
    data = data.data

    assert data["co2"][0] == pytest.approx(414.21)
    assert data["co2_variability"][-1] == pytest.approx(0.247)
    assert data["co2_number_of_observations"][10] == 19.0
