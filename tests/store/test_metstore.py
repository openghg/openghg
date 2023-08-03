from pathlib import Path

import pytest
from openghg.objectstore import get_bucket
from openghg.store import METStore
from requests_mock import ANY

# from helpers import clear_test_store

@pytest.fixture(scope="session")
def mock_data():
    return Path(__file__).resolve(strict=True).parent.parent.joinpath("data/request_return.nc").read_bytes()


@pytest.fixture(scope="session")
def mock_return():
    return {"state": "completed", "location": "https://www.example.com"}


@pytest.fixture()
def met_object(requests_mock, mock_return, mock_data):
    requests_mock.post(ANY, json=mock_return, status_code=200)
    requests_mock.get("https://www.example.com", content=mock_data, status_code=503)

    return METStore.retrieve(site="CGO", network="AGAGE", years="2012")


@pytest.mark.skip(reason="Update METStore for new DS lookup")
def test_retrieve(met_object):
    met = met_object

    # Empty the object store to force retrieval
    # clear_test_store()

    expected_metadata = {
        "product_type": "reanalysis",
        "format": "netcdf",
        "variable": ["u_component_of_wind", "v_component_of_wind"],
        "pressure_level": ["975", "1000"],
        "area": [-40.5, 144.5, -40.75, 144.75],
        "site": "CGO",
        "network": "AGAGE",
        "start_date": "2012-01-01 00:00:00+00:00",
        "end_date": "2012-12-31 00:00:00+00:00",
    }

    assert met.metadata == expected_metadata

    assert met.data["longitude"][0] == 144.5
    assert met.data["latitude"][0] == -40.5
    assert met.data["level"][0] == 975


@pytest.mark.skip(reason="Update METStore for new DS lookup")
def test_search(met_object):
    met = METStore.load()
    start_date = "2000-01-01"
    end_date = "2025-01-01"

    results = met.search(search_terms=["CGO", "AGAGE"], start_date=start_date, end_date=end_date)

    assert results.data["longitude"][0] == 144.5
    assert results.data["latitude"][0] == -40.5
    assert results.data["level"][0] == 975


@pytest.mark.skip(reason="Update METStore for new DS lookup")
def test_incorrect_site_or_network_raises():
    with pytest.raises(KeyError):
        METStore.retrieve(site="111", network="AGAGE", years="2012")

    with pytest.raises(KeyError):
        METStore.retrieve(site="CGO", network="EGAGA", years="2012")
