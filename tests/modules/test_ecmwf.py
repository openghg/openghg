import pytest
from openghg.modules import retrieve_met
from requests_mock import ANY
from pandas import Timestamp
from pathlib import Path


@pytest.fixture(scope="session")
def mock_data():
    filepath = Path(__file__).resolve(strict=True).parent.parent.joinpath(f"data/request_return.nc")

    with open(filepath, "rb") as f:
        mock_data = f.read()

    return mock_data


@pytest.fixture(scope="session")
def mock_return():
    return {"state": "completed", "location": "https://www.example.com"}


def test_retrieve_met(requests_mock, mock_data, mock_return):
    requests_mock.post(ANY, json=mock_return, status_code=200)
    requests_mock.get("https://www.example.com", content=mock_data, status_code=503)

    ecm = retrieve_met(site="CGO", network="AGAGE", years="2012")

    assert ecm.data.time[0] == Timestamp("2012-01-01T00:00:00.000000000")
    assert ecm.data.latitude[0] == -40.5
    assert ecm.data.longitude[0] == 144.5


def test_receive_404(requests_mock, mock_return, mock_data):
    mock_content = "404 Not Found"

    requests_mock.post(ANY, json=mock_return, status_code=200)
    requests_mock.get("https://www.example.com", text=mock_content, status_code=404)

    with pytest.raises(ValueError):
        retrieve_met(site="CGO", network="AGAGE", years="2012")
