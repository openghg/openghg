from openghg.client import TestService
from pandas import Timestamp
import pytest


@pytest.fixture
def mock_timestamp(monkeypatch):
    def mock_timestamp():
        return Timestamp("2001-01-01")

    monkeypatch.setattr(Timestamp, "now", mock_timestamp)


def test_test_service(authenticated_user, mock_timestamp):
    t = TestService(service_url="openghg")

    timestamp = t.test()

    assert timestamp == "Function run at 2001-01-01 00:00:00+00:00"
