# from openghg.client import call_test_service
# from pandas import Timestamp
# import pytest


# @pytest.fixture
# def mock_timestamp(monkeypatch):
#     def mock_timestamp():
#         return Timestamp("2001-01-01")

#     monkeypatch.setattr(Timestamp, "now", mock_timestamp)

<<<<<<< HEAD
# @pytest.mark.skip("Marked for removal")
# def test_test_service(authenticated_user, mock_timestamp):
#     timestamp = call_test_service()
=======

@pytest.mark.skip("Marked for removal")
def test_test_service(authenticated_user, mock_timestamp):
    timestamp = call_test_service()
>>>>>>> devel

#     assert timestamp == "Function run at 2001-01-01 00:00:00+00:00"
