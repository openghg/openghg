# import os
# import uuid

# import numpy as np
# import pandas as pd
# import pytest


# mocked_uuid = "00000000-0000-1111-00000-000000000000"


# @pytest.fixture(scope="session")
# def data():

#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     return pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")


# @pytest.fixture
# def mock_uuid(monkeypatch):
#     def mock_uuid():
#         return mocked_uuid

#     monkeypatch.setattr(uuid, "uuid4", mock_uuid)
