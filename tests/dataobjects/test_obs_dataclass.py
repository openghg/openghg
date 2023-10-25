import pytest
from openghg.dataobjects import ObsData
import pandas as pd
import numpy as np


@pytest.fixture(scope="session")
def data():
    return pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")).to_xarray()


@pytest.fixture(scope="session")
def metadata():
    return {"test": 1, "key": 2}


def test_str_representation_correct(data, metadata):
    data = {"data": "test"}

    obs = ObsData(data=data, metadata=metadata)

    expected_str = "Data: {'data': 'test'}\nMetadata : {'test': 1, 'key': 2}"

    assert expected_str == str(obs)


def test_all_fields_required(data, metadata):
    with pytest.raises(TypeError):
        obs = ObsData(name="test")

    obs = ObsData(data=data, metadata=metadata)

    assert obs.data == data
    assert obs.metadata == metadata
