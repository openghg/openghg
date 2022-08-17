from dataclasses import FrozenInstanceError

import pytest
from openghg.dataobjects import ObsData

# Some of these tests are really just testing that Python does Python correctly but
# I plan on extending the dataclass so I'll leave these to be filled out later


@pytest.fixture(scope="session")
def data():
    return {"data": 123}
    # return pd.DataFrame(np.random.randint(0, 100, size=(100, 4)), columns=list("ABCD")).to_xarray()


@pytest.fixture(scope="session")
def metadata():
    return {"test": 1, "key": 2}


def test_data_frozen(data, metadata):
    obs = ObsData(data=data, metadata=metadata)

    with pytest.raises(FrozenInstanceError):
        obs.doi = "1"


def test_str_representation_correct(metadata):
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
