from pathlib import Path
import pytest

from openghg.modules import CRANFIELD


def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")


@pytest.fixture(autouse=True)
def cranfield_path():
    return get_datapath(filename="THB_hourly_means_test.csv", data_type="Cranfield_CRDS")


def test_read_file(cranfield_path):
    c = CRANFIELD()

    data = c.read_file(data_filepath=cranfield_path, sampling_period="1200")

    assert sorted(list(data.keys())) == sorted(["co2", "co", "ch4"])

    ch4_data = data["ch4"]["data"]
    co2_data = data["co2"]["data"]
    co_data = data["co"]["data"]

    assert ch4_data["ch4"][0] == pytest.approx(2585.6510)
    assert ch4_data["ch4 variability"][0] == pytest.approx(75.502187065)

    assert co_data["co"][0] == pytest.approx(289.697545)
    assert co_data["co variability"][0] == pytest.approx(6.999084)

    assert co2_data["co2"][0] == pytest.approx(460.573223)
    assert co2_data["co2 variability"][0] == pytest.approx(0.226956417)

    assert data["co"]["metadata"] == {
        "site": "THB",
        "instrument": "CRDS",
        "sampling_period": "1200",
        "height": "10magl",
        "species": "co",
        "inlet": "10magl",
        "network": "CRANFIELD",
    }

    assert data["co2"]["metadata"] == {
        "site": "THB",
        "instrument": "CRDS",
        "sampling_period": "1200",
        "height": "10magl",
        "species": "co2",
        "inlet": "10magl",
        "network": "CRANFIELD",
    }

    assert data["ch4"]["metadata"] == {
        "site": "THB",
        "instrument": "CRDS",
        "sampling_period": "1200",
        "height": "10magl",
        "species": "ch4",
        "inlet": "10magl",
        "network": "CRANFIELD",
    }
