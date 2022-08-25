import pytest
from helpers import get_mobile_datapath
from openghg.standardise.surface import parse_glasow_licor
from pandas import Timestamp


def test_glasgow_licor_read():
    test_data = get_mobile_datapath(filename="glasgow_licor_sample.txt")
    data = parse_glasow_licor(filepath=test_data)

    ch4_data = data["ch4"]["data"]
    metadata = data["ch4"]["metadata"]

    assert ch4_data.time[0] == Timestamp("2021-08-25T14:35:57")
    assert ch4_data.longitude[0] == pytest.approx(-4.2321)
    assert ch4_data.latitude[0] == pytest.approx(55.82689833)
    assert ch4_data.ch4[0] == 13.43

    assert metadata == {
        "units": "ppb",
        "notes": "measurement value is methane enhancement over background",
        "sampling_period": "NOT_SET",
    }
