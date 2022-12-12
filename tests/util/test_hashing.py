import datetime

import numpy as np
import pandas as pd
import pytest
from helpers import get_surface_datapath
from openghg.util import hash_bytes, hash_retrieved_data, hash_string


def test_hash_string():
    assert hash_string("a_good_string") == "6f5b8ce628133facca9028899ac55ae4ddedd8d0"

    assert hash_string("a_good_string") == "6f5b8ce628133facca9028899ac55ae4ddedd8d0"


def test_hash_bytes():
    b = "some interesting data with numbers 192823".encode("utf8")

    assert hash_bytes(b) == "1a7ba5032524ac7c63d3588054e0e20315472b9c"

    assert hash_bytes(b) == "1a7ba5032524ac7c63d3588054e0e20315472b9c"

    with pytest.raises(TypeError):
        hash_bytes("silly walks")

    filepath = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")
    binary_bsd = filepath.read_bytes()

    bsd_hash = hash_bytes(data=binary_bsd)

    assert bsd_hash == "3e64b17551395636162f22cf4b37a4cb7aa8506e"


def test_hash_retrieved_data(mocker):
    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)

    random_data = pd.DataFrame(
        data=np.random.randint(0, 100, size=(100, 4)),
        index=pd.date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
        columns=list("ABCD"),
    )
    random_data.index.name = "time"

    epoch = pd.Timestamp("1970-1-1")
    mock = mocker.patch("pandas.Timestamp.now")
    mock.return_value = epoch

    ds = random_data.to_xarray()

    metadata = {
        "site": "rome",
        "instrument": "lyre",
        "inlet": "21m",
        "weather": "sunny",
        "species": "aquila",
    }

    to_hash = {"rome": {"data": ds, "metadata": metadata}}

    hashes = hash_retrieved_data(to_hash=to_hash)

    assert hashes == {"e0e05110e110cfdb1d0d2cc2b45cb98c4b8a9f85": {"rome": "1970-01-01 00:00:00+00:00"}}

    second_hash = hash_retrieved_data(to_hash=to_hash)

    assert second_hash == hashes

    to_hash = {"rome": {"data": ds.head(25), "metadata": metadata}}

    diff_data_hashes = hash_retrieved_data(to_hash=to_hash)

    expected_diff_data = {"c0d2cd5c1cf95fe5966582ed8cbd2cd22a8d2223": {"rome": "1970-01-01 00:00:00+00:00"}}

    assert diff_data_hashes == expected_diff_data
    assert diff_data_hashes != hashes

    metadata = {
        "site": "london",
        "instrument": "harp",
        "inlet": "11m",
        "weather": "cloudy",
        "species": "raven",
    }

    to_hash = {"london": {"data": ds, "metadata": metadata}}

    hashes = hash_retrieved_data(to_hash=to_hash)

    expected_london = {"c7543e8c4285ccf8825fd5d22820c36f9aedcf56": {"london": "1970-01-01 00:00:00+00:00"}}

    assert hashes == expected_london
