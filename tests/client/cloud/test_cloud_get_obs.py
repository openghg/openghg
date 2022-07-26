from helpers import call_function_packager
import pandas as pd
import datetime

from openghg.dataobjects import ObsData
from openghg.util import compress, compress_str, hash_bytes
from openghg.client import get_obs_surface


def test_get_obs_surface(mocker):
    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)

    mock_dataset = pd.DataFrame(
        data={"A": range(0, n_days)},
        index=pd.date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
    ).to_xarray()

    mock_meta = {"some": "metadata"}
    mock_obs = ObsData(data=mock_dataset, metadata=mock_meta)

    for_transfer = mock_obs.to_data()

    sha1_hash = hash_bytes(data=for_transfer["data"])

    to_return = {
        "found": True,
        "data": compress(data=for_transfer["data"]),
        "metadata": compress_str(s=for_transfer["metadata"]),
        "file_metadata": {
            "data": {"sha1_hash": sha1_hash, "compression_type": "gzip"},
            "metadata": {"sha1_hash": False, "compression_type": "bz2"},
        },
    }

    to_return = call_function_packager(status=200, headers={}, content=to_return)

    mocker.patch("openghg.cloud.call_function", return_value=to_return)

    result = get_obs_surface(site="london", species="hawk")

    assert result == mock_obs
