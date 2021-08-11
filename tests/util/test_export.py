from pandas import DataFrame, date_range
from pathlib import Path
import datetime
from openghg.util import to_dashboard
from openghg.dataobjects import ObsData
from tempfile import TemporaryDirectory
import json


def test_export_to_dashboard():
    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)

    site_A = DataFrame(
        data={"A": range(0, n_days)},
        index=date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
    ).to_xarray()

    obs = ObsData(data=site_A, metadata={"site": "test_site"})

    for_export = to_dashboard(data=obs, selected_vars=["A"])

    assert for_export == {
        "test_site": {
            "A": {
                "3660000": 0,
                "435660000": 5,
                "867660000": 10,
                "1299660000": 15,
                "1731660000": 20,
                "2163660000": 25,
                "2595660000": 30,
                "3027660000": 35,
                "3459660000": 40,
                "3891660000": 45,
                "4323660000": 50,
                "4755660000": 55,
                "5187660000": 60,
                "5619660000": 65,
                "6051660000": 70,
                "6483660000": 75,
                "6915660000": 80,
                "7347660000": 85,
                "7779660000": 90,
                "8211660000": 95,
            }
        }
    }

    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir).joinpath("test_export.json")
        to_dashboard(data=obs, selected_vars=["A"], filename=tmp_path)

        assert tmp_path.exists()
        exported_data = json.loads(tmp_path.read_text())
        assert exported_data == for_export

