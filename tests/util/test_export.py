from os import read
from pandas import DataFrame, date_range
from pathlib import Path
import datetime
from openghg.util import to_dashboard, to_dashboard_mobile
from openghg.dataobjects import ObsData
from openghg.modules import read_glasgow_licor
from tempfile import TemporaryDirectory
import json

from helpers import get_mobile_datapath


def test_export_to_dashboard():
    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)

    site_A = DataFrame(
        data={"A": range(0, n_days)},
        index=date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
    ).to_xarray()

    metadata = {"network": "BEACO2N", "site": "test_site"}

    obs = ObsData(data=site_A, metadata=metadata)

    data = {"site_a": {"species_a": {"inlet_a": obs}}}

    for_export = to_dashboard(data=data, selected_vars=["A"])
    expected_export = {
        "beaco2n": {
            "species_a": {
                "site_a": {
                    "data": {
                        "a": {
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
                    },
                    "metadata": {"network": "BEACO2N", "site": "test_site"},
                }
            }
        }
    }

    assert for_export == expected_export

    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir).joinpath("test_export.json")
        to_dashboard(data=data, selected_vars=["A"], filename=tmp_path)

        assert tmp_path.exists()
        exported_data = json.loads(tmp_path.read_text())
        assert exported_data == for_export


def test_to_dashboard_mobile_return_dict():
    test_data = get_mobile_datapath(filename="glasgow_licor_sample.txt")

    data = read_glasgow_licor(filepath=test_data)

    exported = to_dashboard_mobile(data=data)

    exported_data = exported["ch4"]["data"][0]

    assert exported_data["type"] == "densitymapbox"

    lon_data = exported_data["lon"]
    lat_data = exported_data["lat"]
    ch4_data = exported_data["z"]

    assert lon_data[:2] == [-4.2321, -4.23209667]
    assert lat_data[:2] == [55.82689833, 55.82698]
    assert ch4_data[:2] == [13.43, 21.05]
    assert exported["ch4"]["metadata"] == {'units': 'ppb', 'notes': 'measurement value is methane enhancement over background'}

