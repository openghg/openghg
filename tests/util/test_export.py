from pandas import DataFrame, date_range
from pathlib import Path
import datetime
from openghg.util import to_dashboard, to_dashboard_mobile
from openghg.dataobjects import ObsData
from openghg.standardise.surface import parse_glasow_licor
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

    site_A.attrs = {"station_longitude": 52.123, "station_latitude": 52.123}

    metadata = {
        "network": "BEACO2N",
        "site": "test_site",
        "instrument": "picarro",
        "inlet": "100m",
        "species": "co2",
    }

    obs = ObsData(data=site_A, metadata=metadata)

    for_export = to_dashboard(data=obs, selected_vars=["A"])

    expected_export = {
        "co2": {
            "BEACO2N": {
                "test_site": {
                    "100m": {
                        "picarro": {
                            "data": {
                                "a": {
                                    "3660000": 0,
                                    "262860000": 3,
                                    "522060000": 6,
                                    "781260000": 9,
                                    "1040460000": 12,
                                    "1299660000": 15,
                                    "1558860000": 18,
                                    "1818060000": 21,
                                    "2077260000": 24,
                                    "2336460000": 27,
                                    "2595660000": 30,
                                    "2854860000": 33,
                                    "3114060000": 36,
                                    "3373260000": 39,
                                    "3632460000": 42,
                                    "3891660000": 45,
                                    "4150860000": 48,
                                    "4410060000": 51,
                                    "4669260000": 54,
                                    "4928460000": 57,
                                    "5187660000": 60,
                                    "5446860000": 63,
                                    "5706060000": 66,
                                    "5965260000": 69,
                                    "6224460000": 72,
                                    "6483660000": 75,
                                    "6742860000": 78,
                                    "7002060000": 81,
                                    "7261260000": 84,
                                    "7520460000": 87,
                                    "7779660000": 90,
                                    "8038860000": 93,
                                    "8298060000": 96,
                                    "8557260000": 99,
                                }
                            },
                            "metadata": {
                                "network": "BEACO2N",
                                "site": "test_site",
                                "instrument": "picarro",
                                "inlet": "100m",
                                "latitude": 52.123,
                                "longitude": 52.123,
                                "species": "co2"
                            },
                        }
                    }
                }
            }
        }
    }

    assert for_export == expected_export

    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir).joinpath("test_export.json")
        to_dashboard(data=obs, selected_vars=["A"], filename=tmp_path)

        assert tmp_path.exists()
        exported_data = json.loads(tmp_path.read_text())
        assert exported_data == for_export


def test_to_dashboard_mobile_return_dict():
    test_data = get_mobile_datapath(filename="glasgow_licor_sample.txt")

    data = parse_glasow_licor(filepath=test_data)

    exported = to_dashboard_mobile(data=data)

    exported_data = exported["ch4"]["data"]

    lon_data = exported_data["lon"]
    lat_data = exported_data["lat"]
    ch4_data = exported_data["z"]

    assert lon_data[:2] == [-4.2321, -4.23209667]
    assert lat_data[:2] == [55.82689833, 55.82698]
    assert ch4_data[:2] == [13.43, 21.05]
    assert exported["ch4"]["metadata"] == {
        "units": "ppb",
        "notes": "measurement value is methane enhancement over background",
        "sampling_period": "NOT_SET",
    }
