import pytest
import datetime
import json
from pathlib import Path
import gzip

from helpers import get_mobile_datapath
from openghg.dataobjects import ObsData
from openghg.standardise.surface import parse_glasow_licor
from openghg.util import to_dashboard, to_dashboard_mobile
from pandas import DataFrame, date_range


@pytest.fixture
def fake_obsdata():
    n_days = 365
    epoch = datetime.datetime(1970, 1, 1, 1, 1)

    df_site_a = DataFrame(
        data={"co2": range(0, n_days)},
        index=date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
    )

    ds_site_a = df_site_a.to_xarray()

    ds_site_a.attrs = {"station_longitude": 52.123, "station_latitude": 52.123}

    metadata = {
        "network": "AGAGE",
        "site": "test_site",
        "instrument": "picarro",
        "inlet": "100m",
        "species": "co2",
        "units": "ppm",
        "station_long_name": "Test site",
    }

    return ObsData(data=ds_site_a, metadata=metadata)


def test_export_to_dashboard(tmpdir, fake_obsdata):
    export_folder = Path(tmpdir)

    to_dashboard(data=fake_obsdata, export_folder=export_folder)

    dashboard_config = json.loads(export_folder.joinpath("dashboard_config.json").read_text())

    assert dashboard_config == {"selection_level": "inlet", "float_to_int": False, "compressed_json": False}

    complete_metadata = json.loads(export_folder.joinpath("metadata_complete.json").read_text())

    expected_metadata = {
        "co2": {
            "AGAGE": {
                "test_site": {
                    "100m": {
                        "instrument_key": {
                            "metadata": {
                                "station_latitude": 52.123,
                                "station_longitude": 52.123,
                                "species": "co2",
                                "site": "test_site",
                                "network": "AGAGE",
                                "instrument": "instrument_key",
                                "units": "ppm",
                                "station_long_name": "Test site",
                                "inlet": "100m",
                            },
                            "filepath": "measurements/co2_agage_test_site_100m_instrument_key.json",
                        }
                    }
                }
            }
        }
    }

    assert complete_metadata == expected_metadata

    exported_data = json.loads(
        export_folder.joinpath("measurements/co2_AGAGE_test_site_100m_instrument_key.json").read_text()
    )

    sliced = list(exported_data.items())[:4]

    assert sliced == [("3660000", 0), ("262860000", 3), ("522060000", 6), ("781260000", 9)]


def test_to_dashboard_data_saving(tmpdir, fake_obsdata):
    output_folder = Path(tmpdir)
    to_dashboard(data=fake_obsdata, export_folder=output_folder, compress_json=True)

    dashboard_config = json.loads(output_folder.joinpath("dashboard_config.json").read_text())

    assert dashboard_config == {"selection_level": "inlet", "float_to_int": False, "compressed_json": True}

    metadata = json.loads(output_folder.joinpath("metadata_complete.json").read_text())
    filepath = metadata["co2"]["AGAGE"]["test_site"]["100m"]["instrument_key"]["filepath"]

    exported_datapath = output_folder.joinpath(filepath)
    decompressed = gzip.decompress(exported_datapath.read_bytes())
    exported_data = json.loads(decompressed)

    sliced = list(exported_data.items())[:4]

    assert sliced == [("3660000", 0), ("262860000", 3), ("522060000", 6), ("781260000", 9)]

    to_dashboard(data=fake_obsdata, export_folder=output_folder, compress_json=True, float_to_int=True)

    dashboard_config = json.loads(output_folder.joinpath("dashboard_config.json").read_text())
    metadata = json.loads(output_folder.joinpath("metadata_complete.json").read_text())

    assert dashboard_config == {
        "selection_level": "inlet",
        "float_to_int": True,
        "compressed_json": True,
        "float_to_int_multiplier": 1000,
    }

    filepath = metadata["co2"]["AGAGE"]["test_site"]["100m"]["instrument_key"]["filepath"]
    exported_datapath = output_folder.joinpath(filepath)
    decompressed = gzip.decompress(exported_datapath.read_bytes())
    exported_data = json.loads(decompressed)

    sliced = list(exported_data.items())[:4]

    assert sliced == [("3660000", 0), ("262860000", 3000), ("522060000", 6000), ("781260000", 9000)]


def test_to_dashboard_check_source_select_site_raises_notimplemented(fake_obsdata, tmpdir):
    with pytest.raises(NotImplementedError):
        to_dashboard(data=fake_obsdata, export_folder=tmpdir, selection_level="site")


def test_to_dashboard_mock_inlet(fake_obsdata, tmpdir):
    to_dashboard(data=fake_obsdata, export_folder=tmpdir, mock_inlet=True)

    export_folder = Path(tmpdir)
    metadata = json.loads(export_folder.joinpath("metadata_complete.json").read_text())

    assert (
        metadata["co2"]["AGAGE"]["test_site"]["single_inlet"]["instrument_key"]["metadata"]["inlet"]
        == "single_inlet"
    )


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
        "data_type": "surface",
    }
