import pytest

import pandas as pd
from openghg.retrieve.icos import retrieve
from openghg.dataobjects import SearchResults
from helpers import get_icos_test_file, metadata_checker_obssurface

from icoscp.station.station import Station
from icoscp.cpb.dobj import Dobj  # type: ignore
from openghg.store import ObsSurface

example_metadata = {
    "0": '{"dobj":{"0":"https:\\/\\/meta.icos-cp.eu\\/objects\\/zYxCcjFqcV0gmfMiO4NTUrAg"},"objSpec":{"0":"http:\\/\\/meta.icos-cp.eu\\/resources\\/cpmeta\\/atcCo2L2DataObject"},"nRows":{"0":"27468"},"fileName":{"0":"ICOS_ATC_L2_L2-2021.1_TOH_10.0_CTS_CO2.zip"},"specLabel":{"0":"ICOS ATC CO2 Release"},"columnNames":{"0":null}}',
    "1": '{"objFormat":{"0":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/asciiAtcProductTimeSer","1":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/asciiAtcProductTimeSer","2":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/asciiAtcProductTimeSer","3":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/asciiAtcProductTimeSer","4":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/asciiAtcProductTimeSer"},"colName":{"0":"Flag","1":"NbPoints","2":"Stdev","3":"TIMESTAMP","4":"co2"},"valueType":{"0":"quality flag","1":"number of points","2":"standard deviation of gas mole fraction","3":"time instant, UTC","4":"CO2 mixing ratio (dry mole fraction)"},"valFormat":{"0":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/bmpChar","1":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/int32","2":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/float32","3":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/iso8601dateTime","4":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/float32"},"unit":{"0":null,"1":null,"2":"\\u00b5mol mol-1","3":null,"4":"\\u00b5mol mol-1"},"qKind":{"0":null,"1":null,"2":"portion","3":null,"4":"portion"},"colTip":{"0":"single-letter quality flag","1":null,"2":null,"3":null,"4":null},"isRegex":{"0":null,"1":null,"2":null,"3":null,"4":null}}',
    "2": '{"dobj":{"0":"https:\\/\\/meta.icos-cp.eu\\/objects\\/zYxCcjFqcV0gmfMiO4NTUrAg"},"stationName":{"0":"Torfhaus"},"stationId":{"0":"TOH"},"samplingHeight":{"0":"10.0"},"longitude":{"0":"10.535"},"latitude":{"0":"51.8088"},"elevation":{"0":"801.0"},"theme":{"0":"Atmospheric data"}}',
}


def test_wmo_retrieve():
    from openghg.retrieve.icos import retrieve

    site = "WAO"  # Weybourne
    species = "ch4"

    out = retrieve(site, species)


def test_icos_retrieve_no_local(mocker):
    pid_csv = get_icos_test_file(filename="test_pids_icos.csv.gz")
    pid_df = pd.read_csv(pid_csv)

    valid_station = Station()
    valid_station._valid = True

    mocker.patch("icoscp.station.station.get", return_value=valid_station)
    mocker.patch.object(Station, "data", return_value=pid_df)

    mock_dobj_file = get_icos_test_file(filename="sample_icos_site.csv.gz")
    sample_icos_data = pd.read_csv(mock_dobj_file)

    metadata = []
    for i, df_data in sorted(example_metadata.items()):
        df = pd.read_json(df_data)
        metadata.append(df)

    # Mock the info property on the Dobj class
    mocker.patch("icoscp.cpb.dobj.Dobj.info", return_value=metadata, new_callable=mocker.PropertyMock)

    mock_Dobj = Dobj()

    dobj_mock = mocker.patch("icoscp.cpb.dobj.Dobj", return_value=mock_Dobj)
    get_mock = mocker.patch.object(Dobj, "get", return_value=sample_icos_data)

    toh_metadata_path = get_icos_test_file(filename="toh_metadata.json")
    toh_metadata = toh_metadata_path.read_bytes()

    mocker.patch("openghg.util.download_data", return_value=toh_metadata)
    osbsurface_store = mocker.patch.object(ObsSurface, "store_data")

    retrieved_data = retrieve(site="TOH")

    assert dobj_mock.call_count == 12
    assert get_mock.call_count == 12
    assert osbsurface_store.call_count == 1

    data = retrieved_data.data
    metadata = retrieved_data.metadata

    assert metadata_checker_obssurface(metadata=metadata, species="co2")

    data.time[0] == pd.Timestamp("2017-12-13T00:00:00")
    data["co2"][0] == pytest.approx(420.37399)
    data["co2_variability"][0] == 0.118
    data["co2_number_of_observations"][0] == 4

    # Now disable the store_data mock and store the data,
    # then check we get the data
    osbsurface_store.stop()

    # Now retrieve the data
    toh_data = retrieve(site="TOH")

    assert dobj_mock.call_count == 24
    assert get_mock.call_count == 24

    metadata = toh_data.metadata

    expected_metadata = {
        "dobj_pid": "https://meta.icos-cp.eu/objects/zyxccjfqcv0gmfmio4nturag",
        "species": "co2",
        "meas_type": "co2 mixing ratio (dry mole fraction)",
        "units": "µmol mol-1",
        "site": "toh",
        "station_long_name": "torfhaus",
        "sampling_height": "10m",
        "sampling_height_units": "metres",
        "inlet": "10m",
        "station_latitude": "51.8088",
        "station_longitude": "10.535",
        "elevation": "801",
        "data_owner": "dagmar kubistin",
        "data_owner_email": "dagmar.kubistin@dwd.de",
        "station_height_masl": 801.0,
        "citation_string": "kubistin, d., plaß-dülmer, c., arnold, s., lindauer, m., müller-williams, j., schumacher, m., icos ri, 2021. icos atc co2 release, torfhaus (147.0 m), 2017-12-12–2021-01-31, https://hdl.handle.net/11676/y3-5_i70nw_f5pyn4i8m7wjo",
        "licence": "https://creativecommons.org/licenses/by/4.0",
        "instrument": "co2-ch4-h2o picarro analyzer",
        "instrument_data": [
            "co2-ch4-h2o picarro analyzer",
            "http://meta.icos-cp.eu/resources/instruments/atc_457",
            "co2-ch4-h2o picarro analyzer",
            "http://meta.icos-cp.eu/resources/instruments/atc_271",
        ],
        "network": "icos",
        "data_type": "timeseries",
        "data_source": "icoscp",
        "conditions_of_use": "ensure that you contact the data owner at the outset of your project.",
        "source": "in situ measurements of air",
        "conventions": "cf-1.8",
        "processed_by": "openghg_cloud",
        "calibration_scale": "unknown",
        "sampling_period": "not_set",
        "sampling_period_unit": "s",
    }

    assert expected_metadata.items() <= metadata.items()


def test_icos_retrieve_invalid_site(mocker, capfd):
    s = Station()
    s._valid = False

    mocker.patch("icoscp.station.station.get", return_value=s)

    no_data = retrieve(site="ABC123")

    assert no_data is None

    out, _ = capfd.readouterr()

    assert out.rstrip() == "Please check you have passed a valid ICOS site."


def test_icos_retrieve_and_store(mocker):
    pid_csv = get_icos_test_file(filename="test_pids_icos.csv.gz")
    pid_df = pd.read_csv(pid_csv)

    valid_station = Station()
    valid_station._valid = True

    mocker.patch("icoscp.station.station.get", return_value=valid_station)
    mocker.patch.object(Station, "data", return_value=pid_df)

    mock_dobj_file = get_icos_test_file(filename="sample_icos_site.csv.gz")
    sample_icos_data = pd.read_csv(mock_dobj_file)

    metadata = []
    for _, df_data in sorted(example_metadata.items()):
        df = pd.read_json(df_data)
        metadata.append(df)

    # Mock the info property on the Dobj class
    mocker.patch("icoscp.cpb.dobj.Dobj.info", return_value=metadata, new_callable=mocker.PropertyMock)

    mock_Dobj = Dobj()

    dobj_mock = mocker.patch("icoscp.cpb.dobj.Dobj", return_value=mock_Dobj)
    get_mock = mocker.patch.object(Dobj, "get", return_value=sample_icos_data)

    toh_metadata_path = get_icos_test_file(filename="toh_metadata.json")
    toh_metadata = toh_metadata_path.read_bytes()

    mocker.patch("openghg.util.download_data", return_value=toh_metadata)
    # We patch this here so we can make sure we're getting the result from retrieve_all and not from
    # search
    retrieve_all = mocker.patch.object(
        SearchResults, "retrieve_all", side_effect=SearchResults.retrieve_all, autospec=True
    )

    retrieved_data_first = retrieve(site="TOH")

    assert retrieve_all.call_count == 0

    retrieved_data_second = retrieve(site="TOH")

    assert retrieve_all.call_count == 1

    assert dobj_mock.call_count == 12
    assert get_mock.call_count == 12

    assert retrieved_data_first.metadata == retrieved_data_second.metadata
    assert retrieved_data_first.data.co2.equals(retrieved_data_second.data.co2)
