import pytest

import pandas as pd
from openghg.retrieve.icos import retrieve
from openghg.dataobjects import SearchResults
from helpers import get_retrieval_data_file, metadata_checker_obssurface

from icoscp.station.station import Station
from icoscp.cpb.dobj import Dobj  # type: ignore

example_metadata = {
    "0": '{"dobj":{"0":"https:\\/\\/meta.icos-cp.eu\\/objects\\/zYxCcjFqcV0gmfMiO4NTUrAg"},"objSpec":{"0":"http:\\/\\/meta.icos-cp.eu\\/resources\\/cpmeta\\/atcCo2L2DataObject"},"nRows":{"0":"27468"},"fileName":{"0":"ICOS_ATC_L2_L2-2021.1_TOH_10.0_CTS_CO2.zip"},"specLabel":{"0":"ICOS ATC CO2 Release"},"columnNames":{"0":null}}',
    "1": '{"objFormat":{"0":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/asciiAtcProductTimeSer","1":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/asciiAtcProductTimeSer","2":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/asciiAtcProductTimeSer","3":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/asciiAtcProductTimeSer","4":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/asciiAtcProductTimeSer"},"colName":{"0":"Flag","1":"NbPoints","2":"Stdev","3":"TIMESTAMP","4":"co2"},"valueType":{"0":"quality flag","1":"number of points","2":"standard deviation of gas mole fraction","3":"time instant, UTC","4":"CO2 mixing ratio (dry mole fraction)"},"valFormat":{"0":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/bmpChar","1":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/int32","2":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/float32","3":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/iso8601dateTime","4":"http:\\/\\/meta.icos-cp.eu\\/ontologies\\/cpmeta\\/float32"},"unit":{"0":null,"1":null,"2":"\\u00b5mol mol-1","3":null,"4":"\\u00b5mol mol-1"},"qKind":{"0":null,"1":null,"2":"portion","3":null,"4":"portion"},"colTip":{"0":"single-letter quality flag","1":null,"2":null,"3":null,"4":null},"isRegex":{"0":null,"1":null,"2":null,"3":null,"4":null}}',
    "2": '{"dobj":{"0":"https:\\/\\/meta.icos-cp.eu\\/objects\\/zYxCcjFqcV0gmfMiO4NTUrAg"},"stationName":{"0":"Torfhaus"},"stationId":{"0":"TOH"},"samplingHeight":{"0":"10.0"},"longitude":{"0":"10.535"},"latitude":{"0":"51.8088"},"elevation":{"0":"801.0"},"theme":{"0":"Atmospheric data"}}',
}


def test_icos_retrieve_invalid_site(mocker, capfd):
    s = Station()
    s._valid = False

    mocker.patch("icoscp.station.station.get", return_value=s)

    no_data = retrieve(site="ABC123")

    assert no_data is None

    out, _ = capfd.readouterr()

    assert out.rstrip() == "Please check you have passed a valid ICOS site."


def test_icos_retrieve_and_store(mocker, capfd):
    pid_csv = get_retrieval_data_file(filename="test_pids_icos.csv.gz")
    pid_df = pd.read_csv(pid_csv)

    valid_station = Station()
    valid_station._valid = True

    mocker.patch("icoscp.station.station.get", return_value=valid_station)
    mocker.patch.object(Station, "data", return_value=pid_df)

    mock_dobj_file = get_retrieval_data_file(filename="sample_icos_site.csv.gz")
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

    toh_metadata_path = get_retrieval_data_file(filename="toh_metadata.json")
    toh_metadata = toh_metadata_path.read_bytes()

    mocker.patch("openghg.util.download_data", return_value=toh_metadata)
    # We patch this here so we can make sure we're getting the result from retrieve_all and not from
    # search
    retrieve_all = mocker.patch.object(
        SearchResults, "retrieve_all", side_effect=SearchResults.retrieve_all, autospec=True
    )

    retrieved_data_first = retrieve(site="TOH")

    data = retrieved_data_first.data
    metadata = retrieved_data_first.metadata

    assert metadata_checker_obssurface(metadata=metadata, species="co2")

    expected_metadata = {
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
        "elevation": "801m",
        "data_owner": "dagmar kubistin",
        "data_owner_email": "dagmar.kubistin@dwd.de",
        "station_height_masl": "801m",
        "licence": "https://creativecommons.org/licenses/by/4.0",
        "instrument": "co2-ch4-h2o picarro analyzer",
        "network": "icos",
        "data_type": "timeseries",
        "data_source": "icoscp",
        "icos_data_level": "2",
        "conditions_of_use": "ensure that you contact the data owner at the outset of your project.",
        "source": "in situ measurements of air",
        "conventions": "cf-1.8",
        "processed_by": "openghg_cloud",
        "calibration_scale": "unknown",
        "sampling_period": "not_set",
        "sampling_period_unit": "s",
        "dobj_pid": "https://meta.icos-cp.eu/objects/zYxCcjFqcV0gmfMiO4NTUrAg",
        "citation_string": "Kubistin, D., Plaß-Dülmer, C., Arnold, S., Lindauer, M., Müller-Williams, J., Schumacher, M., ICOS RI, 2021. ICOS ATC CO2 Release, Torfhaus (147.0 m), 2017-12-12–2021-01-31, https://hdl.handle.net/11676/y3-5_I70nW_F5PYN4i8m7WjO",
        "instrument_data": [
            "CO2-CH4-H2O Picarro Analyzer",
            "http://meta.icos-cp.eu/resources/instruments/ATC_457",
            "CO2-CH4-H2O Picarro Analyzer",
            "http://meta.icos-cp.eu/resources/instruments/ATC_271",
        ],
        "Conventions": "CF-1.8",
    }

    assert expected_metadata.items() <= metadata.items()

    data.time[0] == pd.Timestamp("2017-12-13T00:00:00")
    data["co2"][0] == pytest.approx(420.37399)
    data["co2_variability"][0] == 0.118
    data["co2_number_of_observations"][0] == 4

    assert retrieve_all.call_count == 0

    retrieved_data_second = retrieve(site="TOH")

    assert retrieve_all.call_count == 1

    assert dobj_mock.call_count == 12
    assert get_mock.call_count == 12

    # At the moment Datasource lowercases all the metadata, this behaviour should be changed
    # assert retrieved_data_first.metadata == retrieved_data_second.metadata
    assert retrieved_data_first.data.co2.equals(retrieved_data_second.data.co2)

    # Now we do a force retrieve and make sure we get the correct message printed
    retrieve(site="TOH", force_retrieval=True)

    out, _ = capfd.readouterr()

    assert "There is no new data to process." in out.strip()


# def test_retrieve_twice():
#     wao_one = retrieve(site="WAO", species="ch4")

#     wao_data = retrieve(site="WAO", species="CH4", data_level=1)

#     wao_data2 = retrieve(site="WAO", species="CH4", data_level=1)
