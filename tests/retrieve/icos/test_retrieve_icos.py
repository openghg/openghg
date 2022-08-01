import pytest

import pandas as pd
from openghg.retrieve.icos import retrieve
from openghg.dataobjects import SearchResults
from helpers import get_retrieval_data_file, metadata_checker_obssurface
import json
from icoscp.station.station import Station
from icoscp.cpb.dobj import Dobj  # type: ignore


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

    example_metadata_path = get_retrieval_data_file(filename="wao_co2_10m_metadata.json")
    example_metadata = json.loads(example_metadata_path.read_text())

    mocker.patch("icoscp.station.station.get", return_value=valid_station)
    mocker.patch.object(Station, "data", return_value=pid_df)

    mock_dobj_file = get_retrieval_data_file(filename="sample_icos_site.csv.gz")
    sample_icos_data = pd.read_csv(mock_dobj_file)

    # Mock the info property on the Dobj class
    mocker.patch("icoscp.cpb.dobj.Dobj.meta", return_value=example_metadata, new_callable=mocker.PropertyMock)

    mock_Dobj = Dobj()

    dobj_mock = mocker.patch("icoscp.cpb.dobj.Dobj", return_value=mock_Dobj)
    get_mock = mocker.patch.object(Dobj, "get", return_value=sample_icos_data)

    # We patch this here so we can make sure we're getting the result from retrieve_all and not from
    # search
    retrieve_all = mocker.patch.object(
        SearchResults, "retrieve_all", side_effect=SearchResults.retrieve_all, autospec=True
    )

    retrieved_data_first = retrieve(site="WAO")

    data = retrieved_data_first.data
    metadata = retrieved_data_first.metadata

    assert metadata_checker_obssurface(metadata=metadata, species="co2")

    expected_metadata = {
        "species": "co2",
        "instrument": "ftir",
        "instrument_uri": "http://meta.icos-cp.eu/resources/instruments/atc_505",
        "site": "wao",
        "measurement_type": "co2 mixing ratio (dry mole fraction)",
        "units": "µmol mol-1",
        "sampling_height": "10m",
        "sampling_height_units": "metres",
        "inlet": "10m",
        "station_long_name": "wao",
        "station_latitude": "52.95",
        "station_longitude": "1.121",
        "station_altitude": "31m",
        "data_owner": "andrew manning",
        "data_owner_email": "a.manning@uea.ac.uk",
        "station_height_masl": "31m",
        "licence_name": "icos ccby4 data licence",
        "licence_info": "http://meta.icos-cp.eu/ontologies/cpmeta/icoslicence",
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
        "citation_string": "Forster, G., ICOS RI, 2022. ICOS ATC NRT CO2 growing time series, Weybourne (10.0 m), 2022-03-01–2022-07-26, https://hdl.handle.net/11676/XRijo66u4lkxVVk5osjM84Oo",
        "Conventions": "CF-1.8",
    }

    assert expected_metadata.items() <= metadata.items()

    data.time[0] == pd.Timestamp("2017-12-13T00:00:00")
    data["co2"][0] == pytest.approx(420.37399)
    data["co2_variability"][0] == 0.118
    data["co2_number_of_observations"][0] == 4

    assert retrieve_all.call_count == 0

    retrieved_data_second = retrieve(site="WAO")

    assert retrieve_all.call_count == 1

    assert dobj_mock.call_count == 12
    assert get_mock.call_count == 12

    # At the moment Datasource lowercases all the metadata, this behaviour should be changed
    # assert retrieved_data_first.metadata == retrieved_data_second.metadata
    assert retrieved_data_first.data.co2.equals(retrieved_data_second.data.co2)

    # Now we do a force retrieve and make sure we get the correct message printed
    retrieve(site="WAO", force_retrieval=True)

    out, _ = capfd.readouterr()

    assert "There is no new data to process." in out.strip()
