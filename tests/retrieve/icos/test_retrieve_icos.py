import datetime
import json

import pandas as pd
import pytest
from helpers import get_retrieval_datapath, metadata_checker_obssurface
from icoscp.cpb.dobj import Dobj  # type: ignore
from icoscp.station.station import Station
from openghg.cloud import package_from_function
from openghg.dataobjects import ObsData, SearchResults
from openghg.retrieve.icos import retrieve_atmospheric
from openghg.util import get_logfile_path


def test_retrieve_icos_cloud(monkeypatch, mocker):
    monkeypatch.setenv("OPENGHG_HUB", "1")
    mocker.patch("openghg.cloud.call_function", return_value={"content": {"found": False}})

    res = retrieve_atmospheric(site="WAO")

    assert res is None

    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)

    mock_metadata = {"site": "london", "species": "tiger"}
    mock_dataset = pd.DataFrame(
        data={"A": range(0, n_days)},
        index=pd.date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
    ).to_xarray()

    mock_obs = ObsData(data=mock_dataset, metadata=mock_metadata)

    datafied = mock_obs.to_data()
    binary_data = datafied["data"]
    metadata = datafied["metadata"]

    packed = package_from_function(data=binary_data, metadata=metadata)

    return_val = {"content": {"found": True, "data": {"1": packed}}}

    mocker.patch("openghg.cloud.call_function", return_value=return_val)

    res = retrieve_atmospheric(site="WAO")

    assert res == mock_obs


def test_icos_retrieve_invalid_site(mocker, caplog):
    s = Station()
    s._valid = False

    mocker.patch("icoscp.station.station.get", return_value=s)

    no_data = retrieve_atmospheric(site="ABC123")

    assert no_data is None

    assert "Please check you have passed a valid ICOS site." in caplog.text


def test_icos_retrieve_and_store(mocker):
    pid_csv = get_retrieval_datapath(filename="test_pids_icos.csv.gz")
    pid_df = pd.read_csv(pid_csv)

    valid_station = Station()
    valid_station._valid = True

    example_metadata_path = get_retrieval_datapath(filename="wao_co2_10m_metadata.json")
    example_metadata = json.loads(example_metadata_path.read_text())

    mocker.patch("icoscp.station.station.get", return_value=valid_station)
    mocker.patch.object(Station, "data", return_value=pid_df)

    mock_dobj_file = get_retrieval_datapath(filename="sample_icos_site.csv.gz")
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

    # 05/01/2023: Added update_metadata_mismatch to account for WAO difference
    retrieved_data_first = retrieve_atmospheric(site="WAO", update_metadata_mismatch=True)

    data = retrieved_data_first.data
    metadata = retrieved_data_first.metadata

    assert metadata_checker_obssurface(metadata=metadata, species="co2")

    expected_metadata = {
        "species": "co2",
        "instrument": "ftir",
        "site": "wao",
        "measurement_type": "co2 mixing ratio (dry mole fraction)",
        "units": "µmol mol-1",
        "sampling_height": "10m",
        "sampling_height_units": "metres",
        "inlet": "10m",
        "station_long_name": "weybourne observatory, uk", # May need to be updated
        # "station_long_name": "wao",
        "station_latitude": "52.95",
        "station_longitude": "1.121",
        "station_altitude": "31m",
        "station_height_masl": "10.0",  # Will need to be updated to 17
        # "station_height_masl": "17.0",
        "data_owner": "andrew manning",
        "data_owner_email": "a.manning@uea.ac.uk",
        "licence_name": "icos ccby4 data licence",
        "licence_info": "http://meta.icos-cp.eu/ontologies/cpmeta/icoslicence",
        "network": "icos",
        "data_type": "surface",
        "data_source": "icoscp",
        "icos_data_level": "2",
        "conditions_of_use": "ensure that you contact the data owner at the outset of your project.",
        "source": "in situ measurements of air",
        "conventions": "cf-1.8",
        "processed_by": "openghg_cloud",
        "calibration_scale": "unknown",
        "sampling_period": "not_set",
        "sampling_period_unit": "s",
        "instrument_data": ["FTIR", "http://meta.icos-cp.eu/resources/instruments/ATC_505"],
        "citation_string": "Forster, G., ICOS RI, 2022. ICOS ATC NRT CO2 growing time series, Weybourne (10.0 m), 2022-03-01–2022-07-26, https://hdl.handle.net/11676/XRijo66u4lkxVVk5osjM84Oo",
        "Conventions": "CF-1.8",
    }

    assert expected_metadata.items() <= metadata.items()

    data.time[0] == pd.Timestamp("2017-12-13T00:00:00")
    data["co2"][0] == pytest.approx(420.37399)
    data["co2_variability"][0] == 0.118
    data["co2_number_of_observations"][0] == 4

    assert retrieve_all.call_count == 0

    # 05/01/2023: Added update_metadata_mismatch to account for WAO difference
    retrieved_data_second = retrieve_atmospheric(site="WAO", update_metadata_mismatch=True)

    assert retrieve_all.call_count == 1

    assert dobj_mock.call_count == 12
    assert get_mock.call_count == 12

    # At the moment Datasource lowercases all the metadata, this behaviour should be changed
    # assert retrieved_data_first.metadata == retrieved_data_second.metadata
    assert retrieved_data_first.data.co2.equals(retrieved_data_second.data.co2)

    # Now we do a force retrieve and make sure we get the correct message printed
    # 05/01/2023: Added update_metadata_mismatch to account for WAO difference
    retrieve_atmospheric(site="WAO", force_retrieval=True, update_metadata_mismatch=True)

    logfile_data = get_logfile_path().read_text()
    assert "There is no new data to process." in logfile_data
