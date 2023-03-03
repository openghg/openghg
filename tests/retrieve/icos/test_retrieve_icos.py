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
    # PIDs are the URLs for the data
    pid_csv = get_retrieval_datapath(filename="example_pids_wao.tar.gz")
    pid_df = pd.read_csv(pid_csv)

    valid_station = Station()
    valid_station._valid = True

    mocker.patch("icoscp.station.station.get", return_value=valid_station)
    mocker.patch.object(Station, "data", return_value=pid_df)

    # Sample data
    mock_dobj_file = get_retrieval_datapath(filename="sample_icos_site.csv.gz")
    sample_icos_data = pd.read_csv(mock_dobj_file)

    # Here we need to the list of metadata for each
    example_metadata_path = get_retrieval_datapath(filename="wao_co2_metadata.json")
    example_metadata = json.loads(example_metadata_path.read_text())
    # We don't want a StopIteration below so lets have multiple copies
    example_metadata *= 3

    # Same metadata, for dobj.meta
    # Mock the info property on the Dobj class
    mocker.patch("icoscp.cpb.dobj.Dobj.meta", side_effect=example_metadata, new_callable=mocker.PropertyMock)

    mock_Dobj = Dobj()

    dobj_mock = mocker.patch("icoscp.cpb.dobj.Dobj", return_value=mock_Dobj)
    get_mock = mocker.patch.object(Dobj, "get", return_value=sample_icos_data)

    # We patch this here so we can make sure we're getting the result from retrieve_all and not from
    # search
    retrieve_all = mocker.patch.object(
        SearchResults, "retrieve_all", side_effect=SearchResults.retrieve_all, autospec=True
    )

    # 05/01/2023: Added update_metadata_mismatch to account for WAO difference
    retrieved_data_first = retrieve_atmospheric(
        site="WAO", species="co2", sampling_height="10m", update_metadata_mismatch=True
    )

    assert isinstance(retrieved_data_first, list)
    assert retrieved_data_first is not None

    first_obs = retrieved_data_first[0]
    first_obs_data = first_obs.data
    first_obs_metadata = first_obs.metadata

    first_expected_metadata = {
        "species": "co2",
        "instrument": "ftir",
        "site": "wao",
        "measurement_type": "co2 mixing ratio (dry mole fraction)",
        "units": "µmol mol-1",
        "sampling_height": "10m",
        "sampling_height_units": "metres",
        "inlet": "10m",
        "station_long_name": "weybourne observatory, uk",  # May need to be updated
        # "station_long_name": "wao",
        "station_latitude": "52.95",
        "station_longitude": "1.121",
        "station_altitude": "31m",
        # "station_height_masl": "10.0",  # Will need to be updated to 17
        # "station_height_masl": "17.0",
        # "data_owner": "andrew manning",
        # "data_owner_email": "a.manning@uea.ac.uk",
        "licence_name": "icos ccby4 data licence",
        "licence_info": "http://meta.icos-cp.eu/ontologies/cpmeta/icoslicence",
        "network": "icos",
        "data_type": "surface",
        "data_source": "icoscp",
        "source_format": "icos",
        "icos_data_level": "2",
        "conditions_of_use": "ensure that you contact the data owner at the outset of your project.",
        "source": "in situ measurements of air",
        "conventions": "cf-1.8",
        "processed_by": "openghg_cloud",
        "calibration_scale": "unknown",
        "sampling_period": "not_set",
        "sampling_period_unit": "s",
        "instrument_data": ["FTIR", "http://meta.icos-cp.eu/resources/instruments/ATC_505"],
        "citation_string": "Forster, G., Manning, A. (2022). ICOS ATC CO2 Release, Weybourne (10.0 m), 2021-10-21–2022-02-28, ICOS RI, https://hdl.handle.net/11676/NR9p9jxC7B7M46MdGuCOrzD3",
        "dataset_source": "ICOS",
        "Conventions": "CF-1.8",
    }

    assert first_expected_metadata.items() <= first_obs_metadata.items()

    first_obs_data.time[0] == pd.Timestamp("2017-12-13T00:00:00")
    first_obs_data["co2"][0] == pytest.approx(420.37399)
    first_obs_data["co2_variability"][0] == 0.118
    first_obs_data["co2_number_of_observations"][0] == 4

    second_obs = retrieved_data_first[1]

    assert second_obs.metadata["dataset_source"] == "European ObsPack"

    assert second_obs.metadata["instrument_data"] == [
        "FTIR",
        "http://meta.icos-cp.eu/resources/instruments/ATC_505",
        "ULTRAMAT 6-E",
        "http://meta.icos-cp.eu/resources/instruments/ATC_1391",
    ]

    assert retrieve_all.call_count == 0

    # 05/01/2023: Added update_metadata_mismatch to account for WAO difference
    retrieved_data_second = retrieve_atmospheric(
        site="WAO", species="co2", sampling_height="10m", update_metadata_mismatch=True
    )

    assert retrieved_data_second is not None
    assert retrieve_all.call_count == 1

    assert dobj_mock.call_count == 2
    assert get_mock.call_count == 2

    # At the moment Datasource lowercases all the metadata, this behaviour should be changed
    # assert retrieved_data_first.metadata == retrieved_data_second.metadata
    assert retrieved_data_first[0].data.co2.equals(retrieved_data_second[0].data.co2)

    # Now we do a force retrieve and make sure we get the correct message printed

    # 05/01/2023: Added update_metadata_mismatch to account for WAO difference
    retrieve_atmospheric(
        site="WAO", species="co2", sampling_height="10m", force_retrieval=True, update_metadata_mismatch=True
    )

    logfile_data = get_logfile_path().read_text()
    assert "There is no new data to process." in logfile_data
