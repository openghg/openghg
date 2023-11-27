import bz2
import datetime
import json
import pickle
import pandas as pd
import pytest
from helpers import get_retrieval_datapath, metadata_checker_obssurface, clear_test_stores
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


def test_icos_retrieve_skips_obspack_globalview(mocker, caplog):
    pids_csv = get_retrieval_datapath(filename="wao_pids.csv.bz2")
    pid_df = pd.read_csv(pids_csv)

    valid_station = Station()
    valid_station._valid = True

    mocker.patch("icoscp.station.station.get", return_value=valid_station)
    # Here we mock the station data for the PIDs to retrieve
    mocker.patch.object(Station, "data", return_value=pid_df)

    dobjs = []
    for n in range(1, 4):
        pkl_path = get_retrieval_datapath(filename=f"dobj{n}.pkl.bz2")
        with bz2.open(pkl_path, "rb") as f:
            dobj = pickle.loads(f.read())

        dobjs.append(dobj)

    dobjs *= 2

    # Mock the dobj values, here we'll get two values we read and the third dobj contains
    # ObsPack GlobalView data that should currently be skipped
    mocker.patch("icoscp.cpb.dobj.Dobj", side_effect=dobjs)

    # Note that we get an extra Unamed column in these dataframes due to the trip to csv and back
    data_dobj1 = pd.read_csv(get_retrieval_datapath(filename="df_1.csv.bz2"))
    data_dobj2 = pd.read_csv(get_retrieval_datapath(filename="df_2.csv.bz2"))

    # The two dataframes that are returned
    # Note we only have two here as the third dobj is ObsPack and
    # the get fails with icoscp 0.1.17
    get_return_vals = [data_dobj1, data_dobj2] * 2
    get_mock = mocker.patch.object(Dobj, "get", side_effect=get_return_vals)

    # We patch this here so we can make sure we're getting the result from retrieve_all and not from
    # search
    retrieve_all = mocker.patch.object(
        SearchResults, "retrieve_all", side_effect=SearchResults.retrieve_all, autospec=True
    )

    # 05/01/2023: Added update_mismatch to account for WAO difference
    data_first_retrieval = retrieve_atmospheric(
        site="WAO", species="co2", sampling_height="10m", update_mismatch="metadata", store="user"
    )

    meta1 = data_first_retrieval[0].metadata

    meta1_expected = {
        "species": "co2",
        "instrument": "ftir",
        "site": "wao",
        # REMOVED sampling_height from metadata --> included inlet
        # - This was to better align with metadata within the obs_surface data_type
        # "sampling_height": "10m",
        # "sampling_height_units": "metres",
        "inlet": "10m",
        "inlet_height_magl": "10",
        "station_long_name": "weybourne observatory, uk",
        "station_latitude": 52.95042,
        "station_longitude": 1.12194,
        # TODO: May need to be updated if station_height versus height_station naming is corrected.
        # "station_altitude": "31m",
        # "station_height_masl": 10.0,
        "station_height_masl": 31.0,
        "network": "icos",
        "data_type": "surface",
        "data_source": "icoscp",
        "source_format": "icos",
        "icos_data_level": "2",
        "calibration_scale": "unknown",  # Update when possible (icoscp Issue - ICOS-Carbon-Portal/pylib#148)
        "sampling_period": "not_set",  # Update when possible (icoscp Issue - ICOS-Carbon-Portal/pylib#148)
        "dataset_source": "ICOS",
    }

    assert meta1_expected.items() <= meta1.items()

    data1 = data_first_retrieval[0].data

    assert data1.time[0] == pd.Timestamp("2021-10-21")
    assert data1["co2"][0] == pytest.approx(415.365997)
    assert data1["co2_variability"][0] == pytest.approx(0.234)

    assert retrieve_all.call_count == 0
    assert get_mock.call_count == 2

    # Check attributes within stored Dataset contain extra keys
    attr1_expected_additional = {
        "measurement_type": "co2 mixing ratio (dry mole fraction)",
        "sampling_height": "10m",
        "sampling_height_units": "metres",
        "licence_name": "ICOS CCBY4 Data Licence",
        "licence_info": "http://meta.icos-cp.eu/ontologies/cpmeta/icosLicence",
        "conditions_of_use": "Ensure that you contact the data owner at the outset of your project.",
        "source": "In situ measurements of air",
        "sampling_period_unit": "s",
        "instrument_data": ["FTIR", "http://meta.icos-cp.eu/resources/instruments/ATC_505"],
        "citation_string": "Forster, G., Manning, A. (2022). ICOS ATC CO2 Release, Weybourne (10.0 m), 2021-10-21–2022-02-28, ICOS RI, https://hdl.handle.net/11676/NR9p9jxC7B7M46MdGuCOrzD3",
        "Conventions": "CF-1.8",
    }

    data1_attrs = data1.attrs

    assert attr1_expected_additional.items() <= data1_attrs.items()

    # 05/01/2023: Added update_mismatch to account for WAO difference
    data_second_retrieval = retrieve_atmospheric(
        site="WAO", species="co2", sampling_height="10m", update_mismatch="metadata", store="user"
    )

    data2 = data_second_retrieval[0].data
    meta2 = data_second_retrieval[0].metadata

    assert retrieve_all.call_count == 1
    assert get_mock.call_count == 2

    assert data1.equals(data2)

    assert (
        "Skipping https://meta.icos-cp.eu/objects/azGntCuTmL7lvAFbOnM6G_c0 as ObsPack GlobalView detected."
        in caplog.text
    )

    # 05/01/2023: Added update_mismatch to account for WAO difference
    retrieve_atmospheric(
        site="WAO",
        species="co2",
        sampling_height="10m",
        force_retrieval=True,
        update_mismatch="metadata",
        store="user",
    )

    assert "There is no new data to process." in caplog.text


@pytest.fixture
def mock_retrieve_remote(mocker):
    mock_metadata = {
                "species": "ch4",
                "site": "tac",
                "station_long_name": "Tacolneston",
                "inlet": "185m",
                "instrument": "picarro",
                "network": "decc",
                "source_format": "icos",
                "data_source": "icoscp",
                "icos_data_level": 1,
            }
    n_days = 100
    epoch = datetime.datetime(1970, 1, 1, 1, 1)
    mock_data = pd.DataFrame(
        data={"A": range(0, n_days), "time": pd.date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D")}
    ).set_index("time").to_xarray()

    mocker.patch("openghg.retrieve.icos._retrieve._retrieve_remote", return_value={"ch4": {"metadata": mock_metadata, "data": mock_data}})



def test_retrieved_hash_prevents_storing_twice(mock_retrieve_remote, caplog):
    """Test if retrieving the same data twice issues a warning the second time."""
    clear_test_stores()

    retrieve_atmospheric(site="tac", store="user")
    assert "There is no new data to process." not in caplog.text

    retrieve_atmospheric(site="tac", store="user")
    assert "There is no new data to process." in caplog.text


def test_force_allows_storing_twice(mock_retrieve_remote, caplog):
    """Test if retrieving the same data twice does *not* issue a warning if
    `force=True` is passed to `retrieve_atmospheric` (and hence propegated down
    to `ObsSurface.store_data`).
    """
    clear_test_stores()

    retrieve_atmospheric(site="tac", store="user")
    assert "There is no new data to process." not in caplog.text

    retrieve_atmospheric(site="tac", store="user", force=True)
    assert "There is no new data to process." not in caplog.text
