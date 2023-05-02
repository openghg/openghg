import bz2
import datetime
import json
import pickle
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
    dobj_mock = mocker.patch("icoscp.cpb.dobj.Dobj", side_effect=dobjs)

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
        site="WAO", species="co2", sampling_height="10m", update_mismatch="metadata"
    )

    meta1 = data_first_retrieval[0].metadata

    meta1_expected = {
        "species": "co2",
        "instrument": "ftir",
        "site": "wao",
        "measurement_type": "co2 mixing ratio (dry mole fraction)",
        "units": "µmol mol-1",
        "sampling_height": "10m",
        "sampling_height_units": "metres",
        "inlet": "10m",
        "inlet_height_magl": "10",
        "station_long_name": "weybourne observatory, uk",
        "station_latitude": "52.95",
        "station_longitude": "1.121",
        "station_altitude": "31m",
        "station_height_masl": 10.0,
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

    assert meta1_expected.items() <= meta1.items()

    data1 = data_first_retrieval[0].data

    assert data1.time[0] == pd.Timestamp("2021-10-21")
    assert data1["co2"][0] == pytest.approx(415.365997)
    assert data1["co2_variability"][0] == pytest.approx(0.234)

    assert retrieve_all.call_count == 0
    assert get_mock.call_count == 2

    # 05/01/2023: Added update_mismatch to account for WAO difference
    data_second_retrieval = retrieve_atmospheric(
        site="WAO", species="co2", sampling_height="10m", update_mismatch="metadata"
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
        site="WAO", species="co2", sampling_height="10m", update_mismatch="metadata", force_retrieval=True
    )

    assert "There is no new data to process." in caplog.text
