import io

from helpers import get_retrieval_data_file
from openghg.cloud import package_from_function
from openghg.dataobjects import SearchResults
from openghg.retrieve.ceda import retrieve_surface
from pandas import Timestamp
from xarray import load_dataset


def test_ceda_retrieve_cloud_no_results(monkeypatch, mocker):
    monkeypatch.setenv("OPENGHG_HUB", "1")

    return_val = {"content": {"found": False}}

    mocker.patch("openghg.cloud.call_function", return_value=return_val)

    res = retrieve_surface(site="hfd")

    assert res is None


def test_ceda_retrieve_cloud(monkeypatch, mocker):
    monkeypatch.setenv("OPENGHG_HUB", "1")

    sample_hfd = get_retrieval_data_file(filename="sample_of_bristol-crds_heathfield_20130101_co2-100m.nc")

    mock_data = sample_hfd.read_bytes()
    mock_metadata = {"site": "london", "species": "tiger"}

    buf = io.BytesIO(mock_data)
    ds = load_dataset(buf)

    packed = package_from_function(data=mock_data, metadata=mock_metadata)

    return_val = {"content": {"found": True, "data": {"1": packed}}}

    mocker.patch("openghg.cloud.call_function", return_value=return_val)

    res = retrieve_surface(site="hfd")

    assert res.data.equals(ds)
    assert res.metadata == mock_metadata


def test_ceda_retrieve(mocker):
    sample_hfd = get_retrieval_data_file(filename="sample_of_bristol-crds_heathfield_20130101_co2-100m.nc")

    ds_bytes = sample_hfd.read_bytes()

    download_data = mocker.patch("openghg.util.download_data", return_value=ds_bytes)
    retrieve_all = mocker.patch.object(
        SearchResults, "retrieve_all", side_effect=SearchResults.retrieve_all, autospec=True
    )

    bsd_data = retrieve_surface(
        url="http://test-url-123.openghg/bristol-crds_heathfield_20130101_co2-100m.nc"
    )

    assert retrieve_all.call_count == 0
    assert download_data.call_count == 1

    data = bsd_data.data
    metadata = bsd_data.metadata

    data.time[0] == Timestamp("2013-11-20T12:51:30")
    data.time[-1] == Timestamp("2013-11-20T20:53:3")
    data["co2"][0] == 401.41
    data["co2"][-1] == 406.55

    expected_metadata = {
        "comment": "Cavity ring-down measurements. Output from GCWerks",
        "Source": "In situ measurements of air",
        "Processed by": "Aoife Grant, University of Bristol (aoife.grant@bristol.ac.uk)",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "data_owner": "Simon O'Doherty",
        "inlet_height_magl": 100.0,
        "Conventions": "CF-1.6",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "File created": "2018-10-22 16:05:33.492535",
        "station_long_name": "Heathfield, UK",
        "station_height_masl": 150.0,
        "station_latitude": 50.97675,
        "station_longitude": 0.23048,
        "Calibration_scale": "NOAA-2007",
        "species": "co2",
        "data_type": "surface",
        "data_source": "ceda_archive",
        "network": "CEDA_RETRIEVED",
        "sampling_period": "NA",
        "site": "HFD",
        "inlet": "100m",
    }

    assert expected_metadata.items() <= metadata.items()

    second_bsd_data = retrieve_surface(
        url="http://test-url-123.openghg/bristol-crds_heathfield_20130101_co2-100m.nc"
    )

    assert retrieve_all.call_count == 1
    assert download_data.call_count == 1

    assert bsd_data.data.equals(second_bsd_data.data)
