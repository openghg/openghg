import datetime
import pandas as pd
import pytest
from helpers import clear_test_stores
from openghg.dataobjects import SearchResults
from openghg.retrieve import search_surface
from openghg.retrieve.icos import retrieve_atmospheric
from openghg.types import AttrMismatchError, MetadataAndData


@pytest.mark.icos
def test_icos_retrieve_skips_datalevel_1_csv_files():
    retrieved_data = retrieve_atmospheric(
        site="BIR", species="co2", store="user", data_level=1, update_mismatch="metadata"
    )

    assert len(retrieved_data) == 3

    first_obs = retrieved_data[0]

    expected_metadata = {
        "species": "co2",
        "network": "icos",
        "data_type": "surface",
        "data_source": "icoscp",
        "source_format": "icos",
        "data_level": "1",
        "site": "bir",
        "inlet": "10m",
        "inlet_height_magl": "10",
        "instrument": "co2-ch4-co-h2o picarro analyzer",
        "sampling_period": "not_set",
        "calibration_scale": "unknown",
        # "data_owner": "chris lunder",
        # "data_owner_email": "crl@nilu.no",
        "station_longitude": 8.2519,
        "station_latitude": 58.3886,
        "station_long_name": "birkenes observatory, norway",
        "station_height_masl": 219.0,
        "dataset_source": "ICOS",
    }

    assert expected_metadata.items() <= first_obs.metadata.items()
    assert "data_owner" in first_obs.metadata and "data_owner_email" in first_obs.metadata


@pytest.mark.icos
def test_icos_retrieve_stores_by_data_level():
    """Check that retrieving data with different levels creates different datasources."""
    retrieve_atmospheric(
        site="BIR",
        species=["CH4"],
        inlet="50m",
        data_level=2,
        dataset_source="European ObsPack",
        update_mismatch="from_source",
        store="user",
    )

    retrieve_atmospheric(
        site="BIR",
        species=["CH4"],
        inlet="50m",
        data_level=1,
        dataset_source="ICOS",
        update_mismatch="from_source",
        store="user",
    )

    res = search_surface(store="user", site="BIR", species="CH4")

    assert len(res) == 2


@pytest.mark.icos
def test_icos_retrieve_skips_obspack_globalview(mocker, caplog):
    clear_test_stores()
    # We patch this here so we can make sure we're getting the result from retrieve_all and not from
    # search
    retrieve_all = mocker.patch.object(
        SearchResults, "retrieve_all", side_effect=SearchResults.retrieve_all, autospec=True
    )

    # 05/01/2023: Added update_mismatch to account for WAO difference
    data_first_retrieval = retrieve_atmospheric(
        site="WAO", species="co2", sampling_height="10m", update_mismatch="metadata", store="user"
    )

    data1 = data_first_retrieval[0].data
    meta1 = data_first_retrieval[0].metadata

    # Previous results from ICOS (pre-07/08/2024) contained
    #     "instrument": "ftir",
    #     "station_height_masl": 31.0,

    # Results from ICOS on 07/08/2024
    expected_metadata = {
        # "station_long_name": "weybourne observatory, uk",
        "station_long_name": "wao",
        "station_latitude": 52.9504,
        "station_longitude": 1.1219,
        "species": "co2",
        "network": "icos",
        "data_type": "surface",
        "data_source": "icoscp",
        "source_format": "icos",
        "data_level": "2",
        "site": "wao",
        "inlet": "10m",
        "inlet_height_magl": "10",
        "instrument": "multiple",
        "sampling_period": "not_set",
        "calibration_scale": "unknown",
        # "data_owner": "andrew manning",
        # "data_owner_email": "a.manning@uea.ac.uk",
        "station_height_masl": 17.0,
        "dataset_source": "ICOS",
    }

    all_keys = set(expected_metadata.keys()).union(set(meta1.keys()))
    for key in all_keys:
        print(f"{key:<20}{expected_metadata.get(key, 'None'):<20}{meta1.get(key, 'None'):<20}")

    assert expected_metadata.items() <= meta1.items()
    assert "data_owner" in meta1 and "data_owner_email" in meta1

    assert retrieve_all.call_count == 0

    # 05/01/2023: Added update_mismatch to account for WAO difference
    data_second_retrieval = retrieve_atmospheric(
        site="WAO", species="co2", sampling_height="10m", update_mismatch="metadata", store="user"
    )

    assert data_second_retrieval is not None

    data2 = (
        data_second_retrieval[0].data
        if isinstance(data_second_retrieval, list)
        else data_second_retrieval.data
    )

    assert retrieve_all.call_count == 1

    assert data1.equals(data2)

    # 05/01/2023: Added update_mismatch to account for WAO difference
    retrieve_atmospheric(
        site="WAO",
        species="co2",
        sampling_height="10m",
        force_retrieval=True,
        update_mismatch="metadata",
        store="user",
    )

    assert "Skipping data that overlaps existing data" in caplog.text


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
    mock_data = (
        pd.DataFrame(
            data={
                "A": range(0, n_days),
                "time": pd.date_range(epoch, epoch + datetime.timedelta(n_days - 1), freq="D"),
            }
        )
        .set_index("time")
        .to_xarray()
    )

    mocker.patch(
        "openghg.retrieve.icos._retrieve._retrieve_remote",
        return_value=[MetadataAndData(metadata=mock_metadata, data=mock_data)],
    )


@pytest.mark.icos
def test_retrieved_prevents_storing_twice(mock_retrieve_remote, caplog):
    """Test if retrieving the same data twice issues a warning the second time."""
    clear_test_stores()

    retrieve_atmospheric(site="tac", store="user", update_mismatch="metadata")
    assert "Skipping data that overlaps existing data" not in caplog.text

    retrieve_atmospheric(site="tac", store="user", update_mismatch="metadata")
    assert "Skipping data that overlaps existing data" in caplog.text


@pytest.mark.icos
def test_force_allows_storing_twice(mock_retrieve_remote, caplog):
    """Test if retrieving the same data twice does *not* issue a warning if
    `force=True` is passed to `retrieve_atmospheric` (and hence propegated down
    to `ObsSurface.store_data`).
    """
    clear_test_stores()

    retrieve_atmospheric(site="tac", store="user", update_mismatch="metadata")
    assert "Skipping data that overlaps existing data" not in caplog.text

    retrieve_atmospheric(site="tac", store="user", force=True, update_mismatch="metadata")
    assert "Skipping data that overlaps existing data" not in caplog.text


@pytest.mark.icos
def test_retrieve_icos_attr_mismatch_error():
    """Test that AttrMismatchError is raised.

    An error is raised in this case because the site info in openghg_defs has
    the station long name for SAC as "Saclay, France", while retrieve_atmospheric
    sets it to "SAC".
    """
    with pytest.raises(AttrMismatchError):
        retrieve_atmospheric(site="SAC", species="ch4", inlet="100m", store="user")


@pytest.mark.icos
def test_retrieve_sac_data_update_attrs_with_bool():
    """Test that ValueError is raised if invalid value `True` is passed to `update_mismatch`."""
    with pytest.raises(ValueError):
        retrieve_atmospheric(site="SAC", species="ch4", inlet="100m", update_mismatch=True, store="user")


@pytest.mark.icos
def test_icos_obspack():
    """Test the combined obspack data retrieval"""
    retrieved_data = retrieve_atmospheric(
        site="ZEP", species="co2", dataset_source="ICOS Combined", update_mismatch="from_source", store="user"
    )

    assert "icos_smr" in retrieved_data.data
