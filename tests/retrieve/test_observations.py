from openghg.retrieve._access import get_obs_surface
from openghg.dataobjects._obsdata import ObsData
from pandas import Timestamp

# from openghg.retrieve import get_attributes


def test_get_obs_surface_one_inlet(monkeypatch):
    """
    Test we can access site and species data without needing to specify the inlet
    when this is not needed.
    """
    data = get_obs_surface(site="tac", species="ch4")

    expected_metadata = {
        "site": "tac",
        "instrument": "picarro",
        "sampling_period": "60",
        "inlet": "100m",
        "port": "9",
        "type": "air",
        "network": "decc",
        "species": "ch4",
        "scale": "WMO-X2004A",
        "long_name": "tacolneston",
        "data_type": "timeseries",
        "data_owner": "Simon O'Doherty",
        "data_owner_email": "s.odoherty@bristol.ac.uk",
        "inlet_height_magl": "100m",
        "comment": "Cavity ring-down measurements. Output from GCWerks",
        "Conditions of use": "Ensure that you contact the data owner at the outset of your project.",
        "Source": "In situ measurements of air",
        "Conventions": "CF-1.6",
        "Processed by": "OpenGHG_Cloud",
        "station_longitude": 1.13872,
        "station_latitude": 52.51775,
        "station_long_name": "Tacolneston Tower, UK",
        "station_height_masl": 50.0,
    }

    # TODO - should mock the timestamp_now really
    del data.metadata["File created"]

    assert data.metadata == expected_metadata

    assert data.data.time[0] == Timestamp("2012-07-31T14:50:30")
    assert data.data.time[-1] == Timestamp("2019-06-26T15:53:30")

    assert data.data["mf"][0] == 1905.28
    assert data.data["mf"][-1] == 1933.87
