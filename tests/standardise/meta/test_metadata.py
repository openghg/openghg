import pytest
from openghg.standardise.meta import sync_surface_metadata
from openghg.types import AttrMismatchError


def test_sync_surface_metadata():
    metadata = {
        "site": "sum",
        "network": "NOAA",
        "measurement_type": "flask",
        "species": "ch4",
        "units": "1",
        "sampling_period": "NOT_SET",
        "sampling_period_estimate": "10.0",
        "instrument": "NOT_SET",
        "data_owner": "Ed Dlugokencky",
        "station_longitude": -38.422,
        "station_latitude": 72.5962,
        "calibration_scale": "WMO-CH4-X2004A",
        "inlet": "5m",
        "inlet_height_magl": "5",
    }

    attrs = {
        "site_code": "SUM",
        "site_name": "Summit",
        "site_country": "Greenland",
        "site_country_flag": "http://www.esrl.noaa.gov/gmd/webdata/ccgg/ObsPack/images/flags/GRLD0001.GIF",
        "site_latitude": 72.5962,
        "site_longitude": -38.422,
        "site_elevation": 3209.54,
        "site_elevation_unit": "masl",
        "site_position_comment": "This is the nominal location of the site. The sampling location at many sites has changed over time, and we report here the most recent nominal location. The actual sampling location for each observation is not necessarily the site location. The sampling locations for each observation are reported in the latitude, longitude, and altitude variables.",
        "site_utc2lst": -2.0,
        "site_utc2lst_comment": "Add 'site_utc2lst' hours to convert a time stamp in UTC (Coordinated Universal Time) to LST (Local Standard Time).",
        "site_url": "http://www.esrl.noaa.gov/gmd/obop/sum/",
        "site": "sum",
        "network": "NOAA",
        "measurement_type": "flask",
        "species": "ch4",
        "units": "1",
        "sampling_period": "NOT_SET",
        "sampling_period_estimate": "10.0",
        "instrument": "NOT_SET",
        "data_owner": "Ed Dlugokencky",
        "station_longitude": -38.422,
        "station_latitude": 72.596,
        "calibration_scale": "WMO-CH4-X2004A",
        "inlet": "5m",
        "inlet_height_magl": "5",
        "conditions_of_use": "Ensure that you contact the data owner at the outset of your project.",
        "source": "In situ measurements of air",
        "Conventions": "CF-1.8",
        "file_created": "2022-03-01 10:58:14.001490+00:00",
        "processed_by": "OpenGHG_Cloud",
        "sampling_period_unit": "s",
        "station_long_name": "Summit, Greenland",
        "station_height_masl": 3209.5,
    }

    updated_metadata = sync_surface_metadata(metadata=metadata, attributes=attrs)

    new_meta = {
        "site": "sum",
        "network": "NOAA",
        "measurement_type": "flask",
        "species": "ch4",
        "units": "1",
        "sampling_period": "NOT_SET",
        "sampling_period_estimate": "10.0",
        "instrument": "NOT_SET",
        "data_owner": "Ed Dlugokencky",
        "station_longitude": -38.422,
        "station_latitude": 72.5962,
        "calibration_scale": "WMO-CH4-X2004A",
        "inlet": "5m",
        "inlet_height_magl": "5",
        "station_long_name": "Summit, Greenland",
        "station_height_masl": 3209.5,
    }

    assert metadata != updated_metadata
    assert updated_metadata == new_meta


def test_metadata_latlon_tolerance():
    metadata = {
        "station_longitude": -38.422,
        "station_latitude": 72.5962,
    }

    attrs = {
        "station_longitude": -38.422,
        "station_latitude": 72.5962,
    }

    sync_surface_metadata(metadata, attrs)

    attrs = {
        "station_longitude": -38.422,
        "station_latitude": 72.8,
    }

    with pytest.raises(ValueError):
        sync_surface_metadata(metadata, attrs)

    attrs = {
        "station_longitude": 38.422,
        "station_latitude": -72.8,
    }

    with pytest.raises(ValueError):
        sync_surface_metadata(metadata, attrs)

    attrs = {
        "station_longitude": -38.423,
        "station_latitude": 72.597,
    }

    sync_surface_metadata(metadata, attrs)


def test_ensure_mismatch_raises():
    metadata = {
        "site": "sum",
        "network": "NOAA",
        "measurement_type": "flask",
    }

    attrs = {
        "site": "sum",
        "network": "NOAA",
        "measurement_type": "swallow-carrying-a-flask"}

    with pytest.raises(AttrMismatchError):
        sync_surface_metadata(metadata, attrs)
