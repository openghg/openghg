from openghg.standardise.surface import parse_agage
from helpers import get_surface_datapath


def test_parse_agage():
    folder_path = get_surface_datapath(filename="example_file.nc", source_format="AGAGE").parent
    parsed_data = parse_agage(data_folder=folder_path)

    ds = parsed_data["0"]["data"]
    metadata = parsed_data["0"]["metadata"]

    assert "hfc365mfc" in ds.variables
    assert "hfc365mfc_repeatability" in ds.variables

    expected_metadata = {
        "data_owner_email": "martin.vollmer@empa.ch",
        "data_owner": "Martin K. Vollmer, Stefan Reimann",
        "station_long_name": "Jungfraujoch, Switzerland",
        "inlet_base_elevation_masl": "3559",
        "inlet_latitude": "46.547767",
        "inlet_longitude": "7.985883",
        "inlet_comment": "Monch-2012 and Monch-2020 inlets",
        "species": "hfc-365mfc",
        "calibration_scale": "SIO-14",
        "units": "1e-12",
        "site_code": "JFJ",
        "network": "AGAGE",
        "instrument": "GCMS-Medusa",
        "instrument_date": "2007-12-07",
        "instrument_1": "GCMS-ADS",
        "instrument_date_1": "2000-01-05",
        "filename_original": "agage_sample_jfj_hfc365mfc.nc",
        "file_hash": "ed02ba9c23e4af3b1e61ec5dbf9864b968b892e4",
        "site": "JFJ",
        "inlet": "14",
        "species_label": "hfc365mfc",
    }

    assert metadata == expected_metadata
