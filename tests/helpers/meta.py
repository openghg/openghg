# Check if obssurface metadata contains the minimum expected
from typing import Dict


def combined_surface_metachecker(data: Dict):
    for species, gas_data in data.items():
        metadata = gas_data["metadata"]
        attributes = gas_data["data"].attrs

        assert metadata_checker_obssurface(metadata=metadata, species=species)
        assert attributes_checker_obssurface(attrs=attributes, species=species)


def metadata_checker_obssurface(metadata: Dict, species: str) -> bool:
    """Checks if the passed metadata dictionary contains the minimum expected keys

    Args:
        metadata: Dictionary of metadata
    Returns:
        bool: True if expected keys exist
    """
    # TODO - this could maybe be expanted to make sure latitude / longitude
    # values are sensible etc
    expected_keys = {"site", "species", "inlet", "instrument", "sampling_period"}
    metadata_keys = set(metadata.keys())

    assert species.lower() == metadata["species"].lower()

    return _key_checker(expected=expected_keys, present=metadata_keys)


def attributes_checker_obssurface(attrs: Dict, species: str) -> bool:
    """Checks if the passed attributes dictionary contains the minimum expected keys

    Args:
        metadata: Dictionary of metadata
    Returns:
        bool: True if expected keys exist
    """
    expected_keys = {
        "data_owner",
        "data_owner_email",
        "inlet_height_magl",
        # "instrument",
        "Conditions of use",
        "Source",
        "Conventions",
        "Processed by",
        "species",
        "Calibration_scale",
        "station_longitude",
        "station_latitude",
        "station_long_name",
        # "sampling_period",
    }

    attribute_keys = set(attrs.keys())

    assert species.lower() == attrs["species"].lower()

    return _key_checker(expected=expected_keys, present=attribute_keys)


def _key_checker(expected: set, present: set):
    missing_keys = expected - present

    if missing_keys:
        print(f"Expected keys missing : {missing_keys}")
        return False
    else:
        return True
