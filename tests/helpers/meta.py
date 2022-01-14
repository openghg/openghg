# Check if obssurface metadata contains the minimum expected
from typing import Dict
from openghg.dataobjects import SearchResults

def parsed_surface_metachecker(data: Dict) -> None:
    """Checks the metadata and attributes for data stored in a dictionary of the type
    returned from the standardisation functions such as parse_crds. This is a dictionary
    keyed by species name with metadata and data sub-keys.

    Args:
        data: Dictionary of gas data
    Returns:
        None
    """
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
    expected_keys = {
        "site",
        "species",
        "inlet",
        "instrument",
        "sampling_period",
        "calibration_scale",
        "station_longitude",
        "station_latitude",
    }

    metadata_keys = set(metadata.keys())

    meta_species = metadata["species"].lower()

    if species.lower() != meta_species:
        split_species = species.lower().split("_")[0]

        if split_species != meta_species:
            print(f"Error: species don't match {split_species} != {meta_species}")
            return False

    return _key_checker(expected=expected_keys, present=metadata_keys)


def attributes_checker_get_obs(attrs: Dict, species: str) -> bool:
    """Checks if the passed attributes dictionary contains the minimum expected keys for
     results retured from the get_obs_surface function

    Args:
        metadata: Dictionary of metadata
        speices: Expected species
    Returns:
        bool: True if expected keys exist
    """
    expected_keys = {
        "data_owner",
        "data_owner_email",
        "inlet_height_magl",
        "source",
        "species",
        "scale",
        "station_longitude",
        "station_latitude",
        "station_long_name",
    }

    return _attributes_checker(expected=expected_keys, attrs=attrs, species=species)


def attributes_checker_obssurface(attrs: Dict, species: str) -> bool:
    """Checks if the passed attributes dictionary contains the minimum expected keys

    Args:
        metadata: Dictionary of metadata
        species: Expected species
    Returns:
        bool: True if expected keys exist
    """
    expected_keys = {
        "data_owner",
        "data_owner_email",
        "inlet_height_magl",
        "conditions_of_use",
        "source",
        "Conventions",
        "processed_by",
        "species",
        "calibration_scale",
        "station_longitude",
        "station_latitude",
        "station_long_name",
    }

    return _attributes_checker(expected=expected_keys, attrs=attrs, species=species)


def _attributes_checker(expected: Dict, attrs: Dict, species: str) -> bool:
    attribute_keys = set(attrs.keys())

    meta_species = attrs["species"].lower()
    try:
        assert species.lower() == meta_species
    except AssertionError:
        # GCWERKS species_inlet maybe
        split_species = species.lower().split("_")[0]
        assert split_species == meta_species

    return _key_checker(expected=expected, present=attribute_keys)


def _key_checker(expected: set, present: set):
    missing_keys = expected - present

    if missing_keys:
        print(f"Expected keys missing : {missing_keys}")
        return False
    else:
        return True
