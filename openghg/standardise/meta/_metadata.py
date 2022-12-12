import math
from copy import deepcopy
from typing import Dict, List, Optional

from openghg.util import is_number


def metadata_default_keys() -> List:
    """
    Define default values expected within ObsSurface metadata
    """
    default_keys = [
        "site",
        "species",
        "inlet",
        "inlet_height_magl",
        "network",
        "instrument",
        "sampling_period",
        "calibration_scale",
        "data_owner",
        "data_owner_email",
        "station_longitude",
        "station_latitude",
        "station_long_name",
        "station_height_masl",
    ]

    return default_keys


def sync_surface_metadata(
    metadata: Dict,
    attributes: Dict,
    keys_to_add: Optional[List] = None,
) -> Dict:
    """Makes sure any duplicated keys between the metadata and attributes
    dictionaries match and that certain keys are present in the metadata.

    Args:
        metadata: Dictionary of metadata
        attributes: Attributes
        keys_to_add: Add these keys to the metadata, if not present, based on
        the attribute values. Note: this skips any keys which can't be
        copied from the attribute values.
    Returns:
        dict: Copy of metadata updated with attributes
    """
    meta_copy = deepcopy(metadata)

    # Check if we have differences
    for key, value in metadata.items():
        try:
            attr_value = attributes[key]

            # This should mainly be used for lat/long
            relative_tolerance = 1e-3

            if is_number(attr_value) and is_number(value):
                if not math.isclose(float(attr_value), float(value), rel_tol=relative_tolerance):
                    raise ValueError(
                        f"Value not within tolerance, metadata: {value} - attributes: {attr_value}"
                    )
            else:
                # Here we don't care about case. Within the Datasource we'll store the
                # metadata as all lowercase, within the attributes we'll keep the case.
                if str(value).lower() != str(attr_value).lower():
                    raise ValueError(
                        f"Metadata mismatch for '{key}', metadata: {value} - attributes: {attr_value}"
                    )
        except KeyError:
            # Key wasn't in attributes for comparison
            pass

    default_keys_to_add = metadata_default_keys()

    if keys_to_add is None:
        keys_to_add = default_keys_to_add

    # Check set of keys which should be in metadata and add if not present
    for key in keys_to_add:
        if key not in meta_copy.keys():
            try:
                meta_copy[key] = attributes[key]
            except KeyError:
                print(f"WARNING: {key} key not in attributes or metadata")

    return meta_copy
