import logging
import math
from copy import deepcopy
from typing import Dict, List, Optional
from openghg.types import AttrMismatchError
from openghg.util import is_number

logger = logging.getLogger("openghg.standardise.metadata")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def metadata_default_keys() -> List:
    """
    Defines default values expected within ObsSurface metadata.
    Returns:
        list: keys required in metadata
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


def metadata_keys_as_floats() -> List:
    """
    Defines which keys should be consistently stored as numbers in the metadata
    (even if they are not numbers within the attributes).
    Returns:
        list: keys required to be floats in metadata
    """

    values_as_floats = [
        # "inlet_height_magl",
        "station_longitude",
        "station_latitude",
        "station_height_masl",
    ]

    return values_as_floats


def sync_surface_metadata(
    metadata: Dict,
    attributes: Dict,
    keys_to_add: Optional[List] = None,
    update_mismatch: bool = False,
) -> Dict:
    """
    Makes sure any duplicated keys between the metadata and attributes
    dictionaries match and that certain keys are present in the metadata.

    Args:
        metadata: Dictionary of metadata
        attributes: Attributes
        keys_to_add: Add these keys to the metadata, if not present, based on
        the attribute values. Note: this skips any keys which can't be
        copied from the attribute values.
        update_mismatch: If True, if case insensitive mismatch is found between
            an attribute and a metadata value, update the metadata to contain
            the attribute value.
            If False (and by default) this will raise an AttrMismatchError.
    Returns:
        dict: Copy of metadata updated with attributes
    """
    from rich.progress import Progress

    progress = Progress()
    meta_copy = deepcopy(metadata)

    attr_mismatches = {}

    # Check if we have differences
    for key, value in metadata.items():
        try:
            attr_value = attributes[key]

            # This should mainly be used for lat/long
            relative_tolerance = 1e-3

            if is_number(attr_value) and is_number(value):
                if not math.isclose(float(attr_value), float(value), rel_tol=relative_tolerance):
                    err_warn_str = (
                        f"Value of {key} not within tolerance, metadata: {value} - attributes: {attr_value}"
                    )
                    if not update_mismatch:
                        attr_mismatches[key] = (value, attr_value)
                    else:
                        logger.warning(
                            f"{err_warn_str}\nUpdating metadata to use attribute value of {key} = {attr_value}"
                        )

                    meta_copy[key] = str(attr_value)
            else:
                # Here we don't care about case. Within the Datasource we'll store the
                # metadata as all lowercase, within the attributes we'll keep the case.
                if str(value).lower() != str(attr_value).lower():
                    if not update_mismatch:
                        attr_mismatches[key] = (value, attr_value)
                    else:
                        logger.warning(
                            f"Metadata mismatch for '{key}', metadata: {value} - attributes: {attr_value}\n"
                            f"Updating metadata to use attribute value of {key} = {attr_value}"
                        )
                        meta_copy[key] = attr_value
        except KeyError:
            # Key wasn't in attributes for comparison
            pass

    if attr_mismatches:
        mismatch_details = [
            f" - '{key}', metadata: {values[0]}, attributes: {values[1]}"
            for key, values in attr_mismatches.items()
        ]
        mismatch_str = "\n".join(mismatch_details)
        raise AttrMismatchError(
            f"Metadata mismatch / value not within tolerance for the following keys:\n{mismatch_str}"
        )

    default_keys_to_add = metadata_default_keys()
    keys_as_floats = metadata_keys_as_floats()

    if keys_to_add is None:
        keys_to_add = default_keys_to_add

    # Check set of keys which should be in metadata and add if not present
    for key in keys_to_add:
        if key not in meta_copy.keys():
            try:
                meta_copy[key] = attributes[key]
            except KeyError:
                progress.log(Warning(f"{key} key not in attributes or metadata"))
            else:
                if key in keys_as_floats:
                    meta_copy[key] = float(meta_copy[key])

    return meta_copy
