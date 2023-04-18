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
    update_mismatch: str = "never",
) -> Dict:
    """Makes sure any duplicated keys between the metadata and attributes
    dictionaries match and that certain keys are present in the metadata.

    Args:
        metadata: Dictionary of metadata
        attributes: Attributes
        keys_to_add: Add these keys to the metadata, if not present, based on
        the attribute values. Note: this skips any keys which can't be
        copied from the attribute values.
        update_mismatch: If case insensitive mismatch is found between an
            attribute and a metadata value, this determines the function behaviour.
            This includes the options:
             - "never" - don't update mismatches and raise an AttrMismatchError
             - "attributes" - update mismatches based on input attributes
             - "metadata" - update mismatches based on input metadata
    Returns:
        dict: Copy of metadata updated with attributes
    """
    meta_copy = deepcopy(metadata)

    mismatch_keys = ["never", "attributes", "metadata"]
    if update_mismatch.lower() not in mismatch_keys:
        raise ValueError(f"Input for 'update_mismatch' should be one of {mismatch_keys}")
    else:
        update_mismatch = update_mismatch.lower()

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
                        raise AttrMismatchError(err_warn_str)
                    else:
                        logger.warning(
                            f"{err_warn_str}\nUpdating metadata to use attribute value of {key} = {attr_value}"
                        )

                    meta_copy[key] = str(attr_value)
            else:
                # Here we don't care about case. Within the Datasource we'll store the
                # metadata as all lowercase, within the attributes we'll keep the case.
                if str(value).lower() != str(attr_value).lower():
                    if update_mismatch == "never":
                        raise AttrMismatchError(
                            f"Metadata mismatch for '{key}', metadata: {value} - attributes: {attr_value}"
                        )
                    elif update_mismatch == "attributes":
                        logger.warning(
                            f"Metadata mismatch for '{key}', metadata: {value} - attributes: {attr_value}\n"
                            f"Updating metadata to use attribute value of {key} = {attr_value}"
                        )
                        meta_copy[key] = attr_value
                    elif update_mismatch == "metadata":
                        logger.warning(
                            f"Metadata mismatch for '{key}', metadata: {value} - attributes: {attr_value}\n"
                            f"Using supplied metadata value: {key} = {value}"
                        )
                        meta_copy[key] = attr_value                        
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
                logger.warning(f"{key} key not in attributes or metadata")

    return meta_copy
