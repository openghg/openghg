import logging
import math
from copy import deepcopy
from typing import Dict, List, Tuple, Optional
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
    update_mismatch: str = "never",
) -> Tuple[Dict, Dict]:
    """
    Makes sure any duplicated keys between the metadata and attributes
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
            - "from_source" / "attributes" - update mismatches based on input attributes
            - "from_definition" / "metadata" - update mismatches based on input metadata
    Returns:
        dict, dict: Aligned metadata, attributes
    """
    meta_copy = deepcopy(metadata)
    attrs_copy = deepcopy(attributes)

    mismatch_keys = {
        "never": ["never"],
        "attributes": ["attributes", "from_source"],
        "metadata": ["metadata", "from_definition"],
    }

    for key, options in mismatch_keys.items():
        if update_mismatch.lower() in options:
            update_mismatch = key.lower()
            break
    else:
        raise ValueError(f"Input for 'update_mismatch' should be one of {mismatch_keys}")

    attr_mismatches = {}

    # Check if we have differences
    for key, meta_value in metadata.items():
        try:
            attr_value = attributes[key]

            # This should mainly be used for lat/long
            relative_tolerance = 1e-3

            if is_number(attr_value) and is_number(meta_value):
                if not math.isclose(float(attr_value), float(meta_value), rel_tol=relative_tolerance):
                    err_warn_num = f"Value of {key} not within tolerance, metadata: {meta_value} - attributes: {attr_value}"
                    if update_mismatch == "never":
                        attr_mismatches[key] = (meta_value, attr_value)
                    elif update_mismatch == "attributes":
                        logger.warning(
                            f"{err_warn_num}\nUpdating metadata to use attribute value of {key} = {attr_value}"
                        )
                        meta_copy[key] = str(attr_value)
                    elif update_mismatch == "metadata":
                        logger.warning(
                            f"{err_warn_num}\nUpdating attributes to use metadata value of {key} = {meta_value}"
                        )
                        attrs_copy[key] = str(meta_value)
            else:
                # Here we don't care about case. Within the Datasource we'll store the
                # metadata as all lowercase, within the attributes we'll keep the case.                err_warn_str = f"Metadata mismatch for '{key}', metadata: {meta_value} - attributes: {attr_value}"
                err_warn_str = (
                    f"Metadata mismatch for '{key}', metadata: {meta_value} - attributes: {attr_value}"
                )
                if str(meta_value).lower() != str(attr_value).lower():
                    if update_mismatch == "never":
                        attr_mismatches[key] = (meta_value, attr_value)
                    elif update_mismatch == "attributes":
                        logger.warning(
                            f"{err_warn_str}\nUpdating metadata to use attribute value of {key} = {attr_value}"
                        )
                        meta_copy[key] = attr_value
                    elif update_mismatch == "metadata":
                        logger.warning(
                            f"{err_warn_str}\nUpdating attributes to use metadata value: {key} = {meta_value}"
                        )
                        attrs_copy[key] = meta_value
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
                logger.warning(f"{key} key not in attributes or metadata")
            else:
                if key in keys_as_floats:
                    meta_copy[key] = float(meta_copy[key])

    return meta_copy, attrs_copy
