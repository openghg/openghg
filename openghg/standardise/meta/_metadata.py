from typing import Dict, Tuple
from copy import deepcopy


def surface_standardise(metadata: Dict, attributes: Dict) -> Tuple[Dict, Dict]:
    """Makes sure the metadata and attributes dictionaries are the same
    and that any duplicated keys match.

    Args:
        metadata: Dictionary of metadata
        attributes: Attributes
    Returns:
        dict: Copy of metadata updated with attributes
    """
    meta_copy = deepcopy(metadata)
    attributes = deepcopy(attributes)

    # Check if we have differences
    for key, value in metadata.items():
        try:
            attr_value = attributes[key]

            if str(value).lower() != str(attr_value).lower():
                raise ValueError(f"Metadata mismatch, metadata: {value} - attributes: {attr_value}")
        except KeyError:
            pass

    keys_to_skip = ("Conditions of use", "File created", "Processed by")
    for key in keys_to_skip:
        attributes.pop(key, None)

    meta_copy.update(attributes)

    return meta_copy
