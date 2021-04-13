"""
Functions to easily ensure variables at different stages of the 
"""

__all__ = ["compliant_string"]


def compliant_string(species_str: str) -> str:
    """Create an OpenGHG (and CF) compliant species label

    Args:
        species: Species name
    Returns:
        string: Compliant species string
    """
    from openghg.util import clean_string

    # For now we just use clean_string to keep only
    # alphanumeric characters
    return clean_string(to_clean=species_str)
