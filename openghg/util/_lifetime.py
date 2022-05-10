from typing import Optional, Union, List
from openghg.util._file import load_json


__all__ = ["species_lifetime", "check_lifetime_monthly"]


LifetimeType = Optional[Union[str, List[str]]]


# TODO: Incorporate species synonyms?


def species_lifetime(species: Union[str, None]) -> LifetimeType:
    """
    Find species lifetime from stored reference ("acrg_species_info.json").
    This can either be labelled as "lifetime" or "lifetime_monthly".

    Note: no species synonyms accepted yet

    Args:
        species : Species name e.g. "ch4" or "co2"
    
    Returns:
        str / list / None : Extracted lifetime or None is no lifetime was present.
    """
    species_info = load_json(filename="acrg_species_info.json")

    if species is not None:
        try:
            species_data = species_info[species]
        except KeyError:
                species_upper = species.upper()
                species_data = species_info[species_upper]
    else:
        return None

    lifetime_keywords = ["lifetime", "lifetime_monthly"]
    for key in lifetime_keywords:
        try:
            lifetime: Optional[list] = species_data[key]
        except KeyError:
            continue
        else:
            break
    else:
        lifetime = None

    return lifetime


def check_lifetime_monthly(lifetime: LifetimeType) -> bool:
    """
    Check whether retrieved lifetime value represents monthly lifetimes.
    This checks whether lifetime is a list and contains 12 values.

    Args:
        lifetime : str or list representation of lifetime value
    
    Returns:
        bool : True of lifetime matches criteria for monthly data, False otherwise

        Raises ValueError:
            if lifetime is a list but does not contain exactly 12 entries, one for each month
    """
    if isinstance(lifetime, list):
        if len(lifetime) == 12:
            return True
        else:
            raise ValueError(f"Invalid input for lifetime: {lifetime}")
    else:
        return False
