from typing import Optional, Union
from openghg.util._file import load_json


__all__ = ["species_lifetime", "check_lifetime_monthly"]


LifetimeType = Optional[Union[float, int, list]]


# TODO: Incorporate species synonyms?


def species_lifetime(species: Union[str, None]) -> LifetimeType:
    """
    """
    species_info = load_json(filename="acrg_species_info.json")

    try:
        species_data = species_info[species]
    except KeyError:
        species_upper = species.upper()
        species_data = species_info[species_upper]

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
    """
    if isinstance(lifetime, list):
        if len(lifetime) == 12:
            return True
        else:
            raise ValueError(f"Invalid input for lifetime: {lifetime}")
    else:
        return False
