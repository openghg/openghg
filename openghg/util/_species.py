from typing import Optional, Union, List
from openghg.util import load_json


__all__ = ["synonyms", "species_lifetime", "check_lifetime_monthly", "molar_mass"]


def synonyms(species: str, lower: bool = True, allow_new_species: bool = True) -> str:
    """
    Check to see if there are other names that we should be using for
    a particular input. E.g. If CFC-11 or CFC11 was input, go on to use cfc11,
    as this is used in species_info.json

    Args:
        species : Input string that you're trying to match
        lower : Return all lower case
        allow_new_species : Return original value (may be lower case)
            if this (or a synonym) is not found in the database.
            If False, raise a ValueError.
    Returns:
        str: Matched species string

    TODO: Decide if we need to make this lower case or not.
    Included this here so this occurs in one place which can be linked to
    and changed if needed.
    """
    # Load in the species data
    species_data = load_json(filename="acrg_species_info.json")

    # First test whether site matches keys (case insensitive)
    matched_strings = [k for k in species_data if k.upper() == species.upper()]

    # Used to access the alternative names in species_data
    alt_label = "alt"

    # If not found, search synonyms
    if not matched_strings:
        for key in species_data:
            # Iterate over the alternative labels and check for a match
            matched_strings = [s for s in species_data[key][alt_label] if s.upper() == species.upper()]

            if matched_strings:
                matched_strings = [key]
                break

    if matched_strings:
        updated_species = str(matched_strings[0])
        if lower:
            updated_species = updated_species.lower()
        return updated_species
    else:
        if not allow_new_species:
            raise ValueError(f"Unable to find species (or synonym) in database {species}")
        if lower:
            species = species.lower()
        return species


LifetimeType = Optional[Union[str, List[str]]]


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


def molar_mass(species: str) -> float:
    """
    This function extracts the molar mass of a species from the
    'acrg_species_info.json' data file.

    Args:
        species :

    Returns:
        float : Molar mass of species
    """
    species_info = load_json(filename="acrg_species_info.json")

    # TODO: Add when this functionality has made it into develop
    # species_label = synonyms(species, lower=False, allow_new_species=False)
    # molmass = float(species_info[species_label]['mol_mass'])
    species = species.upper()

    molmass = float(species_info[species]["mol_mass"])
    return molmass
