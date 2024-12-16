import logging
from typing import Optional, Any

from openghg.util import load_json
from openghg.types import optionalPathType


__all__ = [
    "get_species_info",
    "synonyms",
    "species_lifetime",
    "check_lifetime_monthly",
    "check_species_lifetime",
    "check_species_time_resolved",
    "molar_mass",
]


logger = logging.getLogger("openghg.util.species")


def get_species_info(species_filepath: optionalPathType = None) -> dict[str, Any]:
    """Extract data from species info JSON file as a dictionary.

    This uses the data stored within openghg_defs/species_info JSON file by default.

    Args:
        species_filepath: Alternative species info file.
    Returns:
        dict: Data from species JSON file
    """
    from openghg_defs import species_info_file

    fpath = species_info_file if species_filepath is None else species_filepath

    return load_json(path=fpath)


def synonyms(
    species: str,
    lower: bool = True,
    allow_new_species: bool = True,
    species_filepath: optionalPathType = None,
) -> str:
    """Check to see if there are other names that we should be using for
    a particular input. E.g. If CFC-11 or CFC11 was input, go on to use cfc11.

    Args:
        species : Input string that you're trying to match
        lower : Return all lower case
        allow_new_species : Return original value (may be lower case)
            if this (or a synonym) is not found in the database.
            If False, raise a ValueError.
        species_filepath: Alternative species info file. Defaults to openghg_defs input.
    Returns:
        str: Matched species string

    TODO: Decide if we need to make this lower case or not.
    Included this here so this occurs in one place which can be linked to
    and changed if needed.
    """
    # If the species value is inert it should directly return rather than going through below logic
    if species.lower() == "inert":
        return species.lower()

    # Load in the species data
    species_data = get_species_info(species_filepath=species_filepath)

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


LifetimeType = Optional[str | list[str]]


def species_lifetime(species: str | None, species_filepath: optionalPathType = None) -> LifetimeType:
    """Find species lifetime.
    This can either be labelled as "lifetime" or "lifetime_monthly".

    Note: no species synonyms accepted yet

    Args:
        species : Species name e.g. "ch4" or "co2"
        species_filepath: Alternative species info file. Defaults to openghg_defs input.
    Returns:
        str / list / None : Extracted lifetime or None is no lifetime was present.
    """
    species_data = get_species_info(species_filepath=species_filepath)

    if species is not None:
        species_label = synonyms(species, lower=False, allow_new_species=False)
        species_data = species_data[species_label]
    else:
        return None

    lifetime_keywords = ["lifetime", "lifetime_monthly"]
    for key in lifetime_keywords:
        try:
            lifetime: list | None = species_data[key]
        except KeyError:
            continue
        else:
            break
    else:
        lifetime = None

    return lifetime


def check_lifetime_monthly(lifetime: LifetimeType) -> bool:
    """Check whether retrieved lifetime value represents monthly lifetimes.
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


def check_species_lifetime(species: str, short_lifetime: bool = False) -> bool:
    """
    Check whether a species has a [short] lifetime (relevant for footprint types).
    Args:
        species: Name of species
        short_lifetime: Flag for whether this species has a short lifetime
            (and so requires a specific footprint with this taken into account)
    Returns:
        bool : The short_lifetime flag
    """
    if species == "inert":
        if short_lifetime is True:
            raise ValueError(
                "When indicating footprint is for short lived species, 'species' input must be included"
            )
        short_lifetime = False
        lifetime = None
    else:
        lifetime = species_lifetime(species)
        if lifetime is not None:
            # TODO: May want to add a check on length of lifetime here
            short_lifetime = True
            logger.info("Updating short_lifetime to True since species has an associated lifetime")

    return short_lifetime


def check_species_time_resolved(species: str, time_resolved: bool = False) -> bool:
    """
    Check whether a species requires a time_resolved footprint.
    Note: at the moment this is only relevant for "co2".
    Args:
        species: Name of species
    Returns:
        bool: The time_resolved flag
    """
    species = synonyms(species, lower=True, allow_new_species=True)
    if species == "co2":
        if not time_resolved:
            time_resolved = True
            logger.info("Updating time_resolved to True for CO2 data")

    return time_resolved


def molar_mass(species: str, species_filepath: optionalPathType = None) -> float:
    """Extracts the molar mass of a species.

    Args:
        species : Species name
        species_filepath: Alternative species info file. Defaults to openghg_defs input.
    Returns:
        float : Molar mass of species
    """
    species_data = get_species_info(species_filepath=species_filepath)

    species_label = synonyms(species, lower=False, allow_new_species=False)
    molmass = float(species_data[species_label]["mol_mass"])

    return molmass
