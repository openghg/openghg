"""
Functions for formatting metadata.
"""
from typing import cast, Optional

from ._strings import clean_string
from ._inlet import format_inlet as _format_inlet


def format_site(site: str) -> str:
    """Originally from Footprints.read_file"""
    return clean_string(site)


def format_network(network: Optional[str]) -> Optional[str]:
    """Originally from Footprints.read_file"""
    return clean_string(network)


def format_domain(domain: str) -> str:
    """Format domain.

    - Originally from Footprints.read_file
    - Used in BoundaryConditions.read_file (same as original)
    """
    return clean_string(domain)


def consolidate_inlet_height(inlet: Optional[str], height: Optional[str]) -> str:
    """Originally from Footprints.read_file"""
    # Make sure `inlet` OR the alias `height` is included
    # Note: from this point only `inlet` variable should be used.
    if inlet is None and height is None:
        raise ValueError("One of inlet (or height) must be specified as an input")
    elif inlet is None:
        inlet = cast(str, height)

    return inlet


def format_inlet(inlet: str) -> str:
    """Originally from Footprints.read_file"""
    # Try to ensure inlet is 'NUM''UNIT' e.g. "10m"
    inlet = clean_string(inlet)
    inlet = _format_inlet(inlet)
    inlet = cast(str, inlet)
    return inlet


def format_species(species: Optional[str]) -> str:
    """Format species

    - Originally from Footprints.read_file
    - Used in BoundaryConditions.read_file (same as original)
    """
    # Ensure we have a value for species
    if species is None:
        species = "inert"
    else:
        species = clean_string(species)

    return species


def format_met_model(met_model: Optional[str]) -> str:
    """Originally from Footprints.read_file"""
    # Ensure we have a clear missing value for met_model
    if met_model is None:
        met_model = "NOT_SET"
    else:
        met_model = clean_string(met_model)

    return met_model


def format_bc_input(bc_input: str) -> str:
    """Originally from BoundaryConditions.read_file"""
    return clean_string(bc_input)
