import logging
# import os
# from pathlib import Path
import pytest

from openghg.standardise.surface import parse_crds
from openghg.objectstore import get_local_bucket
from openghg.standardise.meta import assign_attributes, get_attributes
# from helpers import get_datapath, metadata_checker_obssurface, attributes_checker_obssurface
from helpers import get_datapath

# import tempfile
# from cfchecker import CFChecker

# flake8: noqa

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def make_dummy_dataset(species_name: str):
    """Make simple Dataset with correct variable name for the species"""
    import xarray as xr
    import numpy as np

    ds = xr.Dataset({species_name: ("time", np.arange(0, 2, 1))},
                    coords={"time": np.array(["2020-01-01", "2020-02-01"], dtype="datetime64[ns]")})

    return ds

@pytest.mark.parametrize("species,name,long_name,units",
    [("carbon dioxide", "co2", "mole_fraction_of_carbon_dioxide_in_air", "1e-6"),
     ("CFC-11", "cfc11", "mole_fraction_of_cfc11_in_air", "1e-12"),
     ("Rn", "rn", "radioactivity_concentration_of_222Rn_in_air", "mBq m$^{-3}$"),
     ("c2f6", "c2f6", "mole_fraction_of_hexafluoroethane_in_air", "1e-12"),
     ("SF5CF3", "sf5cf3", "sf5cf3", "unknown"),
     ("CFC-323", "cfc323", "cfc323", "unknown")
    ]
)
def test_species_attributes(species, name, long_name, units):
    """Test correct species attributes are created for various synonyms
    Cases covered include:
     - "carbon dioxide" - space in input name + check name/units correct
     - "CFC-11" - dash, '-', in input name (should be able to find synonym)
     - "Rn" - not all upper or lower case + check name/units correct
     - "c2f6" - note in code that long name was not being extracted, added check
     - "SF5CF3" - unknown but valid species name
     - "CFC-323" - unknown CFC, '-' in name, should be replaced
    """

    site = "mhd"
    ds = make_dummy_dataset(species)
    ds_updated = get_attributes(ds, species, site)

    assert name in ds_updated

    species_attrs = ds_updated[name].attrs
    assert species_attrs["long_name"] == long_name
    assert species_attrs["units"] == units

    assert ds_updated.attrs["species"] == name


def test_species_attributes_isotopologue():
    """
    Test isotopologue "CH4C13" seperately as this should include
    an extra attribute for "units_description" in addition to
    other details.
    """

    species = "CH4C13"
    site = "mhd"
    ds = make_dummy_dataset(species)
    ds_updated = get_attributes(ds, species, site)

    name = "dch4c13"
    long_name = "delta_ch4_c13"
    units = "1"
    units_non_standard = "per mil"

    assert name in ds_updated

    species_attrs = ds_updated[name].attrs
    assert species_attrs["long_name"] == long_name
    assert species_attrs["units"] == units
    assert species_attrs["units_description"] == units_non_standard

    assert ds_updated.attrs["species"] == name
