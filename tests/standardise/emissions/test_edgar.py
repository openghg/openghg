from openghg.standardise.emissions import parse_edgar
from pathlib import Path


def test_parse_edgar():

    filepath = Path("/group/chemistry/acrg/Gridded_fluxes/CH4/EDGAR_v5.0/yearly")

    species = "ch4"
    year = "2015"

    data = parse_edgar(filepath, species, year)

    print(data)
    # TODO: Add checks here for metadata, attributes and data
