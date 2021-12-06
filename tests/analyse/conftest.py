import pytest
from openghg.objectstore import get_local_bucket
from openghg.store import ObsSurface, Emissions, Footprints
from helpers import get_datapath, get_emissions_datapath, get_footprint_datapath


@pytest.fixture(scope="module", autouse=True)
def data_read():
    get_local_bucket(empty=True)

    # Files for creating forward model (mf_mod) for methane at TAC site

    # Observation data
    #  - TAC at 100m for 201208
    site = "tac"
    network = "DECC"
    data_type = "CRDS"

    tac_path = get_datapath(filename="tac.picarro.1minute.100m.201208.dat", data_type="CRDS")
    ObsSurface.read_file(filepath=tac_path, data_type=data_type, site=site, network=network)

    # Emissions data
    # Anthropogenic ch4 (methane) data from 2012 for EUROPE
    species = "ch4"
    source = "anthro"
    domain = "EUROPE"

    emissions_datapath = get_emissions_datapath("ch4-anthro_EUROPE_2012.nc")

    Emissions.read_file(
        filepath=emissions_datapath,
        species=species,
        source=source,
        date="2012",
        domain=domain,
        high_time_resolution=False,
    )

    # Footprint data
    # TAC footprint from 2012-08 - 2012-09 at 100m
    height = "100m"
    model = "NAME"

    fp_datapath = get_footprint_datapath("TAC-100magl_EUROPE_201208.nc")

    Footprints.read_file(
        filepath=fp_datapath, site=site, model=model, network=network, height=height, domain=domain
    )
