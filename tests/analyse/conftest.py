import os
import tempfile
import pytest
from helpers import get_datapath, get_emissions_datapath, get_footprint_datapath
from openghg.objectstore import get_local_bucket
from openghg.store import ObsSurface, Emissions, Footprints


@pytest.fixture(scope="module", autouse=True)
def data_read():
    """
    Data set up for running tests for these sets of modules.
    """
    get_local_bucket(empty=True)

    # Files for creating forward model (mf_mod) for methane and carbon dioxide at TAC site

    # Observation data
    #  - TAC at 100m for 201208 and 201407
    #  - Includes CH4 and CO2 data
    site = "tac"
    network = "DECC"
    data_type = "CRDS"

    tac_path1 = get_datapath(filename="tac.picarro.1minute.100m.201208.dat", data_type="CRDS")
    tac_path2 = get_datapath(filename="tac.picarro.1minute.100m.201407.dat", data_type="CRDS")
    tac_filepaths = [tac_path1, tac_path2]
    ObsSurface.read_file(filepath=tac_filepaths, data_type=data_type, site=site, network=network)

    # Emissions data
    # Anthropogenic ch4 (methane) data from 2012 for EUROPE
    source1 = "anthro"
    domain = "EUROPE"

    emissions_datapath1 = get_emissions_datapath("ch4-anthro_EUROPE_2012.nc")

    Emissions.read_file(
        filepath=emissions_datapath1,
        species="ch4",
        source=source1,
        date="2012",
        domain=domain,
        high_time_resolution=False,
    )

    # Waste data for CH4 (from UKGHG model)
    source2 = "waste"

    emissions_datapath2 = get_emissions_datapath("ch4-ukghg-waste_EUROPE_2012.nc")

    Emissions.read_file(
        filepath=emissions_datapath2,
        species="ch4",
        source=source2,
        date="2012",
        domain=domain,
        high_time_resolution=False,
    )

    # Natural sources for CO2 (R-total from Cardamom)
    #  - 2 hourly (high resolution?)
    source3 = "natural-rtot"

    emissions_datapath3 = get_emissions_datapath("co2-rtot-cardamom-2hr_TEST_2014.nc")

    Emissions.read_file(
        filepath=emissions_datapath3,
        species="co2",
        source=source3,
        date="2014",
        domain="TEST",
        high_time_resolution=True,
    )

    # Ocean flux for CO2
    #  - monthly
    source4 = "ocean"

    emissions_datapath4 = get_emissions_datapath("co2-nemo-ocean-mth_TEST_2014.nc")

    Emissions.read_file(
        filepath=emissions_datapath4,
        species="co2",
        source=source4,
        date="2014",
        domain="TEST",
        high_time_resolution=False,
    )

    # Footprint data
    # TAC footprint from 2012-08 - 2012-09 at 100m
    height = "100m"
    model = "NAME"

    fp_datapath1 = get_footprint_datapath("TAC-100magl_EUROPE_201208.nc")

    Footprints.read_file(
        filepath=fp_datapath1, site=site, model=model, network=network, 
        height=height, domain=domain
    )

    # TAC footprint from 2014-07 - 2014-09 at 100m for CO2 (high time resolution)
    fp_datapath2 = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")

    Footprints.read_file(
        filepath=fp_datapath2, site=site, model=model, network=network, metmodel="UKV",
        height=height, domain="TEST", species="co2"
    )
