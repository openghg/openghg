import pytest
from helpers import (
    get_bc_datapath,
    get_emissions_datapath,
    get_footprint_datapath,
    get_surface_datapath,
)
from openghg.standardise import standardise_surface, standardise_flux, standardise_bc, standardise_footprint
from helpers import clear_test_stores

@pytest.fixture(scope="module", autouse=True)
def data_read():
    """
    Data set up for running tests for these sets of modules.
    """
    clear_test_stores()

    # Files for creating forward model (mf_mod) for methane and carbon dioxide at TAC site

    # Observation data
    #  - TAC at 100m for 201208 and 201407
    #  - Includes CH4 and CO2 data
    site1 = "tac"
    network1 = "DECC"
    source_format1 = "CRDS"

    tac_path1 = get_surface_datapath(filename="tac.picarro.1minute.100m.201208.dat", source_format="CRDS")
    tac_path2 = get_surface_datapath(filename="tac.picarro.1minute.100m.201407.dat", source_format="CRDS")
    tac_filepaths = [tac_path1, tac_path2]

    # WAO data for radon from 2021-12-04 (data level 1 (NRT product) from ICOS)
    # - This has been standardised through openghg already from download.
    # - This data was then output to a netcdf file we can read.
    site2 = "wao"
    network2 = "ICOS"
    source_format2 = "OPENGHG"

    wao_path = get_surface_datapath(filename="wao_rn_icos_standardised_2021-12-04.nc", source_format="OPENGHG")

    standardise_surface(store="user", filepath=tac_filepaths, source_format=source_format1, site=site1, network=network1)
    standardise_surface(store="user",
                             filepath=wao_path,
                             source_format=source_format2,
                             site=site2,
                             network=network2,
                             inlet="10m")

    # Emissions data
    # Anthropogenic ch4 (methane) data from 2012 for EUROPE
    source1 = "anthro"
    domain = "EUROPE"
    emissions_datapath1 = get_emissions_datapath("ch4-anthro_EUROPE_2012.nc")
    standardise_flux(store="user",
                             filepath=emissions_datapath1,
                             species="ch4",
                             source=source1,
                             domain=domain,
                             high_time_resolution=False,
                             )

    # Waste data for CH4 (from UKGHG model)
    source2 = "waste"
    emissions_datapath2 = get_emissions_datapath("ch4-ukghg-waste_EUROPE_2012.nc")
    standardise_flux(store="user",
                             filepath=emissions_datapath2,
                             species="ch4",
                             source=source2,
                             domain=domain,
                             high_time_resolution=False,
                             )

        # Natural sources for CO2 (R-total from Cardamom)
        #  - 2 hourly (high resolution?)
    source3 = "natural-rtot"
    emissions_datapath3 = get_emissions_datapath("co2-rtot-cardamom-2hr_TEST_2014.nc")
    standardise_flux(store="user",
                             filepath=emissions_datapath3,
                             species="co2",
                             source=source3,
                             domain="TEST",
                             high_time_resolution=True,
                             )

    # Ocean flux for CO2
    #  - monthly (cut down data to 1 month)
    source4 = "ocean"
    emissions_datapath4a = get_emissions_datapath("co2-nemo-ocean-mth_TEST_2014.nc")
    emissions_datapath4b = get_emissions_datapath("co2-nemo-ocean-mth_TEST_2013.nc")
    standardise_flux(store="user",
                             filepath=emissions_datapath4a,
                             species="co2",
                             source=source4,
                             domain="TEST",
                             high_time_resolution=False,
                             )
    standardise_flux(store="user",
                             filepath=emissions_datapath4b,
                             species="co2",
                             source=source4,
                             domain="TEST",
                             high_time_resolution=False,
                             )

    # Boundary conditions data
    # CH4
    bc_filepath1 = get_bc_datapath("ch4_EUROPE_201208.nc")
    standardise_bc(store="user",
                             filepath=bc_filepath1,
                             species="ch4",
                             domain="EUROPE",
                             bc_input="MOZART",
                             period="monthly",
                             )

    # CO2
    bc_filepath1 = get_bc_datapath("co2_TEST_201407.nc")
    standardise_bc(store="user",
                             filepath=bc_filepath1,
                             species="co2",
                             domain="TEST",
                             bc_input="MOZART",
                             period="monthly",
                             )

    # Footprint data
    # TAC footprint from 2012-08 - 2012-09 at 100m
    height1 = "100m"
    model1 = "NAME"

    fp_datapath1 = get_footprint_datapath("TAC-100magl_EUROPE_201208.nc")
    standardise_footprint(store="user",
                             filepath=fp_datapath1, site=site1, model=model1, network=network1,
                             height=height1, domain=domain
                             )

    # TAC footprint from 2014-07 - 2014-09 at 100m for CO2 (high time resolution)
    fp_datapath2 = get_footprint_datapath("TAC-100magl_UKV_co2_TEST_201407.nc")
    standardise_footprint(store="user",
                             filepath=fp_datapath2, site=site1, model=model1, network=network1, metmodel="UKV",
                             height=height1, domain="TEST", species="co2"
                             )

    # WAO radon footprint from 2021-12-04
    # - cut down from full file to one day
    # - cut down to only include TEST domain rather than full EUROPE
    fp_height2 = "20m"
    model2 = "NAME"
    domain2 = "TEST"
    species2 = "rn"  # Species-specific footprint for short-lived species.

    fp_datapath2 = get_footprint_datapath("WAO-20magl_UKV_rn_TEST_202112.nc")
    standardise_footprint(store="user",
                             filepath=fp_datapath2, site=site2, model=model2, network=network2,
                             height=fp_height2, domain=domain2, species=species2,
                             )
