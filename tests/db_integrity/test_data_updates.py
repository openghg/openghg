import os
import tempfile
import pytest
import datetime
import pandas as pd
import numpy as np
from helpers import get_datapath, get_emissions_datapath, get_bc_datapath, get_footprint_datapath
from openghg.store import ObsSurface, Emissions, BoundaryConditions, Footprints
from openghg.retrieve import get_flux
from openghg.retrieve import search
from openghg.objectstore import get_bucket

def flux_data_read():
    """
    Flux data set up.
    """

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


def test_database_update_repeat():
    """
    Test object store can handle the same date (flux data) being added twice.
    """

    # Attempt to add same data to the database twice
    get_bucket(empty=True)
    flux_data_read()
    flux_data_read()

    em_param = {}
    em_param["start_date"] = "2012-01-01"
    em_param["end_date"] = "2013-01-01"

    em_param["species"] = "ch4"
    em_param["domain"] = "EUROPE"
    em_param["source"] = "anthro"

    flux = get_flux(**em_param)    

    assert flux is not None

    # bc_param = {}
    # bc_param["start_date"] = "2012-08-01"
    # bc_param["end_date"] = "2012-09-01"

    # bc_param["domain"] = "EUROPE"
    # bc_param["species"] = "ch4"
    # bc_param["bc_input"] = "MOZART"
    
    # bc = get_bc(**bc_param)

    # assert bc

    # fp_param = {}
    # fp_param["start_date"] = "2012-08-01"
    # fp_param["end_date"] = "2012-09-01"

    # fp_param["site"] = "TAC"
    # fp_param["height"] = "100m"
    # fp_param["domain"] = "EUROPE"
    # fp_param["model"] = "NAME"
    
    # footprint = get_footprint(**fp_param)

    # assert footprint is not None


def bsd_data_read():
    """
    Add Bilsdale data for CRDS and GCMD instrument to object store.
     - CRDS: ch4, co2, co
     - GCMD: sf6, n2o
    """

    site = "bsd"
    network = "DECC"
    data_type1 = "CRDS"

    bsd_path1 = get_datapath(filename="bsd.picarro.1minute.108m.min.dat", data_type="CRDS")
    
    ObsSurface.read_file(filepath=bsd_path1, data_type=data_type1, site=site, network=network)

    data_type2 = "GCWERKS"
    instrument = "GCMD"

    bsd_path2 = get_datapath(filename="bilsdale-md.14.C", data_type="GC")
    bsd_prec_path2 = get_datapath(filename="bilsdale-md.14.precisions.C", data_type="GC")
    
    ObsSurface.read_file(filepath=(bsd_path2, bsd_prec_path2),
                         data_type=data_type2,
                         site=site,
                         network=network,
                         instrument=instrument)


def bsd_small_edit_data_read():
    """
    Add overlapping Bilsdale data to the object store:
     - Same data
     - Small difference header details (should create different hash)
    """
    site = "bsd"
    network = "DECC"
    data_type2 = "GCWERKS"
    instrument = "GCMD"

    bsd_path3 = get_datapath(filename="bilsdale-md.small-edit.14.C", data_type="GC")
    bsd_prec_path3 = get_datapath(filename="bilsdale-md.14.precisions.C", data_type="GC")
    
    ObsSurface.read_file(filepath=(bsd_path3, bsd_prec_path3),
                         data_type=data_type2,
                         site=site,
                         network=network,
                         instrument=instrument)


def bsd_diff_data_read():
    """
    Add overlapping Bilsdale data to the object store:
     - Small different in data values (should create different hash)
    """
    site = "bsd"
    network = "DECC"
    data_type2 = "GCWERKS"
    instrument = "GCMD"

    bsd_path4 = get_datapath(filename="bilsdale-md.diff-value.14.C", data_type="GC")
    bsd_prec_path4 = get_datapath(filename="bilsdale-md.14.precisions.C", data_type="GC")
    
    ObsSurface.read_file(filepath=(bsd_path4, bsd_prec_path4),
                         data_type=data_type2,
                         site=site,
                         network=network,
                         instrument=instrument)


def read_crds_file_pd(filename, species_list=["ch4", "co2", "co"]):
    """
    Read CRDS data file using pandas (to create expected values).
    """
    data_path = get_datapath(filename=filename, data_type="CRDS")

    columns = ["date", "time", "type", "port"]
    for species in species_list:
        columns.append(species)
        columns.append(f"{species} stdev")
        columns.append(f"{species} N")

    file_data = pd.read_csv(data_path, names=columns, delim_whitespace=True, skiprows=3, dtype={"date":str, "time":str})
    file_data["date_time"] = file_data["date"] + file_data["time"]
    file_data["date_time"] = pd.to_datetime(file_data["date_time"], format="%y%m%d%H%M%S")

    file_data = file_data.dropna()

    return file_data


def read_gc_file_pd(filename):
    """
    Read GC data file using pandas (to create expected values).
    """
    data_path = get_datapath(filename=filename, data_type="GC")
    gcwerks_file_data = pd.read_csv(data_path, delim_whitespace=True, skipinitialspace=True,
                                    skiprows=4, dtype={"yyyy": str, "mm": str, "dd": str, "hh": str, "mi": str})

    gcwerks_file_data["date_time"] = gcwerks_file_data["yyyy"] \
                                   + gcwerks_file_data["mm"] \
                                   + gcwerks_file_data["dd"] \
                                   + gcwerks_file_data["hh"] \
                                   + gcwerks_file_data["mi"]
    
    gcwerks_file_data["date_time"] = pd.to_datetime(gcwerks_file_data["date_time"], format="%Y%m%d%H%M")
    gcwerks_file_data = gcwerks_file_data.dropna(subset="SF6")

    return gcwerks_file_data


def test_obs_data_read_small_diff():
    """
    Test adding new file for GC data (same data as original file but different header)
    """
    get_bucket(empty=True)
    # Load BSD data
    bsd_data_read()
    # Load BSD data (GCWERKS) with small edit in header
    bsd_small_edit_data_read()

    # Search for expected species
    # CRDS data
    search_ch4 = search(site="bsd", species="ch4")
    search_co2 = search(site="bsd", species="co2")
    search_co = search(site="bsd", species="co")
    # GCMD data
    search_sf6 = search(site="bsd", species="sf6")
    search_n2o = search(site="bsd", species="n2o")

    # Check something is found for each search
    assert bool(search_ch4) == True
    assert bool(search_co2) == True
    assert bool(search_co) == True
    assert bool(search_sf6) == True
    assert bool(search_n2o) == True

    crds_file_data = read_crds_file_pd(filename="bsd.picarro.1minute.108m.min.dat")

    obs_data_ch4 = search_ch4.retrieve()
    data_ch4 = obs_data_ch4.data

    ch4 = data_ch4["ch4"].values
    expected_ch4 = crds_file_data["ch4"].values
    np.testing.assert_allclose(ch4, expected_ch4)

    # ch4_time = data_ch4["time"].values
    # expected_ch4_time = crds_file_data["date_time"].values
    # np.testing.assert_allclose(ch4_time, expected_ch4_time)

    gcwerks_file_data = read_gc_file_pd("bilsdale-md.14.C")

    obs_data_sf6 = search_sf6.retrieve()
    data_sf6 = obs_data_sf6.data

    sf6 = data_sf6["sf6"].values
    expected_sf6 = gcwerks_file_data["SF6"].values
    np.testing.assert_allclose(sf6, expected_sf6)

    # Load datasource and keys, key dictionary includes "v1", "latest" etc.

    # TODO: Can we check if this has been saved as a new version?   


def test_obs_data_read_data_diff():
    """
    Test adding new file for GC with same time points but some different data values
    """
    get_bucket(empty=True)  
    # Load BSD data
    bsd_data_read()
    # Load BSD data (GCWERKS) with edit to data (will produce different hash)
    bsd_diff_data_read()

    # Search for expected species
    # CRDS data
    search_ch4 = search(site="bsd", species="ch4")
    search_co2 = search(site="bsd", species="co2")
    search_co = search(site="bsd", species="co")
    # GCMD data
    search_sf6 = search(site="bsd", species="sf6")
    search_n2o = search(site="bsd", species="n2o")

    # Check something is found for each search
    assert bool(search_ch4) == True
    assert bool(search_co2) == True
    assert bool(search_co) == True
    assert bool(search_sf6) == True
    assert bool(search_n2o) == True

    crds_file_data = read_crds_file_pd(filename="bsd.picarro.1minute.108m.min.dat")

    obs_data_ch4 = search_ch4.retrieve()
    data_ch4 = obs_data_ch4.data

    ch4 = data_ch4["ch4"].values
    expected_ch4 = crds_file_data["ch4"].values
    np.testing.assert_allclose(ch4, expected_ch4)

    # ch4_time = data_ch4["time"].values
    # expected_ch4_time = crds_file_data["date_time"].values
    # np.testing.assert_allclose(ch4_time, expected_ch4_time)

    gcwerks_file_data = read_gc_file_pd("bilsdale-md.small-edit.14.C")

    obs_data_sf6 = search_sf6.retrieve()
    data_sf6 = obs_data_sf6.data

    sf6 = data_sf6["sf6"].values
    expected_sf6 = gcwerks_file_data["SF6"].values
    np.testing.assert_allclose(sf6, expected_sf6)

    # TODO: Can we check if this has been saved as a new version?





    
