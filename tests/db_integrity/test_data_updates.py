import pytest
import pandas as pd
import numpy as np
from helpers import get_surface_datapath, get_emissions_datapath, get_footprint_datapath
from openghg.store import ObsSurface, Emissions
from openghg.store.base import Datasource
from openghg.store._connection import get_object_store_connection
from openghg.retrieve import get_flux
from openghg.retrieve import search
from openghg.standardise import standardise_footprint
from openghg.objectstore import get_bucket


from helpers import clear_test_stores


def flux_data_read():
    """
    Flux data set up.
    """
    # Emissions data
    # Anthropogenic ch4 (methane) data from 2012 for EUROPE
    source1 = "anthro"
    domain = "EUROPE"

    emissions_datapath1 = get_emissions_datapath("ch4-anthro_EUROPE_2012.nc")

    bucket = get_bucket()
    with Emissions(bucket=bucket) as ems:
        ems.read_file(
            filepath=emissions_datapath1,
            species="ch4",
            source=source1,
            domain=domain,
            high_time_resolution=False,
        )


def test_database_update_repeat():
    """
    Test object store can handle the same date (flux data) being added twice.
    """
    # Attempt to add same data to the database twice
    clear_test_stores()
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


#  Test variants in data from the same source being added


def bsd_data_read_crds():
    """
    Add Bilsdale *minutely* data for CRDS instrument to object store.
     - CRDS: ch4, co2, co
    """

    site = "bsd"
    network = "DECC"
    source_format1 = "CRDS"

    bsd_path1 = get_surface_datapath(filename="bsd.picarro.1minute.108m.min.dat", source_format="CRDS")

    bucket = get_bucket()
    with ObsSurface(bucket=bucket) as obs:
        obs.read_file(filepath=bsd_path1, source_format=source_format1, site=site, network=network)


def bsd_data_read_gcmd():
    """
    Add Bilsdale data GCMD instrument to object store.
     - GCMD: sf6, n2o
    """

    site = "bsd"
    network = "DECC"
    source_format2 = "GCWERKS"
    instrument = "GCMD"

    bsd_path2 = get_surface_datapath(filename="bilsdale-md.14.C", source_format="GC")
    bsd_prec_path2 = get_surface_datapath(filename="bilsdale-md.14.precisions.C", source_format="GC")

    bucket = get_bucket()
    with ObsSurface(bucket=bucket) as obs:
        obs.read_file(
            filepath=(bsd_path2, bsd_prec_path2),
            source_format=source_format2,
            site=site,
            network=network,
            instrument=instrument,
        )


def bsd_small_edit_data_read():
    """
    Add overlapping Bilsdale GCMD data to the object store:
     - Same data
     - Small difference header details (should create different hash)
    """
    site = "bsd"
    network = "DECC"
    source_format2 = "GCWERKS"
    instrument = "GCMD"

    bsd_path3 = get_surface_datapath(filename="bilsdale-md.small-edit.14.C", source_format="GC")
    bsd_prec_path3 = get_surface_datapath(filename="bilsdale-md.14.precisions.C", source_format="GC")

    bucket = get_bucket()
    with ObsSurface(bucket=bucket) as obs:
        obs.read_file(
            filepath=(bsd_path3, bsd_prec_path3),
            source_format=source_format2,
            site=site,
            network=network,
            instrument=instrument,
        )


def bsd_diff_data_read(overwrite=False):
    """
    Add overlapping Bilsdale GCMD data to the object store:
     - Small difference in data values (should create different hash)
    """
    site = "bsd"
    network = "DECC"
    source_format2 = "GCWERKS"
    instrument = "GCMD"

    bsd_path4 = get_surface_datapath(filename="bilsdale-md.diff-value.14.C", source_format="GC")
    bsd_prec_path4 = get_surface_datapath(filename="bilsdale-md.14.precisions.C", source_format="GC")

    bucket = get_bucket()
    with ObsSurface(bucket=bucket) as obs:
        obs.read_file(
            filepath=(bsd_path4, bsd_prec_path4),
            source_format=source_format2,
            site=site,
            network=network,
            instrument=instrument,
            overwrite=overwrite,
        )


def bsd_diff_date_range_read(overwrite=False):
    """
    Add overlapping Bilsdale GCMD data to the object store:
     - Small difference in data date range (should create different hash)
    """
    site = "bsd"
    network = "DECC"
    source_format2 = "GCWERKS"
    instrument = "GCMD"

    bsd_path5 = get_surface_datapath(filename="bilsdale-md.diff-date-range.14.C", source_format="GC")
    bsd_prec_path5 = get_surface_datapath(filename="bilsdale-md.14.precisions.C", source_format="GC")

    bucket = get_bucket()
    with ObsSurface(bucket=bucket) as obs:
        obs.read_file(
            filepath=(bsd_path5, bsd_prec_path5),
            source_format=source_format2,
            site=site,
            network=network,
            instrument=instrument,
            overwrite=overwrite,
        )


def read_crds_file_pd(filename, species_list=["ch4", "co2", "co"]):
    """
    Read CRDS data file using pandas (to create expected values).
    """
    data_path = get_surface_datapath(filename=filename, source_format="CRDS")

    columns = ["date", "time", "type", "port"]
    for species in species_list:
        columns.append(species)
        columns.append(f"{species} stdev")
        columns.append(f"{species} N")

    file_data = pd.read_csv(
        data_path, names=columns, delim_whitespace=True, skiprows=3, dtype={"date": str, "time": str}
    )
    file_data["date_time"] = file_data["date"] + file_data["time"]
    file_data["date_time"] = pd.to_datetime(file_data["date_time"], format="%y%m%d%H%M%S")

    file_data = file_data.dropna()

    return file_data


def read_gcmd_file_pd(filename):
    """
    Read GCMD data file using pandas (to create expected values).
    """
    data_path = get_surface_datapath(filename=filename, source_format="GC")
    gcwerks_file_data = pd.read_csv(
        data_path,
        delim_whitespace=True,
        skipinitialspace=True,
        skiprows=4,
        dtype={"yyyy": str, "mm": str, "dd": str, "hh": str, "mi": str},
    )

    gcwerks_file_data["date_time"] = (
        gcwerks_file_data["yyyy"]
        + gcwerks_file_data["mm"]
        + gcwerks_file_data["dd"]
        + gcwerks_file_data["hh"]
        + gcwerks_file_data["mi"]
    )

    gcwerks_file_data["date_time"] = pd.to_datetime(gcwerks_file_data["date_time"], format="%Y%m%d%H%M")
    gcwerks_file_data = gcwerks_file_data.dropna(subset="SF6")

    return gcwerks_file_data


def test_obs_data_read_header_diff():
    """
    Test adding new file for GC data (same data as original file but different header).
    Steps:
     - BSD CRDS minutely data added
     - BSD GCMD data added
     - BSD GCMD different data added - header changed so hash will be different but data will be the same
    Expect that GCMD (and CRDS) data can still be accessed.
    """
    clear_test_stores()
    # Load BSD data - CRDS data
    bsd_data_read_crds()
    # Load BSD data - GCMD data (GCWERKS)
    bsd_data_read_gcmd()
    # Load BSD data - GCMD data (GCWERKS) with small edit in header
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

    gcwerks_file_data = read_gcmd_file_pd("bilsdale-md.14.C")

    obs_data_sf6 = search_sf6.retrieve()
    data_sf6 = obs_data_sf6.data

    sf6 = data_sf6["sf6"].values
    expected_sf6 = gcwerks_file_data["SF6"].values
    np.testing.assert_allclose(sf6, expected_sf6)

    # Load datasource and keys, key dictionary includes "v1", "latest" etc.

    # TODO: Can we check if this has been saved as a new version?


@pytest.mark.xfail(
    reason="Related to Issue #591.\n"
    " This test is to check updated data values will be stored within the object store for a current data set.\n"
    " Currently doesn't seem to be adding the new data and retains the original data.\n",
    raises=AssertionError,
    strict=True,
)
def test_obs_data_read_data_diff():
    """
    Test adding new file for GC with same time points but some different data values.
    Steps:
     - BSD CRDS minutely data added
     - BSD GCMD data added
     - BSD GCMD different data added - data has been changed
    Expect that different GCMD will be retrieved from search (as latest version).
    Expect CRDS data can still be accessed.
    """
    clear_test_stores()
    # Load BSD data - CRDS
    bsd_data_read_crds()
    # Load BSD data - GCMD data (GCWERKS)
    bsd_data_read_gcmd()
    # Load BSD data - GCMD data (GCWERKS) with edit to data values (will produce different hash)
    bsd_diff_data_read(overwrite=True)

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

    gcwerks_file_data = read_gcmd_file_pd("bilsdale-md.diff-value.14.C")

    obs_data_sf6 = search_sf6.retrieve()
    data_sf6 = obs_data_sf6.data

    sf6 = data_sf6["sf6"].values
    expected_sf6 = gcwerks_file_data["SF6"].values
    np.testing.assert_allclose(sf6, expected_sf6)

    # TODO: Can we check if this has been saved as a new version?


# TODO: Add test for different time values as well.

#  Look at different data frequencies for the same data


def bsd_data_read_crds_diff_frequency():
    """
    Add Bilsdale *hourly* data for CRDS instrument to object store
     - CRDS: ch4, co2, co
    """

    site = "bsd"
    network = "DECC"
    source_format1 = "CRDS"

    bsd_path_hourly = get_surface_datapath(filename="bsd.picarro.hourly.108m.min.dat", source_format="CRDS")

    bucket = get_bucket()
    with ObsSurface(bucket=bucket) as obs:
        obs.read_file(filepath=bsd_path_hourly, source_format=source_format1, site=site, network=network)


def test_obs_data_read_two_frequencies():
    """
    Test database when two different frequencies for the same site are added.
    Steps:
     - BSD CRDS minutely data added
     - BSD CRDS hourly data added
     - BSD GCMD data added
    Expect both minutely and hourly CRDS data can be accessed (searching by sampling_period).
    Expect hourly data to be found as "latest" version to be retrieved (is this what we want?).
    Expect GCMD data to still be available.
    """
    clear_test_stores()
    # Load BSD data - CRDS minutely frequency (and GCWERKS data)
    bsd_data_read_crds()
    # Load BSD data - CRDS hourly frequency
    bsd_data_read_crds_diff_frequency()
    # Load BSD data - GCMD data (GCWERKS)
    bsd_data_read_gcmd()

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

    # Extract data from original files
    crds_file_data_hourly = read_crds_file_pd(filename="bsd.picarro.hourly.108m.min.dat")
    crds_file_data_minutely = read_crds_file_pd(filename="bsd.picarro.1minute.108m.min.dat")

    # Compare ch4 data stored to data from file
    data_ch4_all = search_ch4.retrieve()
    assert len(data_ch4_all) == 2
    # ch4 = data_ch4["ch4"].values
    # expected_ch4 = crds_file_data_hourly["ch4"].values
    # np.testing.assert_allclose(ch4, expected_ch4)

    # Check both minutely and hourly are still stored and correct
    # TODO: Make the get_* lines work (e.g. add kwargs to all get_* functions)
    # data_co_hourly = get_obs_surface(site="bsd", species="ch4", sampling_period="3600.0").data
    data_co_hourly = search_co.retrieve(sampling_period="3600.0").data
    co_hourly = data_co_hourly["co"].values
    expected_co_hourly = crds_file_data_hourly["co"].values
    np.testing.assert_allclose(co_hourly, expected_co_hourly)

    # data_co_minutely = get_obs_surface(site="bsd", species="ch4", sampling_period="60.0").data
    data_co_hourly = search_co.retrieve(sampling_period="60.0").data
    co_minutely = data_co_hourly["co"].values
    expected_co_minutely = crds_file_data_minutely["co"].values
    np.testing.assert_allclose(co_minutely, expected_co_minutely)

    # Check SF6 data can still be accessed
    gcwerks_file_data = read_gcmd_file_pd("bilsdale-md.14.C")

    obs_data_sf6 = search_sf6.retrieve()
    data_sf6 = obs_data_sf6.data

    sf6 = data_sf6["sf6"].values
    expected_sf6 = gcwerks_file_data["SF6"].values
    np.testing.assert_allclose(sf6, expected_sf6)

    # TODO: Can we check if this has been saved as a new version?


#  Look at replacing data with different / overlapping internal time stamps


def bsd_data_read_crds_internal_overlap(overwrite=False):
    """
    Add Bilsdale *hourly* data for CRDS instrument to object store
     - CRDS: ch4, co2, co
    """

    site = "bsd"
    network = "DECC"
    source_format1 = "CRDS"

    bsd_path_hourly = get_surface_datapath(
        filename="bsd.picarro.hourly.108m.overlap-dates.dat", source_format="CRDS"
    )

    bucket = get_bucket()
    with ObsSurface(bucket=bucket) as obs:
        obs.read_file(
            filepath=bsd_path_hourly,
            source_format=source_format1,
            site=site,
            network=network,
            overwrite=overwrite,
        )


def test_obs_data_representative_date_overlap():
    """
    Added test based on fix for Issue 506.

    Due to sampling period being used to create representative date string
    when storing data. If the end of one chunk overlapped with the start of the
    next chunk this created overlapping date ranges.

    This test checks this will no longer raise a KeyError based on this.
    """

    clear_test_stores()
    bsd_data_read_crds_internal_overlap()
    bsd_data_read_crds_internal_overlap(overwrite=True)

    bucket = get_bucket()
    with get_object_store_connection(data_type="surface", bucket=bucket) as obs:
        uuids = obs._datasources()

    datasources = []
    for uuid in uuids:
        datasource = Datasource.load(bucket=bucket, uuid=uuid)
        datasources.append(datasource)

    data = [datasource.data() for datasource in datasources]
    one_species_data = data[0]
    keys = list(one_species_data.keys())
    keys.sort()

    time_range_key1 = keys[0]
    time_range_key2 = keys[1]

    start1, end1 = time_range_key1.split("_")
    start2, end2 = time_range_key2.split("_")

    time_buffer = pd.Timedelta(seconds=1)

    expected_start1 = pd.Timestamp("2014-01-30T11:20:45", tz="utc")
    expected_start2 = pd.Timestamp("2015-01-01T00:01:00", tz="utc")
    expected_end1 = expected_start2 - time_buffer

    assert pd.Timestamp(start1) == expected_start1
    assert pd.Timestamp(end1) == expected_end1
    assert pd.Timestamp(start2) == expected_start2


# Check appropriate metadata is updated when data is added to data sources


def test_metadata_update():
    """
    Add data and then update this to check that the version is both added to the original
    metadata and subsequently updated when the datasource is updated.
    """
    clear_test_stores()
    # Load BSD data - GCMD data (GCWERKS)
    bsd_data_read_gcmd()

    # Search for expected species
    # GCMD data
    search_sf6_1 = search(site="bsd", species="sf6")

    # Set expectations for start and end date (for GC data this is altered from file details
    # based on known sampling period).
    sampling_period = 75
    sampling_period_td = pd.Timedelta(seconds=int(sampling_period))
    time_buffer = pd.Timedelta(seconds=1)  # Buffer subtracted from end to make this exclusive end.
    expected_start_1 = str(pd.Timestamp("2014-01-01T00:13", tz="utc") - sampling_period_td / 2.0)
    expected_end_1 = str(
        pd.Timestamp("2014-12-03T02:18", tz="utc")
        - sampling_period_td / 2.0
        + sampling_period_td
        - time_buffer
    )

    sf6_metadata_1 = search_sf6_1.retrieve().metadata
    assert sf6_metadata_1["latest_version"] == "v1"
    assert sf6_metadata_1["start_date"] == expected_start_1
    assert sf6_metadata_1["end_date"] == expected_end_1

    # Load BSD data - GCMD data (GCWERKS) with small change in date range
    bsd_diff_date_range_read(overwrite=True)

    search_sf6_2 = search(site="bsd", species="sf6")

    expected_start_2 = expected_start_1
    expected_end_2 = str(
        pd.Timestamp("2014-12-06T10:48", tz="utc")
        - sampling_period_td / 2.0
        + sampling_period_td
        - time_buffer
    )

    sf6_metadata_2 = search_sf6_2.retrieve().metadata
    assert sf6_metadata_2["latest_version"] == "v2"
    assert sf6_metadata_2["start_date"] == expected_start_2
    assert sf6_metadata_2["end_date"] == expected_end_2


# Check overwrite functionality
# TODO: Add check to overwrite functionality
# - need to be clear on what we expect to happen here


def bsd_data_read_crds_overwrite():
    """
    Add Bilsdale data for CRDS instrument to object store.
     - CRDS: ch4, co2, co
    """

    site = "bsd"
    network = "DECC"
    source_format1 = "CRDS"

    bsd_path1 = get_surface_datapath(filename="bsd.picarro.1minute.108m.min.dat", source_format="CRDS")

    bucket = get_bucket()
    with ObsSurface(bucket=bucket) as obs:
        obs.read_file(
            filepath=bsd_path1, source_format=source_format1, site=site, network=network, overwrite=True
        )


# def test_obs_data_read_overwrite():
#     """
#     Test adding new file for GC with same time points but some different data values
#     """
#     clear_test_stores()
#     # Load BSD data - CRDS minutely frequency
#     bsd_data_read_crds()
#     # Load BSD data - CRDS hourly frequency
#     bsd_data_read_crds_diff_frequency()
#     # Load BSD data - GCMD data (GCWERKS)
#     bsd_data_read_gcmd()
#     # Load BSD data - CRDS minutely frequency - overwrite
#     bsd_data_read_crds_overwrite()

#     # Search for expected species
#     # CRDS data
#     search_ch4 = search(site="bsd", species="ch4")
#     search_co2 = search(site="bsd", species="co2")
#     search_co = search(site="bsd", species="co")
#     # GCMD data
#     search_sf6 = search(site="bsd", species="sf6")
#     search_n2o = search(site="bsd", species="n2o")

#     # Check something is found for each search
#     assert bool(search_ch4) == True
#     assert bool(search_co2) == True
#     assert bool(search_co) == True
#     assert bool(search_sf6) == True
#     assert bool(search_n2o) == True

#     crds_file_data = read_crds_file_pd(filename="bsd.picarro.1minute.108m.min.dat")

#     obs_data_ch4 = search_ch4.retrieve()
#     data_ch4 = obs_data_ch4.data

#     ch4 = data_ch4["ch4"].values
#     expected_ch4 = crds_file_data["ch4"].values
#     np.testing.assert_allclose(ch4, expected_ch4)

#     # ch4_time = data_ch4["time"].values
#     # expected_ch4_time = crds_file_data["date_time"].values
#     # np.testing.assert_allclose(ch4_time, expected_ch4_time)

#     # gcwerks_file_data = read_gcmd_file_pd("bilsdale-md.small-edit.14.C")

#     # obs_data_sf6 = search_sf6.retrieve()
#     # data_sf6 = obs_data_sf6.data

#     # sf6 = data_sf6["sf6"].values
#     # expected_sf6 = gcwerks_file_data["SF6"].values
#     # np.testing.assert_allclose(sf6, expected_sf6)

#     # TODO: Can we check if this has been saved as a new version?


# Test
@pytest.mark.parametrize(
    "standard_filename,special_filename,site,domain,model,metmodel,inlet,species",
    [
        (
            "TAC-185magl_UKV_EUROPE_TEST_201405.nc",
            "TAC-185magl_UKV_co2_EUROPE_TEST_201405.nc",
            "TAC",
            "EUROPE",
            "NAME",
            "UKV",
            "185m",
            "co2",
        ),
        (
            "HFD-100magl_UKV_EUROPE_202001_TRUNCATED.nc",
            "HFD-100magl_UKV_rn_EUROPE_202001_TRUNCATED.nc",
            "HFD",
            "EUROPE",
            "NAME",
            "UKV",
            "100m",
            "rn",
        )
    ],
)
def test_standardising_footprint_with_additional_keys(standard_filename, special_filename, site, domain, model, metmodel, inlet, species):
    """
    Expected behavior: adding a high time resolution
    (or short_lifetime) footprint whose other metadata
    is the same as an existing footprint will create a
    new uuid for the high resolution (or short_lifetime)
    footprint.
    """
    standard_datapath = get_footprint_datapath(standard_filename)
    special_datapath = get_footprint_datapath(special_filename)

    clear_test_stores()

    standard_standardised = standardise_footprint(standard_datapath,
                                                site=site,
                                                domain=domain,
                                                model=model,
                                                inlet=inlet,
                                                metmodel=metmodel,
                                                store="user",
                                                )

    special_standardised = standardise_footprint(special_datapath,
                                                site=site,
                                                domain=domain,
                                                model=model,
                                                inlet=inlet,
                                                metmodel=metmodel,
                                                species=species,
                                                store="user",
                                                )

    standard_dict = standard_standardised[next(iter(standard_standardised))]
    special_dict = special_standardised[next(iter(special_standardised))]

    assert special_dict["new"] == True
    assert special_dict["uuid"] != standard_dict["uuid"]  # redundant?
