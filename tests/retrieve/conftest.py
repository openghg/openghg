import pytest
from helpers import (
    get_bc_datapath,
    get_column_datapath,
    get_emissions_datapath,
    get_eulerian_datapath,
    get_footprint_datapath,
    get_surface_datapath,
)
from openghg.objectstore import get_bucket
from openghg.store import (
    BoundaryConditions,
    Emissions,
    EulerianModel,
    Footprints,
    ObsColumn,
    ObsSurface,
)


@pytest.fixture(scope="module", autouse=True)
def data_read():
    get_bucket(empty=True)

    # DECC network sites
    network = "DECC"
    bsd_248_path = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")
    bsd_108_path = get_surface_datapath(filename="bsd.picarro.1minute.108m.min.dat", source_format="CRDS")
    bsd_42_path = get_surface_datapath(filename="bsd.picarro.1minute.42m.min.dat", source_format="CRDS")

    bsd_paths = [bsd_248_path, bsd_108_path, bsd_42_path]

    bsd_results = ObsSurface.read_file(filepath=bsd_paths, source_format="CRDS", site="bsd", network=network)

    hfd_100_path = get_surface_datapath(filename="hfd.picarro.1minute.100m.min.dat", source_format="CRDS")
    hfd_50_path = get_surface_datapath(filename="hfd.picarro.1minute.50m.min.dat", source_format="CRDS")
    hfd_paths = [hfd_100_path, hfd_50_path]

    ObsSurface.read_file(filepath=hfd_paths, source_format="CRDS", site="hfd", network=network)

    tac_path = get_surface_datapath(filename="tac.picarro.1minute.100m.test.dat", source_format="CRDS")
    ObsSurface.read_file(filepath=tac_path, source_format="CRDS", site="tac", network=network)

    # GCWERKS data (AGAGE network sites)
    data_filepath = get_surface_datapath(filename="capegrim-medusa.18.C", source_format="GC")
    prec_filepath = get_surface_datapath(filename="capegrim-medusa.18.precisions.C", source_format="GC")

    ObsSurface.read_file(
        filepath=(data_filepath, prec_filepath), site="CGO", source_format="GCWERKS", network="AGAGE"
    )

    mhd_data_filepath = get_surface_datapath(filename="macehead.12.C", source_format="GC")
    mhd_prec_filepath = get_surface_datapath(filename="macehead.12.precisions.C", source_format="GC")

    ObsSurface.read_file(
        filepath=(mhd_data_filepath, mhd_prec_filepath),
        site="MHD",
        source_format="GCWERKS",
        network="AGAGE",
        instrument="GCMD",
    )

    obs = ObsSurface.load()

    uid_248 = bsd_results["processed"]["bsd.picarro.1minute.248m.min.dat"]["ch4"]["uuid"]
    obs.set_rank(uuid=uid_248, rank=1, date_range="2014-01-30_2015-01-01")

    uid_108 = bsd_results["processed"]["bsd.picarro.1minute.108m.min.dat"]["ch4"]["uuid"]
    obs.set_rank(uuid=uid_108, rank=1, date_range="2015-01-02_2015-11-01")

    obs.set_rank(uuid=uid_248, rank=1, date_range="2016-04-01_2017-11-01")

    uid_42 = bsd_results["processed"]["bsd.picarro.1minute.42m.min.dat"]["ch4"]["uuid"]
    obs.set_rank(uuid=uid_42, rank=1, date_range="2019-01-01_2021-01-01")

    obs.save()

    # Obs Column data
    column_datapath = get_column_datapath("gosat-fts_gosat_20170318_ch4-column.nc")

    ObsColumn.read_file(
        filepath=column_datapath,
        source_format="OPENGHG",
        satellite="GOSAT",
        domain="BRAZIL",
        species="methane",
    )

    # Emissions data
    test_datapath = get_emissions_datapath("co2-gpp-cardamom_EUROPE_2012.nc")

    Emissions.read_file(
        filepath=test_datapath,
        species="co2",
        source="gpp-cardamom",
        date="2012",
        domain="europe",
        high_time_resolution=False,
    )

    # Footprint data
    fp_datapath1 = get_footprint_datapath("footprint_test.nc")

    site = "TMB"
    network = "LGHG"
    height = "10m"
    domain = "EUROPE"
    model = "test_model"

    Footprints.read_file(
        filepath=fp_datapath1,
        site=site,
        model=model,
        network=network,
        height=height,
        domain=domain,
        high_spatial_res=True,
    )

    # Add two footprints with the same inputs but covering different time periods
    fp_datapath2 = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    fp_datapath3 = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")

    site = "TAC"
    network = "DECC"
    height = "100m"
    domain = "TEST"
    model = "NAME"
    metmodel = "UKV"

    Footprints.read_file(
        filepath=fp_datapath2,
        site=site,
        model=model,
        network=network,
        height=height,
        domain=domain,
        metmodel=metmodel,
    )

    Footprints.read_file(
        filepath=fp_datapath3,
        site=site,
        model=model,
        network=network,
        height=height,
        domain=domain,
        metmodel=metmodel,
    )


    test_datapath = get_bc_datapath("n2o_EUROPE_2012.nc")

    species = "n2o"
    bc_input = "MOZART"
    domain = "EUROPE"

    BoundaryConditions.read_file(
        filepath=test_datapath,
        species=species,
        bc_input=bc_input,
        domain=domain,
    )

    test_datapath = get_eulerian_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")

    proc_results = EulerianModel.read_file(filepath=test_datapath, model="GEOSChem", species="ch4")
