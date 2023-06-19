import pytest
from helpers import (
    get_bc_datapath,
    get_column_datapath,
    get_emissions_datapath,
    get_eulerian_datapath,
    get_footprint_datapath,
    get_surface_datapath,
    clear_test_stores,
)

from openghg.store import (
    BoundaryConditions,
    Emissions,
    EulerianModel,
    Footprints,
    ObsColumn,
    ObsSurface,
)

from openghg.objectstore import get_bucket


@pytest.fixture(scope="module", autouse=True)
def data_read():
    clear_test_stores()

    # DECC network sites
    network = "DECC"
    bsd_248_path = get_surface_datapath(filename="bsd.picarro.1minute.248m.min.dat", source_format="CRDS")
    bsd_108_path = get_surface_datapath(filename="bsd.picarro.1minute.108m.min.dat", source_format="CRDS")
    bsd_42_path = get_surface_datapath(filename="bsd.picarro.1minute.42m.min.dat", source_format="CRDS")

    bsd_paths = [bsd_248_path, bsd_108_path, bsd_42_path]

    bucket = get_bucket()
    with ObsSurface(bucket=bucket) as obs:
        bsd_results = obs.read_file(filepath=bsd_paths, source_format="CRDS", site="bsd", network=network)

        hfd_100_path = get_surface_datapath(filename="hfd.picarro.1minute.100m.min.dat", source_format="CRDS")
        hfd_50_path = get_surface_datapath(filename="hfd.picarro.1minute.50m.min.dat", source_format="CRDS")
        hfd_paths = [hfd_100_path, hfd_50_path]

        obs.read_file(filepath=hfd_paths, source_format="CRDS", site="hfd", network=network)

        tac_path = get_surface_datapath(filename="tac.picarro.1minute.100m.test.dat", source_format="CRDS")
        obs.read_file(filepath=tac_path, source_format="CRDS", site="tac", network=network)

        # GCWERKS data (AGAGE network sites)
        data_filepath = get_surface_datapath(filename="capegrim-medusa.18.C", source_format="GC")
        prec_filepath = get_surface_datapath(filename="capegrim-medusa.18.precisions.C", source_format="GC")

        obs.read_file(
            filepath=(data_filepath, prec_filepath), site="CGO", source_format="GCWERKS", network="AGAGE"
        )

        mhd_data_filepath = get_surface_datapath(filename="macehead.12.C", source_format="GC")
        mhd_prec_filepath = get_surface_datapath(filename="macehead.12.precisions.C", source_format="GC")

        obs.read_file(
            filepath=(mhd_data_filepath, mhd_prec_filepath),
            site="MHD",
            source_format="GCWERKS",
            network="AGAGE",
            instrument="GCMD",
        )

    # with ObsSurface(bucket=bucket) as obs:

    #     uid_248 = bsd_results["processed"]["bsd.picarro.1minute.248m.min.dat"]["ch4"]["uuid"]
    #     obs.set_rank(uuid=uid_248, rank=1, date_range="2014-01-30_2015-01-01")

    #     uid_108 = bsd_results["processed"]["bsd.picarro.1minute.108m.min.dat"]["ch4"]["uuid"]
    #     obs.set_rank(uuid=uid_108, rank=1, date_range="2015-01-02_2015-11-01")

    #     obs.set_rank(uuid=uid_248, rank=1, date_range="2016-04-01_2017-11-01")

    #     uid_42 = bsd_results["processed"]["bsd.picarro.1minute.42m.min.dat"]["ch4"]["uuid"]
    #     obs.set_rank(uuid=uid_42, rank=1, date_range="2019-01-01_2021-01-01")

    with ObsSurface(bucket=bucket) as obs:
    # Obs Surface - openghg pre-formatted data
    # - This shouldn't conflict with TAC data above as this is for 185m rather than 100m
        openghg_path = get_surface_datapath(
            filename="DECC-picarro_TAC_20130131_co2-185m-20220929_cut.nc", source_format="OPENGHG"
        )
        obs.read_file(
            filepath=openghg_path,
            source_format="OPENGHG",
            site="tac",
            network="DECC",
            instrument="picarro",
            sampling_period="1H",
        )

    # Obs Column data
    column_datapath = get_column_datapath("gosat-fts_gosat_20170318_ch4-column.nc")

    with ObsColumn(bucket=bucket) as obscol:
        obscol.read_file(
            filepath=column_datapath,
            source_format="OPENGHG",
            satellite="GOSAT",
            domain="BRAZIL",
            species="methane",
        )

    # Emissions data - added consecutive data for 2012-2013
    # This will be seen as "yearly" data and each file only contains one time point.
    test_datapath1 = get_emissions_datapath("co2-gpp-cardamom_EUROPE_2012.nc")
    test_datapath2 = get_emissions_datapath("co2-gpp-cardamom_EUROPE_2013.nc")

    species = "co2"
    source = "gpp-cardamom"
    domain = "europe"

    with Emissions(bucket=bucket) as ems:
        ems.read_file(
            filepath=test_datapath1,
            species=species,
            source=source,
            domain=domain,
            high_time_resolution=False,
        )

        ems.read_file(
            filepath=test_datapath2,
            species=species,
            source=source,
            domain=domain,
            high_time_resolution=False,
        )

    # Footprint data
    datapath = get_footprint_datapath("footprint_test.nc")

    site = "TMB"
    network = "LGHG"
    height = "10m"
    domain = "EUROPE"
    model = "test_model"

    with Footprints(bucket=bucket) as fps:
        fps.read_file(
            filepath=datapath,
            site=site,
            model=model,
            network=network,
            height=height,
            domain=domain,
            high_spatial_res=True,
        )

    test_datapath = get_bc_datapath("n2o_EUROPE_2012.nc")

    species = "n2o"
    bc_input = "MOZART"
    domain = "EUROPE"

    with BoundaryConditions(bucket=bucket) as bcs:
        bcs.read_file(
            filepath=test_datapath,
            species=species,
            bc_input=bc_input,
            domain=domain,
        )

    test_datapath = get_eulerian_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")

    with EulerianModel(bucket=bucket) as em:
        em.read_file(filepath=test_datapath, model="GEOSChem", species="ch4")
