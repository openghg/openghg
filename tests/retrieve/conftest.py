import pytest
from openghg.objectstore import get_local_bucket
from openghg.store import ObsSurface, Emissions, Footprints
from helpers import get_datapath, get_emissions_datapath, get_footprint_datapath


@pytest.fixture(scope="module", autouse=True)
def data_read():
    get_local_bucket(empty=True)

    # DECC network sites
    network = "DECC"
    bsd_248_path = get_datapath(filename="bsd.picarro.1minute.248m.min.dat", data_type="CRDS")
    bsd_108_path = get_datapath(filename="bsd.picarro.1minute.108m.min.dat", data_type="CRDS")
    bsd_42_path = get_datapath(filename="bsd.picarro.1minute.42m.min.dat", data_type="CRDS")

    bsd_paths = [bsd_248_path, bsd_108_path, bsd_42_path]

    bsd_results = ObsSurface.read_file(filepath=bsd_paths, data_type="CRDS", site="bsd", network=network)

    hfd_100_path = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")
    hfd_50_path = get_datapath(filename="hfd.picarro.1minute.50m.min.dat", data_type="CRDS")
    hfd_paths = [hfd_100_path, hfd_50_path]

    ObsSurface.read_file(filepath=hfd_paths, data_type="CRDS", site="hfd", network=network)

    tac_path = get_datapath(filename="tac.picarro.1minute.100m.test.dat", data_type="CRDS")
    ObsSurface.read_file(filepath=tac_path, data_type="CRDS", site="tac", network=network)

    # GCWERKS data (AGAGE network sites)
    data_filepath = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    prec_filepath = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

    ObsSurface.read_file(filepath=(data_filepath, prec_filepath), site="CGO", data_type="GCWERKS", network="AGAGE")

    mhd_data_filepath = get_datapath(filename="macehead.12.C", data_type="GC")
    mhd_prec_filepath = get_datapath(filename="macehead.12.precisions.C", data_type="GC")

    ObsSurface.read_file(filepath=(mhd_data_filepath, mhd_prec_filepath), site="MHD", data_type="GCWERKS", network="AGAGE", instrument="GCMD")

    # Set ranking information for BSD
    obs = ObsSurface.load()

    uid_248 = bsd_results["processed"]["bsd.picarro.1minute.248m.min.dat"]["ch4"]
    obs.set_rank(uuid=uid_248, rank=1, date_range="2012-01-01_2013-01-01")

    uid_108 = bsd_results["processed"]["bsd.picarro.1minute.108m.min.dat"]["ch4"]
    obs.set_rank(uuid=uid_108, rank=1, date_range="2014-09-02_2014-11-01")

    obs.set_rank(uuid=uid_248, rank=1, date_range="2015-01-01_2015-11-01")

    obs.set_rank(uuid=uid_108, rank=1, date_range="2016-09-02_2018-11-01")

    uid_42 = bsd_results["processed"]["bsd.picarro.1minute.42m.min.dat"]["ch4"]
    obs.set_rank(uuid=uid_42, rank=1, date_range="2019-01-02_2021-01-01")

    # Emissions data
    test_datapath = get_emissions_datapath("co2-gpp-cardamom-mth_EUROPE_2012.nc")

    Emissions.read_file(
        filepath=test_datapath,
        species="co2",
        source="gpp-cardamom",
        date="2012",
        domain="europe",
        high_time_resolution=False,
    )

    # Footprint data
    datapath = get_footprint_datapath("footprint_test.nc")

    site = "TMB"
    network = "LGHG"
    height = "10m"
    domain = "EUROPE"
    model = "test_model"

    Footprints.read_file(
        filepath=datapath, site=site, model=model, network=network, height=height, domain=domain
    )
