import pytest
from openghg.store import Emissions, ObsSurface
from helpers import get_datapath, get_emissions_datapath


@pytest.fixture(scope="session")
def process_crds():

    bsd_248_path = get_datapath(filename="bsd.picarro.1minute.248m.min.dat", data_type="CRDS")
    bsd_108_path = get_datapath(filename="bsd.picarro.1minute.108m.min.dat", data_type="CRDS")
    bsd_42_path = get_datapath(filename="bsd.picarro.1minute.42m.min.dat", data_type="CRDS")
    hfd_100_path = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")

    ObsSurface.read_file(
        filepath=[bsd_248_path, bsd_108_path, bsd_42_path],
        data_type="CRDS",
        network="DECC",
        site="bsd",
    )

    ObsSurface.read_file(
        filepath=hfd_100_path,
        data_type="CRDS",
        network="DECC",
        site="hfd",
        inlet="100m",
    )


@pytest.fixture(scope="session")
def process_gcwerks():
    cgo = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    cgo_prec = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")
    ObsSurface.read_file(filepath=(cgo, cgo_prec), data_type="GCWERKS", network="AGAGE", site="CGO")


# @pytest.fixture(scope="module")
# def process_emissions():
#     co2_emissions = get_emissions_datapath("co2-gpp-cardamom-mth_EUROPE_2012.nc")

#     Emissions.read_file(
#         filepath=co2_emissions,
#         species="co2",
#         source="gpp-cardamom",
#         date="2012",
#         domain="europe",
#         high_time_resolution=False,
#     )
