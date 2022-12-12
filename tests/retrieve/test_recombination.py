import logging

from helpers import get_surface_datapath
from openghg.objectstore import get_bucket
from openghg.retrieve import search
from openghg.standardise.surface import parse_crds, parse_gcwerks
from openghg.store import ObsSurface, recombine_datasets

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_recombination_CRDS():
    filename = "hfd.picarro.1minute.100m.min.dat"
    filepath = get_surface_datapath(filename=filename, source_format="CRDS")

    gas_data = parse_crds(data_filepath=filepath, site="HFD", network="AGAGE")

    ch4_data_read = gas_data["ch4"]["data"]

    species = "ch4"
    site = "hfd"
    inlet = "100m"

    result = search(species=species, site=site, inlet=inlet)

    uuid = next(iter(result.key_data))

    keys = result.key_data[uuid]

    ch4_data_recombined = recombine_datasets(keys=keys)

    ch4_data_recombined.attrs = {}

    assert ch4_data_read.time.equals(ch4_data_recombined.time)
    assert ch4_data_read["ch4"].equals(ch4_data_recombined["ch4"])


def test_recombination_GC():
    data = get_surface_datapath(filename="capegrim-medusa.18.C", source_format="GC")
    precision = get_surface_datapath(filename="capegrim-medusa.18.precisions.C", source_format="GC")

    data = parse_gcwerks(
        data_filepath=data, precision_filepath=precision, site="CGO", instrument="medusa", network="AGAGE"
    )

    toluene_data = data["c6h5ch3_70m"]["data"]

    species = "c6h5ch3"
    site = "CGO"
    inlet = "70m"

    result = search(species=species, site=site, inlet=inlet)

    uuid = next(iter(result.key_data))

    keys = result.key_data[uuid]

    toluene_data_recombined = recombine_datasets(keys=keys)

    toluene_data.attrs = {}
    toluene_data_recombined.attrs = {}

    assert toluene_data.time.equals(toluene_data_recombined.time)
    assert toluene_data["c6h5ch3"].equals(toluene_data_recombined["c6h5ch3"])
    assert toluene_data["c6h5ch3_repeatability"].equals(toluene_data_recombined["c6h5ch3_repeatability"])
    assert toluene_data["c6h5ch3_status_flag"].equals(toluene_data_recombined["c6h5ch3_status_flag"])
    assert toluene_data["c6h5ch3_integration_flag"].equals(
        toluene_data_recombined["c6h5ch3_integration_flag"]
    )
