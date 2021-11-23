import logging

from openghg.process.surface import CRDS, GCWERKS
from openghg.store import ObsSurface
from openghg.objectstore import get_local_bucket
from openghg.store import recombine_datasets
from openghg.retrieve import search
from helpers import get_datapath

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_recombination_CRDS():
    get_local_bucket(empty=True)

    filename = "hfd.picarro.1minute.100m.min.dat"
    filepath = get_datapath(filename=filename, data_type="CRDS")

    crds = CRDS()

    ObsSurface.read_file(filepath, data_type="CRDS", site="hfd", network="DECC")

    gas_data = crds.read_data(data_filepath=filepath, site="HFD", network="AGAGE")

    ch4_data_read = gas_data["ch4"]["data"]

    species = "ch4"
    site = "hfd"
    inlet = "100m"

    result = search(species=species, site=site, inlet=inlet)

    keys = result.keys(site=site, species=species, inlet=inlet)

    ch4_data_recombined = recombine_datasets(keys=keys)

    ch4_data_recombined.attrs = {}

    assert ch4_data_read.time.equals(ch4_data_recombined.time)
    assert ch4_data_read["ch4"].equals(ch4_data_recombined["ch4"])


def test_recombination_GC():
    get_local_bucket(empty=True)

    gc = GCWERKS()

    data = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    precision = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

    ObsSurface.read_file((data, precision), data_type="GCWERKS", site="cgo", network="agage")

    data = gc.read_data(data_filepath=data, precision_filepath=precision, site="CGO", instrument="medusa", network="AGAGE")

    toluene_data = data["toluene_70m"]["data"]

    species = "toluene"
    site = "CGO"
    inlet = "70m"

    result = search(species=species, site=site, inlet=inlet)
    keys = result.keys(site=site, species=species, inlet=inlet)

    toluene_data_recombined = recombine_datasets(keys=keys)

    toluene_data.attrs = {}
    toluene_data_recombined.attrs = {}

    assert toluene_data.time.equals(toluene_data_recombined.time)
    assert toluene_data["toluene"].equals(toluene_data_recombined["c6h5ch3"])
    assert toluene_data["toluene repeatability"].equals(toluene_data_recombined["c6h5ch3_repeatability"])
    assert toluene_data["toluene status_flag"].equals(toluene_data_recombined["c6h5ch3_status_flag"])
    assert toluene_data["toluene integration_flag"].equals(toluene_data_recombined["c6h5ch3_integration_flag"])
