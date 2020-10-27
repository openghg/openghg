import logging
import os
from pathlib import Path

import pytest

from openghg.modules import CRDS, GCWERKS, ObsSurface
from openghg.objectstore import get_local_bucket
from openghg.processing import recombine_sections, search

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


@pytest.fixture(scope="session")
def data_path():
    return (
        os.path.dirname(os.path.abspath(__file__))
        + os.path.sep
        + "../data/proc_test_data/GC/capegrim-medusa.18.C"
    )


@pytest.fixture(scope="session")
def precision_path():
    return (
        os.path.dirname(os.path.abspath(__file__))
        + os.path.sep
        + "../data/proc_test_data/GC/capegrim-medusa.18.precisions.C"
    )


def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")


def test_recombination_CRDS():
    get_local_bucket(empty=True)

    filename = "hfd.picarro.1minute.100m.min.dat"
    filepath = get_datapath(filename=filename, data_type="CRDS")

    crds = CRDS()

    ObsSurface.read_file(filepath, data_type="CRDS")

    gas_data = crds.read_data(data_filepath=filepath, site="HFD", network="AGAGE")

    ch4_data_read = gas_data["ch4"]["data"]

    gas_name = "ch4"
    location = "hfd"

    keys = search(species=gas_name, locations=location)

    to_download = keys["ch4_hfd_100m_picarro"]["keys"]["2013-12-04-14:02:30_2019-05-21-15:46:30"]

    ch4_data_recombined = recombine_sections(data_keys=to_download)

    ch4_data_recombined.attrs = {}

    assert ch4_data_read.time.equals(ch4_data_recombined.time)
    assert ch4_data_read["ch4"].equals(ch4_data_recombined["ch4"])


def test_recombination_GC():
    get_local_bucket(empty=True)

    gc = GCWERKS()

    data = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    precision = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

    ObsSurface.read_file((data, precision), data_type="GCWERKS")

    data = gc.read_data(data_filepath=data, precision_filepath=precision, site="CGO", instrument="medusa", network="AGAGE")

    toluene_data = data["toluene_75m_4"]["data"]

    gas_name = "toluene"
    location = "CGO"

    keys = search(species=gas_name, locations=location)

    to_download = keys["toluene_cgo_75m_4_medusa"]["keys"]["2018-01-01-02:24:00_2018-01-31-23:33:00"]

    toluene_data_recombined = recombine_sections(data_keys=to_download)

    toluene_data.attrs = {}
    toluene_data_recombined.attrs = {}

    assert toluene_data.time.equals(toluene_data_recombined.time)
    assert toluene_data["toluene"].equals(toluene_data_recombined["c6h5ch3"])
    assert toluene_data["toluene repeatability"].equals(toluene_data_recombined["c6h5ch3_repeatability"])
    assert toluene_data["toluene status_flag"].equals(toluene_data_recombined["c6h5ch3_status_flag"])
    assert toluene_data["toluene integration_flag"].equals(toluene_data_recombined["c6h5ch3_integration_flag"])
