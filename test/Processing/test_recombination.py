import logging
mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

import datetime
import os
from pathlib import Path
import pandas as pd
import pytest
# import xarray

from HUGS.Modules import CRDS, GC
from HUGS.Processing import search, recombine_sections
from HUGS.ObjectStore import get_local_bucket

@pytest.fixture(scope="session")
def data_path():
    return os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "../data/proc_test_data/GC/capegrim-medusa.18.C"


@pytest.fixture(scope="session")
def precision_path():
    return os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "../data/proc_test_data/GC/capegrim-medusa.18.precisions.C"

def test_recombination_CRDS():
    _ = get_local_bucket(empty=True)

    crds = CRDS.load()

    # filename = "bsd.picarro.1minute.248m.dat"
    filename = "hfd.picarro.1minute.100m_min.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)
    
    filepath = Path(filepath)

    uuids = CRDS.read_file(filepath)

    gas_data = crds.read_data(data_filepath=filepath, site="HFD")

    ch4_data_read = gas_data["ch4"]["data"]

    gas_name = "ch4"
    location = "hfd"
    data_type = "CRDS"

    keys = search(search_terms=gas_name, locations=location, data_type=data_type)

    to_download = keys["hfd_ch4_100m_min"]["keys"]

    ch4_data_recombined = recombine_sections(data_keys=to_download)

    ch4_data_recombined.attrs = {}

    assert ch4_data_read.time.equals(ch4_data_recombined.time)
    assert ch4_data_read["ch4"].equals(ch4_data_recombined["ch4"])


