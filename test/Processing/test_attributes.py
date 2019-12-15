from pathlib import Path
import pytest
import os

from HUGS.Processing import get_attributes
from HUGS.Modules import CRDS
from HUGS.ObjectStore import get_local_bucket

import logging
mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

def test_crds_attributes():
    _ = get_local_bucket(empty=True)

    crds = CRDS.load()

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "tac.picarro.1minute.100m.test.dat"

    filepath = os.path.join(dir_path, test_data, filename)

    filepath = Path(filepath)

    combined = crds.read_data(data_filepath=filepath, site="tac")

    combined_attributes = crds.assign_attributes(data=combined, site="tac")

    assert False


def test_acrg_attributes():
    _ = get_local_bucket(empty=True)

    crds = CRDS.load()

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filename = "tac.picarro.1minute.100m.test.dat"

    filepath = os.path.join(dir_path, test_data, filename)

    filepath = Path(filepath)

    combined = crds.read_data(data_filepath=filepath, site="tac")

    combined_attributes = crds.acrg_assign_attributes(data=combined, site="tac")

    return False    

