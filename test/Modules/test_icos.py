import logging
mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

import os
import pandas as pd
import pytest

from HUGS.Modules import Datasource, ICOS
from HUGS.ObjectStore import get_local_bucket

def test_read_data():
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/ICOS"
    filename = "tta.co2.1minute.222m.min.dat"

    filepath = os.path.join(dir_path, test_data, filename)

    icos = ICOS.load()

    data  = icos.read_data(data_filepath=filepath, species="CO2")
    
    print(data)

    assert False


