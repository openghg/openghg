import datetime
import os
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

    crds = CRDS.create()
    crds.save()
    # filename = "bsd.picarro.1minute.248m.dat"
    filename = "hfd.picarro.1minute.100m_min.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)
    
    crds = CRDS.read_file(filepath)

    gas_data = crds.read_data(data_filepath=filepath)

    # Date from the processing function, before being passed to the
    # Datasources for segmentation by date
    complete_data = gas_data[2][3]

    gas_name = "co"
    location = "hfd"
    data_type = "CRDS"

    keys = search(search_terms=gas_name, locations=location, data_type=data_type)

    recombined_dataframes = recombine_sections(data_keys=keys)

    assert len(keys) == 1
    assert list(recombined_dataframes.keys())[0] == "hfd_co"
    assert complete_data.equals(recombined_dataframes["hfd_co"])




