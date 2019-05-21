import datetime
import os
import pandas as pd
import pytest

from objectstore.local_bucket import get_local_bucket
from processing._crds import CRDS
from processing import _recombination

@pytest.fixture(scope="session")
def keylist():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    crds = CRDS.read_file(filepath)
    # Get the keylist
    bucket = get_local_bucket("crds")
    # Create and store data
    crds.save(bucket=bucket)

    start = datetime.datetime.strptime("2014-01-30", "%Y-%m-%d")
    end = datetime.datetime.strptime("2014-01-30", "%Y-%m-%d")

    keys = crds.search_store(bucket=bucket, root_path="datasource", datetime_begin=start, datetime_end=end)

    return keys

def test_get_sections(keylist):
    bucket = get_local_bucket("crds")

    datasources = _recombination.get_sections(bucket, keylist)

    gas_names = ["co", "co2", "ch4"]
    recorded_gas_names = [datasources[0]._name, datasources[1]._name, datasources[2]._name]

    assert sorted(gas_names) == sorted(recorded_gas_names)
    assert len(datasources) == 3


def test_combine_sections(keylist):
    bucket = get_local_bucket("crds")

    datasources = _recombination.get_sections(bucket, keylist)

    dataframes = [datasource._data for datasource in datasources]

    # print(dataframes)

    assert isinstance(dataframes[0], pd.DataFrame)

    combined = _recombination.combine_sections(dataframes)

    # The same 3 dataframes are being returned each time - fix this
    
    assert False

    # print(combined)

    



    






    

