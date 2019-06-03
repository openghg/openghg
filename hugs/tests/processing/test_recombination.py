import datetime
import os
import pandas as pd
import pytest
import xarray

from objectstore.local_bucket import get_local_bucket
from processing._crds import CRDS
from processing import _recombination

from processing._segment import parse_gases
from processing._recombination import combine_sections

@pytest.fixture(scope="session")
def keylist():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    crds = CRDS.read_file(filepath)
    # Get the keylist
    bucket = get_local_bucket()
    # Create and store data
    crds.save(bucket=bucket)

    start = datetime.datetime.strptime("2014-01-30", "%Y-%m-%d")
    end = datetime.datetime.strptime("2014-01-31", "%Y-%m-%d")

    keys = crds.search_store(bucket=bucket, root_path="datasource", start_datetime=start, end_datetime=end)

    return keys

# def test_get_datasources(keylist):
#     bucket = get_local_bucket()

#     datasources = _recombination.get_sections(bucket, keylist)

#     gas_names = ["co", "co2", "ch4"]
#     recorded_gas_names = [datasources[0]._name, datasources[1]._name, datasources[2]._name]

#     assert sorted(gas_names) == sorted(recorded_gas_names)
#     assert len(datasources) == 3


def test_combine_sections():
    from modules._datasource import Datasource
    from objectstore.hugs_objstore import get_object
    
    bucket = get_local_bucket()

    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    # Split and combine without passing through the object store
    raw_data = pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

    # raw_data = raw_data.dropna(axis=0, how="any")
    # raw_data.index = pd.RangeIndex(raw_data.index.size)

    gas_data = parse_gases(raw_data)
    dataframes = [data for _, data in gas_data]
    complete = combine_sections(dataframes)
    
    # Parse through the object store
    crds = CRDS.read_file(filepath)
    # Create and store data
    crds.save(bucket=bucket)
    uuid_list = [d._uuid for d in crds._datasources]
    datasources = _recombination.get_datasources(bucket, uuid_list)
    dataframes = [datasource._data for datasource in datasources]
    combined = _recombination.combine_sections(dataframes)

    assert combined.equals(complete)


def test_convert_to_netcdf(): 
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    raw_data = pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")
    gas_data = parse_gases(raw_data)
    dataframes = [data for _, data in gas_data]
    complete = combine_sections(dataframes)

    filename = _recombination.convert_to_netcdf(complete)
    # Open the NetCDF and check it's valid?

    x_dataset = complete.to_xarray()

    ds = xarray.open_dataset(filename)

    os.remove(filename)

    assert ds.equals(x_dataset)
    




