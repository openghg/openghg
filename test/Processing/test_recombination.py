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
    # filename = "bsd.picarro.1minute.248m.dat"
    filename = "hfd.picarro.1minute.100m_min.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    _ = get_local_bucket(empty=True)

    crds = CRDS.read_file(filepath)

    gas_data = crds.read_data(data_filepath=filepath)

    # Date from the processing function, before being passed to the
    # Datasources for segmentation by date
    complete_data = gas_data[2][3]

    gas_name = "co"
    data_type = "CRDS"

    keys = search(search_terms=gas_name, data_type=data_type)

    recombined_dataframes = recombine_sections(data_keys=keys)

    assert len(keys) == 1
    assert list(recombined_dataframes.keys())[0] == "co"
    assert complete_data.equals(recombined_dataframes["co"])



# def test_recombination_GC(data_path, precision_path):
#     gc = GC.create()
#     gc.save()

#     gc = GC.read_file(data_filepath=data_path, precision_filepath=precision_path)

#     site = "CGO"
#     instrument_name = "GCMD"
#     gas_data = gc.read_data(data_filepath=data_path, precision_filepath=precision_path, site=site, instrument=instrument_name)

#     complete_data = gas_data[0][3]

#     gas_name = "NF3"
#     data_type = "GC"

#     keys = gas_search(species=gas_name, data_type=data_type)

#     recombined_dataframe = recombine_sections(data_keys=keys)

#     assert len(keys) == 1
#     assert recombined_dataframe.equals(complete_data)





# @pytest.fixture(scope="session")
# def keylist():
#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     crds = CRDS.read_file(filepath)
#     # Get the keylist
#     bucket = get_local_bucket()
#     # Create and store data
#     crds.save(bucket=bucket)

#     start = datetime.datetime.strptime("2014-01-30", "%Y-%m-%d")
#     end = datetime.datetime.strptime("2014-01-31", "%Y-%m-%d")

#     keys = search_store(bucket=bucket, root_path="datasource", start_datetime=start, end_datetime=end)

#     return keys

# # def test_get_datasources(keylist):
# #     bucket = get_local_bucket()

# #     datasources = _recombination.get_sections(bucket, keylist)

# #     gas_names = ["co", "co2", "ch4"]
# #     recorded_gas_names = [datasources[0]._name, datasources[1]._name, datasources[2]._name]

# #     assert sorted(gas_names) == sorted(recorded_gas_names)
# #     assert len(datasources) == 3


# def test_combine_sections():
#     bucket = get_local_bucket(empty=True)

#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     # Split and combine without passing through the object store
#     raw_data = pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")

#     # raw_data = raw_data.dropna(axis=0, how="any")
#     # raw_data.index = pd.RangeIndex(raw_data.index.size)
#     # datasource_ids, dataframes = parse_gases(raw_data)

#     gas_data = parse_gases(raw_data)

#     _, _, dataframes = zip(*gas_data)
#     complete = combine_sections(dataframes)

#     # Load in from object store
#     crds = CRDS.read_file(filepath)

#     # Get the instrument
#     instrument_uuids = list(crds._instruments)

#     # Get UUID from Instrument
#     instrument =  Instrument.load(bucket=bucket, uuid=instrument_uuids[0])

#     # Get Datasource IDs from Instrument
#     uuid_list = [d._uuid for d in instrument._datasources]
#     datasources = get_datasources(bucket, uuid_list)
#     combined = combine_sections(dataframes)

#     assert combined.equals(complete)


# def test_convert_to_netcdf(): 
#     filename = "bsd.picarro.1minute.248m.dat"
#     dir_path = os.path.dirname(__file__)
#     test_data = "../data/proc_test_data/CRDS"
#     filepath = os.path.join(dir_path, test_data, filename)

#     raw_data = pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+")
#     gas_data = parse_gases(raw_data)
#     dataframes = [data for _, data in gas_data]
#     complete = combine_sections(dataframes)

#     filename = _recombination.convert_to_netcdf(complete)

#     # Open the NetCDF and check it's valid?
#     x_dataset = complete.to_xarray()

#     ds = xarray.open_dataset(filename)

#     os.remove(filename)

#     assert ds.equals(x_dataset)
    




