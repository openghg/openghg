# TODO - look into what's causing the logging messages in the first place
import logging
mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

# Testing the GC class
import datetime
import pytest
from pathlib import Path
import pandas as pd
import os
import uuid

from Acquire.ObjectStore import datetime_to_string, string_to_datetime, datetime_to_datetime
from HUGS.Modules import GC
from HUGS.Processing import read_metadata
from HUGS.ObjectStore import get_local_bucket
from HUGS.ObjectStore import get_object_names
from HUGS.Util import get_datetime_epoch

@pytest.fixture(scope="session")
def data_path():
    # This feels messy
    return Path(__file__).resolve().parent.joinpath("../data/proc_test_data/GC/capegrim-medusa.18.C")

@pytest.fixture(scope="session")
def precision_path():
    return Path(__file__).resolve().parent.joinpath("../data/proc_test_data/GC/capegrim-medusa.18.precisions.C")

@pytest.fixture
def gc():
    gc = GC()
    gc._uuid = "123"
    gc._creation_datetime = datetime_to_datetime(datetime.datetime(1970,1,1))
    gc.save()

    return gc

def test_read_data(data_path, precision_path):
    # Capegrim
    site = "CGO"
    instrument = "GCMD"

    data_path = Path(data_path)
    precision_path = Path(precision_path)

    gc = GC()
    gas_data, species, metadata = gc.read_data(data_filepath=data_path, precision_filepath=precision_path, site=site, instrument=instrument)

    head_data = gas_data.head(1)
    tail_data = gas_data.tail(1)

    assert head_data.first_valid_index() == pd.Timestamp("2018-01-01 00:23:22.500")
    assert head_data["propane repeatability"].iloc[0] == 0.22325
    assert head_data["c-propane repeatability"].iloc[0] == 0.10063

    assert tail_data.first_valid_index() == pd.Timestamp("2018-01-31 23:42:22.500")
    assert tail_data["propane repeatability"].iloc[0] == 0.16027
    assert tail_data["c-propane repeatability"].iloc[0] == 0.06071

    assert species[:8] == ['NF3', 'CF4', 'PFC-116', 'PFC-218', 'PFC-318', 'C4F10', 'C6F14', 'SF6'] 

    assert metadata["site"] == "capegrim"
    assert metadata["instrument"] == "medusa"


def test_read_precision(precision_path):
    gc = GC()

    precision, precision_series = gc.read_precision(precision_path)

    prec_test = ['NF3', 'CF4', 'PFC-116', 'PFC-218', 'PFC-318', 'C4F10', 'C6F14', 'SF6']
    end_prec_test = ['ethene', 'ethane', 'propane', 'c-propane', 'benzene', 'toluene', 'COS', 'desflurane']

    assert precision_series[:8] == prec_test
    assert precision_series[-8:] == end_prec_test

    precision_head = precision.head(1)
    
    assert precision_head.iloc[0,0] == 0.02531
    assert precision_head.iloc[0,2] == 0.08338
    assert precision_head.iloc[0,5] == 10
    assert precision_head.iloc[0,7] == 10
    assert precision_head.iloc[0,10] == 0.00565
    
def test_split(data_path, precision_path):
    # Capegrim
    site = "capegrim"
    instrument = "GCMD"

    gc = GC()
    data, species, metadata = gc.read_data(data_filepath=data_path, precision_filepath=precision_path, site=site, instrument=instrument)
    metadata = read_metadata(filepath=data_path, data=None, data_type="GC")
    gas_data = gc.split(data=data, site=site, species=species, metadata=metadata)

    metadata = gas_data["NF3"]["metadata"]
    data = gas_data["NF3"]["data"]

    assert metadata == {'inlet': '75m_4', 'instrument': 'medusa', 'site': 'capegrim', 'species': 'NF3'}
    
    head_data = data.head(1)
    assert head_data["NF3"].iloc[0] == pytest.approx(1.603)
    assert head_data["NF3 repeatability"].iloc[0] == pytest.approx(0.02531)
    assert head_data["NF3 status_flag"].iloc[0] == 0
    assert head_data["NF3 integration_flag"].iloc[0] == 0
    assert head_data["Inlet"].iloc[0] == "75m_4"

def test_to_data(gc):
    data = gc.to_data()

    assert data["stored"] == True
    assert data["creation_datetime"] == datetime_to_string(datetime.datetime(1970,1,1))


def test_from_data(gc):
    data = gc.to_data()

    epoch = datetime_to_datetime(datetime.datetime(1970, 1, 1, 1, 1))
    data["creation_datetime"] = datetime_to_string(epoch)

    random_data1 = uuid.uuid4()
    random_data2 = uuid.uuid4()

    test_hashes = {"test1": random_data1, "test2": random_data2}
    test_datasources = {"datasource1": random_data1, "datasource2": random_data2}

    data["file_hashes"] = test_hashes
    data["datasource_names"] = test_datasources
    data["datasource_uuids"] = test_datasources

    gc_new = GC.from_data(data)

    assert gc_new._stored == False
    assert gc_new._creation_datetime == epoch
    assert gc_new._datasource_names == test_datasources
    assert gc_new._datasource_uuids == test_datasources
    assert gc_new._file_hashes == test_hashes


def test_save(gc):
    bucket = get_local_bucket(empty=True)

    gc.save()

    prefix = f""
    objs = get_object_names(bucket, prefix)

    assert objs[0].split("/")[-1] == GC._uuid

def test_load(gc):
    gc.save()
    gc_new = GC.load()

    assert gc_new._stored == False
    assert gc_new._creation_datetime == datetime_to_datetime(datetime.datetime(1970,1,1))

def test_exists(gc):
    bucket = get_local_bucket()
    gc.save(bucket=bucket)

    assert GC.exists(bucket=bucket) == True
