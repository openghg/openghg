# TODO - look into what's causing the logging messages in the first place
# This does stop them
import logging
mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

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

def test_read_file(data_path, precision_path):
    uuids = GC.read_file(data_filepath=data_path, precision_filepath=precision_path, source_name="capegrim_medusa", site="CGO")

    assert len(uuids) == 56

    first_nine = ['capegrim_medusa_C4F10',
                  'capegrim_medusa_C6F14',
                  'capegrim_medusa_CCl4',
                  'capegrim_medusa_CF4',
                  'capegrim_medusa_CFC-11',
                  'capegrim_medusa_CFC-112',
                  'capegrim_medusa_CFC-113',
                  'capegrim_medusa_CFC-114',
                  'capegrim_medusa_CFC-115',
                  'capegrim_medusa_CFC-12']

    key_list = sorted(list(uuids.keys()))[:10]

    assert first_nine == key_list

def test_read_data(data_path, precision_path):
    # Capegrim
    site = "CGO"
    instrument = "GCMD"

    data_path = Path(data_path)
    precision_path = Path(precision_path)

    gc = GC()
    data = gc.read_data(data_filepath=data_path, precision_filepath=precision_path, site=site, instrument=instrument)

    propane_data = data["propane"]["data"]

    head_data = propane_data.head(1)
    tail_data = propane_data.tail(1)

    assert head_data.first_valid_index() == pd.Timestamp("2018-01-01 02:33:22.500")
    assert head_data["propane"].iloc[0] == pytest.approx(5.458)
    assert head_data["propane repeatability"].iloc[0] == 0.22325

    assert tail_data.first_valid_index() == pd.Timestamp("2018-01-31 23:42:22.500")
    assert tail_data["propane"].iloc[0] == 4.136
    assert tail_data["propane repeatability"].iloc[0] == 0.16027

    species = list(data.keys())

    assert species[:8] == ['NF3', 'CF4', 'PFC-116', 'PFC-218', 'PFC-318', 'C4F10', 'C6F14', 'SF6'] 

    attributes = {'data_owner': 'Paul Krummel', 'data_owner_email': 'paul.krummel@csiro.au', 
                    'inlet_height_magl': '75m_4', 
                    'comment': {'GCMD': 'Gas chromatograph measurements. Output from GCWerks.',
                    'GCMS': 'Gas chromatograph-mass spectrometer measurements. Output from GCWerks.', 
                    'medusa': 'Medusa measurements. Output from GCWerks. See Miller et al. (2008).'}}

    assert data["NF3"]["attributes"] == attributes


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

# TODO - write a new test for this function  
def test_split(data_path, precision_path):
    site = "CGO"
    instrument = "GCMD"

    data_path = Path(__file__).resolve().parent.joinpath("../data/proc_test_data/GC/test_split_data.hdf")
   # Load in the test data
    df = pd.read_hdf(data_path)

    species = ['NF3', 'CF4', 'PFC-116', 'PFC-218', 'PFC-318', 'C4F10', 'C6F14', 'SF6', 'SO2F2', 'SF5CF3', 
               'HFC-23', 'HFC-32', 'HFC-125', 'HFC-134a', 'HFC-143a', 'HFC-152a', 'HFC-227ea', 'HFC-236fa', 
               'HFC-245fa', 'HFC-365mfc', 'HFC-4310mee', 'HCFC-22', 'HCFC-123', 'HCFC-124', 'HCFC-132b', 'HCFC-133a', 'HCFC-141b',
               'HCFC-142b', 'CFC-11', 'CFC-12', 'CFC-13', 'CFC-112', 'CFC-113', 'CFC-114', 'CFC-115', 'H-1211', 'H-1301', 'H-2402', 
               'CH3Cl', 'CH3Br', 'CH3I', 'CH2Cl2', 'CHCl3', 'CCl4', 'CH2Br2', 'CHBr3', 'CH3CCl3', 'TCE', 'PCE', 'ethyne', 'ethene', 
               'ethane', 'propane', 'c-propane', 'benzene', 'toluene', 'COS', 'desflurane']

    metadata = {"foo": "bar"}
    
    gc = GC.load()

    data = gc.split_species(data=df, site=site, instrument=instrument, species=species, metadata=metadata)

    sorted_species = ['C4F10', 'C6F14', 'CCl4', 'CF4', 'CFC-11',
                      'CFC-112', 'CFC-113', 'CFC-114', 'CFC-115', 'CFC-12']

    assert len(data) == 56

    assert sorted(list(data.keys()))[:10] == sorted_species


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
