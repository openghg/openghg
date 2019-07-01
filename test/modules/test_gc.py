# Testing the GC class
import pytest
import pandas as pd
import os
import uuid

from Acquire.ObjectStore import datetime_to_string
from HUGS.Modules import GC
from HUGS.ObjectStore import get_local_bucket
from HUGS.ObjectStore import get_object_names
from HUGS.Util import get_datetime_epoch

@pytest.fixture(scope="session")
def data_path():
    return os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "../data/proc_test_data/GC/capegrim-medusa.18.C"

@pytest.fixture(scope="session")
def precision_path():
    return os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "../data/proc_test_data/GC/capegrim-medusa.18.precisions.C"

@pytest.fixture
def gc():
    gc = GC.create()
    gas_data = gc.read_data(data_filepath=data_path, precision_filepath=precision_path, site=site, instrument=instrument)

mocked_uuid = "10000000-0000-0000-00000-000000000001"

@pytest.fixture
def mock_uuid(monkeypatch):
    def mock_uuid():
        return mocked_uuid

    monkeypatch.setattr(uuid, 'uuid4', mock_uuid)

@pytest.fixture
def gc(mock_uuid):
    gc = GC.create()
    gc.add_instrument(instrument_id="2001", value=datetime_to_string(get_datetime_epoch()))
    return gc


def test_read_file(data_path, precision_path):
    gc = GC.read_file(data_filepath=data_path, precision_filepath=precision_path)

    header = gc._proc_data.head(1)
    assert header["Year"].iloc[0] == 2018.000046
    assert header["propane repeatability"].iloc[0] == 0.22325
    assert header["c-propane repeatability"].iloc[0] == 0.10063
    assert header["benzene repeatability"].iloc[0] == 0.01107

def test_read_data(data_path, precision_path):
    # Capegrim
    site = "CGO"
    instrument = "GCMD"

    gc = GC.create()
    gas_data = gc.read_data(data_filepath=data_path, precision_filepath=precision_path, site=site, instrument=instrument)

    species, metadata, uuid, data = gas_data[0]

    head_data = data.head(1)

    assert species == "NF3"
    assert metadata["inlet"] == "75m_4"
    assert metadata["species"] == "NF3"
    assert head_data["NF3"].iloc[0] == pytest.approx(1.603)
    assert head_data["NF3 repeatability"].iloc[0] == pytest.approx(0.02531)
    assert head_data["NF3 status_flag"].iloc[0] == 0
    assert head_data["NF3 integration_flag"].iloc[0] == 0
    assert head_data["Inlet"].iloc[0] == "75m_4"

def test_read_precision(precision_path):
    gc = GC.create()

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
    site = "CGO"
    instrument = "GCMD"

    gc = GC.create()
    gc.read_data(data_filepath=data_path, precision_filepath=precision_path, site=site, instrument=instrument)
    gas_data = gc.split(site)

    assert gas_data[0][0] == "NF3"
    assert gas_data[0][1] == {'inlet': '75m_4', 'species': 'NF3'}
    
    head_data = gas_data[0][3].head(1)
    assert head_data["NF3"].iloc[0] == pytest.approx(1.603)
    assert head_data["NF3 repeatability"].iloc[0] == pytest.approx(0.02531)
    assert head_data["NF3 status_flag"].iloc[0] == 0
    assert head_data["NF3 integration_flag"].iloc[0] == 0
    assert head_data["Inlet"].iloc[0] == "75m_4"

def test_to_data(gc):
    data = gc.to_data()

    assert data["uuid"] == mocked_uuid
    assert data["instruments"] == {'2001': '1970-01-01T00:00:00'}
    assert data["stored"] == False

def test_from_data(gc):
    data = gc.to_data()

    gc_new = GC.from_data(data)

    assert gc_new._uuid == mocked_uuid
    assert gc_new._instruments == {'2001': '1970-01-01T00:00:00'}
    assert gc_new._stored == False

def test_save(gc):
    gc.save()

    bucket = get_local_bucket()
    prefix = "%s/uuid/%s" % (GC._gc_root, gc._uuid)
    objs = get_object_names(bucket, prefix)

    assert objs[0].split("/")[-1] == mocked_uuid

def test_load(gc):
    gc.save()

    gc_new = GC.load(uuid=mocked_uuid)

    assert gc_new._uuid == mocked_uuid
    assert gc_new._instruments == {'2001': '1970-01-01T00:00:00'}
    assert gc_new._stored == False

def test_exists(gc):
    gc.save()

    assert GC.exists(uuid=mocked_uuid) == True




# def test_exists():
#     gc = GC.create()
#     uuid = gc._uuid

#     assert False
