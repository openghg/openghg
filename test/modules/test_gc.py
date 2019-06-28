# Testing the GC class
import pytest
import pandas as pd
import os

from HUGS.Modules import GC

@pytest.fixture(scope="session")
def data_path():
    return os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "../data/proc_test_data/GC/capegrim-medusa.18.C"

@pytest.fixture(scope="session")
def precision_path():
    return os.path.dirname(os.path.abspath(__file__)) + os.path.sep + "../data/proc_test_data/GC/capegrim-medusa.18.precisions.C"


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

    precision_head = precision.head(1)

    print(precision_head)
    print("Fix dtype for reading of GC data")

    assert False
    
    


    


