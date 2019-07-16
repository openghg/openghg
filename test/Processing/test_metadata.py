import pandas as pd
import pytest
import os

from HUGS.Processing import read_metadata

def test_parse_CRDS():
    filename = "hfd.picarro.1minute.100m_min.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    data = pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+") 

    metadata = read_metadata(filename=filename, data=data, data_type="CRDS")

    assert metadata["site"] == "hfd"
    assert metadata["instrument"] == "picarro"
    assert metadata["time_resolution"] == "1_minute"
    assert metadata["port"] == "10"
    assert metadata["type"] == "air"

def test_parse_GC():
    filename = "capegrim-medusa.18.C"

    metadata = read_metadata(filename=filename, data=None, data_type="GC")

    assert metadata["site"] == "capegrim"
    assert metadata["instrument"] == "medusa"
