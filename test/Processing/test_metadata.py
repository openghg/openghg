import pandas as pd
import pytest
from pathlib import Path

from HUGS.Processing import read_metadata

def test_parse_CRDS():
    filename = "hfd.picarro.1minute.100m_min.dat"

    filepath = Path(__file__).resolve().parent.joinpath("../data/proc_test_data/CRDS/").joinpath(filename)

    data = pd.read_csv(filepath, header=None, skiprows=1, sep=r"\s+") 

    metadata = read_metadata(filepath=filepath, data=data, data_type="CRDS")

    assert metadata["site"] == "hfd"
    assert metadata["instrument"] == "picarro"
    assert metadata["time_resolution"] == "1_minute"
    assert metadata["port"] == "10"
    assert metadata["type"] == "air"

def test_parse_GC():
    filename = "capegrim-medusa.18.C"
    filepath = Path(__file__).resolve().parent.joinpath("../data/proc_test_data/GC/").joinpath(filename)

    metadata = read_metadata(filepath=filepath, data=None, data_type="GC")

    assert metadata["site"] == "capegrim"
    assert metadata["instrument"] == "medusa"
