# Testing the GC class
import pytest
import pandas as pd
import os

from modules import GC

def test_read_file():
    datafile = "capegrim-medusa.18.C"
    datafile_precision = "capegrim-medusa.18.precisions.C"

    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/GC"
    data_filepath = os.path.join(dir_path, test_data, datafile)
    precision_filepath = os.path.join(dir_path, test_data, datafile_precision)

    gc = GC.read_file(data_filepath=data_filepath, precision_filepath=precision_filepath)

    print(gc._uuid)

    assert False
