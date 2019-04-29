import pytest
import numpy as np
import pandas as pd
import xarray as xr
import os
import shutil
import glob

import hashlib

from Acquire.ObjectStore import ObjectStore, ObjectStoreError
from Acquire.Service import get_service_account_bucket, \
    push_is_running_service, pop_is_running_service

from data_processing import hugs_objstore

test_dir = os.path.dirname(os.path.abspath(__file__))


# @pytest.fixture(scope="session")
# def bucket(tmpdir_factory):
#     d = tmpdir_factory.mktemp("simple_objstore")
#     push_is_running_service()
#     bucket = get_service_account_bucket(str(d))
#     pop_is_running_service()
#     return bucket


# def test_raw_data_store():
#     # Get a bucket to store the data
#     test_bucket = ObjectStore.create_bucket(bucket, "test_bucket")

#     # test_path = os.path.dirname(__file__)
#     # raw_data_path = "data/proc_test_data/CRDS/bsd.picarro.1minute.248m.dat"

#     # filepath = os.path.join(test_path, raw_data_path)

#     # fname, size, md5 = objstore.store_raw_data(filepath)

#     assert True

     




    
