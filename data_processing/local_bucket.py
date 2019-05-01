

from Acquire.ObjectStore import ObjectStore, use_testing_object_store_backend

import tempfile
import datetime
import shutil
import os

def time_now():
    """ Returns a prettified (maybe) string of the current 
        time and date

        Returns:
            str: A formatted version of datetime.now()
    """

    return datetime.datetime.now().strftime("%Y%m%d_%H:%M")

def local_bucket():
    # Get the path of the user's home directory
    home_path = os.path.expanduser("~")
    hugs_test_buckets = "hugs_tmp/test_buckets"

    local_buckets_dir = os.path.join(home_path, hugs_test_buckets)

    root_bucket = use_testing_object_store_backend(local_buckets_dir)

    bucket = ObjectStore.create_bucket(bucket=root_bucket, bucket_name="hugs")

    return bucket

    # ObjectStore.set_string_object(bucket=bucket, key="test", string_data="value")

    # print(ObjectStore.get_string_object(bucket=bucket, key="test"))


test_bucket()




