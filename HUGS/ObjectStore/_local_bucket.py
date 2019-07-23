import tempfile
import datetime
import shutil
import os

from Acquire.ObjectStore import ObjectStore, use_testing_object_store_backend


def bucket_time():
    """ Returns a prettified (maybe) string of the current
        time and date

        Returns:
            str: A formatted version of datetime.now()
    """
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def get_local_bucket(name=None, empty=False):
    """ Creates and returns a local bucket
        that's created in the user's home directory

        Args:
            name (str, default=None): Extra string to add to bucket name
            empty (bool, default=False): If True return an empty bucket
        Returns:
            dict: Local bucket
    """
    from Acquire.Service import get_service_account_bucket

    # TODO - clean this up
    # try:
    #     return get_service_account_bucket()
    # except:
    # Get the path of the user's home directory
    home_path = os.path.expanduser("~")
    hugs_test_buckets = "hugs_tmp/test_buckets"
    
    local_buckets_dir = os.path.join(home_path, hugs_test_buckets)

    if name:
        hugs_test_buckets += "/%s" % name

    if empty:
        import shutil as _shutil
        import os as _os
        # Remove the directory and recreate
        if os.path.isdir(local_buckets_dir):
            _shutil.rmtree(local_buckets_dir)
            os.makedirs(local_buckets_dir)
        else:
            os.makedirs(local_buckets_dir)

    root_bucket = use_testing_object_store_backend(local_buckets_dir)

    bucket = ObjectStore.create_bucket(bucket=root_bucket, bucket_name="hugs_test")

    return bucket








