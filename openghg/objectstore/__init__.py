import os

cloud_env = os.environ.get("OPENGHG_CLOUD", False)

# if cloud_env:
from ._my_oci import create_bucket, upload, create_par
from ._par import PAR

# else:
from ._local_store import (
    delete_object,
    exists,
    get_all_object_names,
    get_bucket,
    get_local_bucket,
    get_object,
    get_object_from_json,
    get_object_names,
    get_openghg_local_path,
    query_store,
    set_object,
    set_object_from_file,
    set_object_from_json,
    visualise_store,
)

from ._encoding import get_datetime_now, datetime_to_datetime


# from ._cloud import OCI_ObjectStore

# if cloud_env:
#     from ._openghg_objstore import (
#         delete_object,
#         exists,
#         get_abs_filepaths,
#         get_bucket,
#         get_local_bucket,
#         get_md5,
#         get_md5_bytes,
#         get_object,
#         get_object_from_json,
#         hash_files,
#         set_object_from_file,
#         set_object_from_json,
#     )
# else:
