import sys

from ._local_store import (
    delete_object,
    get_hugs_local_path,
    get_all_object_names,
    get_object_names,
    get_bucket,
    get_local_bucket,
    get_object,
    set_object,
    set_object_from_json,
    set_object_from_file,
    get_object_from_json,
    exists,
    query_store
)

if sys.version_info.major < 3:
    raise ImportError("HUGS requires Python 3.6 minimum")

if sys.version_info.minor < 6:
    raise ImportError("HUGS requires Python 3.6 minimum")


# from ._hugs_objstore import (delete_object, exists, get_abs_filepaths, get_bucket,
#                              get_object, get_object_from_json, get_md5,
#                              get_md5_bytes, get_object, get_local_bucket, set_object_from_json,
#                              set_object_from_file, get_object_names, hash_files, query_store)
