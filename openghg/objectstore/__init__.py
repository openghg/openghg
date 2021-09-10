import sys as _sys
import os as _os

cloud_env = _os.environ.get("OPENGHG_CLOUD")

if cloud_env is not None:
    from ._openghg_objstore import (
        delete_object,
        exists,
        get_abs_filepaths,
        get_bucket,
        get_object_from_json,
        get_md5,
        get_md5_bytes,
        get_object,
        get_local_bucket,
        set_object_from_json,
        set_object_from_file,
        hash_files,
    )
else:
    from ._local_store import (
        delete_object,
        get_openghg_local_path,
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
        query_store,
        visualise_store,
    )
