# cloud_env = os.environ.get("OPENGHG_CLOUD", False)

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
