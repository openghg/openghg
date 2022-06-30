import os

cloud_env_var = os.environ.get("OPENGHG_CLOUD", False)

if cloud_env_var:
    cloud_env = bool(int(cloud_env_var))
else:
    cloud_env = False

if cloud_env:
    from ._oci_store import (
        exists,
        get_all_object_names,
        get_object,
        get_object_from_json,
        set_object,
        set_object_from_file,
        set_object_from_json,
        get_bucket,
        delete_object,
        create_bucket,
        upload,
        create_par,
        delete_par,
    )

    from ._par import PAR
else:
    from ._local_store import (
        delete_object,
        exists,
        get_all_object_names,
        get_bucket,
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

from ._encoding import get_datetime_now, datetime_to_datetime, string_to_bytes, bytes_to_string
