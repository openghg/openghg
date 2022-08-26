from openghg.util import running_locally

cloud_env = not running_locally()

if cloud_env:
    from ._oci_store import (
        create_bucket,
        create_par,
        delete_object,
        delete_par,
        exists,
        get_all_object_names,
        get_bucket,
        get_object,
        get_object_from_json,
        set_object,
        set_object_from_file,
        set_object_from_json,
        upload,
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

from ._encoding import bytes_to_string, datetime_to_datetime, get_datetime_now, string_to_bytes
