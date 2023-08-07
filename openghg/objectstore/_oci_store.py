import json
from functools import lru_cache
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Optional, Union

try:
    from oci.config import validate_config
    from oci.object_storage import ObjectStorageClient, UploadManager
    from oci.object_storage.models import CreateBucketDetails
    from oci.object_storage.transfer.constants import MEBIBYTE
    from oci.response import Response
except ImportError:
    raise ImportError("To use the OCI object store you must install oci.")

from openghg.types import ObjectStoreError


def _clean_key(key: str) -> str:
    """This function cleans and returns a key so that it is suitable
    for use both as a key and a directory/file path
    e.g. it removes double-slashes

    Args:
         key (str): Key to clean
    Returns:
         str: Cleaned key

    """
    from os.path import normpath

    key = normpath(key)

    if len(key) > 1024:
        raise ObjectStoreError(
            f"The object store does not support keys with longer than 1024 characters ({len(key)}) - {key}"
        )

    return key


# The results of the functions below won't change during a function invocation so
# we'll cache the results
@lru_cache()
def _load_config() -> Dict:
    """Loads the OCI Config for accessing the object store from memory

    Returns:
        dict: Config as dictionary
    """
    import os
    from json import loads
    from pathlib import Path

    from cryptography.fernet import Fernet
    from openghg.objectstore import string_to_bytes

    try:
        encrypted_config = os.environ["SECRET_CONFIG"]
    except KeyError:
        raise ValueError(
            "Cannot read SECRET_CONFIG environment variable. Please setup the correct function secrets."
        )

    decoded = string_to_bytes(encrypted_config)
    fernet_key = Path("fernet_key").read_bytes()
    decrypted = Fernet(fernet_key).decrypt(decoded)

    config = loads(decrypted)

    key_path = Path("/tmp/key.pem")
    key_data = string_to_bytes(config["key_data"])
    key_path.write_bytes(key_data)

    tenancy_data = config["tenancy"]
    tenancy_data["key_file"] = str(key_path)
    tenancy_data["pass_phrase"] = config["passphrase"]

    validate_config(config=tenancy_data)

    return tenancy_data


@lru_cache
def _load_client() -> ObjectStorageClient:
    """Creates an ObjectStorageClient object using the in memory
    configuration data

    Returns:
        ObjectStorageClient: Object storage client
    """
    oci_config = _load_config()
    return ObjectStorageClient(config=oci_config)


@lru_cache
def _get_namespace() -> str:
    """Get the Object Store client's namespace

    Returns:
        str: Object store namespace
    """
    return _load_client().get_namespace().data


def _create_full_uri(uri: str, region: str):
    """Internal function used to get the full URL to the passed PAR URI
    for the specified region. This has the format;

    https://objectstorage.{region}.oraclecloud.com/{uri}

    Args:
         uri: URI of PAR
         region: Region for cloud service
    Returns:
         str: Full URI for client data read / write
    """
    server = f"https://objectstorage.{region}.oraclecloud.com"

    while uri.startswith("/"):
        uri = uri[1:]

    return f"{server}/{uri}"


def get_bucket(name: Optional[str] = None) -> str:
    """Get the default bucket to use for OpenGHG storage.
    If an OPENGHG_BUCKET environment variable isn't set
    it defaults to openghg_storage.

    Args:
        name: Name of bucket
    Returns:
        str: Bucket name
    """
    from os import getenv

    return getenv(key="OPENGHG_BUCKET", default="openghg_storage")


def _get_oci_bucket(bucket_name: str) -> Dict:
    """Get a bucket within the tenancy

    Args:
        bucket: Name of bucket
    Returns:
        dict: Bucket data
    Raises:
        ObjectStoreError: If bucket not found
    """
    from openghg.types import ObjectStoreError

    object_storage = _load_client()
    namespace = _get_namespace()

    try:
        bucket = object_storage.get_bucket(namespace_name=namespace, bucket_name=bucket_name)
    except Exception as e:
        raise ObjectStoreError(f"Error retrieving bucket {bucket}: {str(e)}")

    return bucket


def create_bucket(bucket: str) -> Dict:
    """Create a bucket"""

    oci_config = _load_config()
    object_storage = _load_client()
    namespace = _get_namespace()

    try:
        compartment_id = oci_config["tenancy"]
    except KeyError:
        raise KeyError("Can't read compartment_id from tenancy in config file.")

    request = CreateBucketDetails()
    request.compartment_id = compartment_id
    request.name = bucket

    bucket = object_storage.create_bucket(namespace_name=namespace, create_bucket_details=request)

    return bucket


def get_all_object_names(bucket: str, prefix: Optional[str] = None, without_prefix: bool = False) -> List:
    """Returns the names of all objects in the passed bucket

    Args:
         bucket (dict): Bucket containing data
         prefix (str): Prefix for data
    Returns:
         list: List of all objects in bucket

    """
    if prefix is not None:
        prefix = _clean_key(prefix)

    object_storage = _load_client()
    namespace = _get_namespace()

    objects = object_storage.list_objects(namespace_name=namespace, bucket_name=bucket, prefix=prefix)
    bucket_objects = objects.data.objects

    if without_prefix:
        prefix_len = len(prefix)

    names = []
    for obj in bucket_objects:
        if prefix is not None:
            if obj.name.startswith(prefix):
                name = obj.name
        else:
            name = obj.name

        while name.endswith("/"):
            name = name[0:-1]

        while name.startswith("/"):
            name = name[1:]

        if without_prefix:
            name = name[prefix_len:]

            while name.startswith("/"):
                name = name[1:]

        if len(name) > 0:
            names.append(name)

    return names


def exists(bucket: str, key: str) -> bool:
    """Checks if there is an object in the object store with the given key

    Args:
        bucket: Bucket containing data
        key: Prefix for key in object store
    Returns:
        bool: True if key exists in store
    """
    names = get_all_object_names(bucket=bucket, prefix=key)

    return len(names) > 0


def upload(
    bucket: str, key: str, data: Optional[bytes] = None, filepath: Optional[Union[str, Path]] = None
) -> Response:
    """Uploads data to the object store

    Args:
        bucket: Name of bucket
        key: Key at which to store data
        data: Binary data
    Returns:
        oci.response.Response: Response from OCIs
    """
    # See
    # https://github.com/oracle/oci-python-sdk/blob/master/examples/multipart_object_upload.py

    object_storage = _load_client()
    namespace = _get_namespace()

    upload_manager = UploadManager(
        object_storage_client=object_storage, allow_parallel_uploads=True, parallel_process_count=3
    )

    if data is not None and filepath is None:
        byte_stream = BytesIO(data)

        response = upload_manager.upload_stream(
            namespace_name=namespace, bucket_name=bucket, object_name=key, stream_ref=byte_stream
        )
    elif filepath is not None and data is None:
        # We'll upload in 10 MiB chunks
        part_size = 10 * MEBIBYTE

        response = upload_manager.upload_file(
            namespace_name=namespace,
            bucket_name=bucket,
            object_name=key,
            file_path=filepath,
            part_size=part_size,
            # progress_callback=progress_callback,
        )
    else:
        raise ValueError("Only one of data or filepath can be passed.")

    if response.status != 200:
        raise ValueError("Unable to upload file to object store.")

    return response


def create_par(
    bucket: str,
    key: str,
    readable: bool = True,
    writeable: bool = True,
    validity: int = 3600,
    user_id: Optional[str] = None,
) -> None:
    """Create a PAR for client data upload

    Args:
        user_id: User ID string
        readable: Allow read
        writeable: Allow write
        validity: Validity of PAR in seconds
    Returns:
        PAR: OpenGHG wrapper of OSPar
    """
    from datetime import timedelta
    from uuid import uuid4

    from oci.object_storage.models import CreatePreauthenticatedRequestDetails
    from openghg.objectstore import PAR, get_datetime_now

    oci_config = _load_config()
    object_storage = _load_client()
    namespace = _get_namespace()

    #  We'll need the region to create the URL for data upload later
    region = oci_config["region"]

    expires_datetime = get_datetime_now() + timedelta(seconds=validity)

    request = CreatePreauthenticatedRequestDetails(time_expires=expires_datetime)

    is_bucket = key is None

    if is_bucket:
        request.access_type = "AnyObjectWrite"
    if readable and writeable:
        request.access_type = "ObjectReadWrite"
    elif readable:
        request.access_type = "ObjectRead"
    elif writeable:
        request.access_type = "ObjectWrite"

    # Give the PAR an ID that we can later use to delete it
    # How should we store this? Check how PARRegistry worked in Acquire
    request.name = f"client-par-{str(uuid4())}"

    if not is_bucket:
        request.object_name = key

    response = object_storage.create_preauthenticated_request(
        namespace_name=namespace, bucket_name=bucket, create_preauthenticated_request_details=request
    )

    if response.status != 200:
        raise ObjectStoreError(
            f"Unable to create PAR, {str(request)}. Status: {response.status}. Error: {response.data}"
        )

    par_data = response.data
    bare_uri = par_data.access_uri

    uri = _create_full_uri(uri=bare_uri, region=region)
    par_id = par_data.id
    par_name = par_data.name
    time_created = par_data.time_created
    time_expires = par_data.time_expires

    client_par = PAR(
        uri=uri, par_id=par_id, par_name=par_name, time_created=time_created, time_expires=time_expires
    )

    return client_par.to_json()


def delete_par(par_id: str, bucket: str) -> None:
    """Delete a PAR to prevent its future use

    Args:
        par_id: ID of PAR
    Returns:
        None
    """
    object_storage = _load_client()
    namespace = _get_namespace()

    response = object_storage.delete_preauthenticated_request(
        namespace_name=namespace, bucket_name=bucket, par_id=par_id
    )

    if response.status != 200:
        raise ObjectStoreError("Unable to delete PAR")


def delete_object(bucket: str, key: str) -> None:
    """Delete object at key

    Args:
        bucket: Name of bucket
        key: Key to object in object store
    Returns:
        None
    """
    key = _clean_key(key=key)

    object_storage = _load_client()
    namespace = _get_namespace()

    try:
        object_storage.delete_object(namespace_name=namespace, bucket_name=bucket, object_name=key)
    except Exception:
        raise ObjectStoreError(f"Unable to get object at {key} in bucket {bucket}")


def get_object(bucket: str, key: str) -> bytes:
    """Get object from a key in the object store

    Args:
        bucket: Name of bucket
        key: Key to object in object store
    Returns:
        bytes: Binary data
    """
    key = _clean_key(key=key)

    object_storage = _load_client()
    namespace = _get_namespace()

    try:
        response = object_storage.get_object(namespace_name=namespace, bucket_name=bucket, object_name=key)
    except Exception:
        raise ObjectStoreError(f"Unable to get object at {key} in bucket {bucket}")

    binary_data = response.data.content

    return binary_data


def get_object_from_json(bucket: str, key: str) -> Dict[str, Union[str, Dict]]:
    """Return an object constructed from JSON stored at key.

    Args:
        bucket: Bucket containing data
        key: Key for data in bucket
    Returns:
        dict: Dictionary
    """
    data = get_object(bucket, key).decode("utf-8")
    data_dict: Dict = json.loads(data)

    return data_dict


def set_object(bucket: str, key: str, data: bytes) -> None:
    """Store data in the object store

    Args:
        bucket: Name of bucket
        key: Key to object in object store
        data: Data to store
    Returns:
        None
    """
    from io import BytesIO

    key = _clean_key(key=key)

    object_storage = _load_client()
    namespace = _get_namespace()

    data_buf = BytesIO(data)

    try:
        object_storage.put_object(
            namespace_name=namespace, bucket_name=bucket, object_name=key, put_object_body=data_buf
        )
    except Exception:
        raise ObjectStoreError(f"Unable to store object at {key} in bucket {bucket}")


def set_object_from_json(bucket: str, key: str, data: Union[str, Dict]) -> None:
    """Store a string in the object store

     Args:
        bucket: Name of bucket
        key: Key to object in object store
        data: JSON data
    Returns:
        None
    """
    data_bytes = json.dumps(data).encode("utf-8")
    set_object(bucket=bucket, key=key, data=data_bytes)


def set_object_from_file(bucket: str, key: str, filename: Union[str, Path]) -> None:
    """Store the contents of a file in the object store

     Args:
        bucket: Name of bucket
        key: Key to object in object store
        data: JSON data
    Returns:
        None
    """
    data = Path(filename).read_bytes()
    set_object(bucket=bucket, key=key, data=data)
