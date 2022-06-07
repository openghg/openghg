from typing import Dict, Optional, Union
from io import BytesIO
from pathlib import Path
from oci.object_storage import ObjectStorageClient
from oci.response import Response
from openghg.types import ObjectStoreError
import json

# from functools import lru_cache

# def _progress_callback(bytes_uploaded):
#     print("{} additional bytes uploaded".format(bytes_uploaded))

# Cache this?
# @lru_cache()
# def _get_objectstore_client():
#     oci_config = _load_config()
#     object_storage = ObjectStorageClient(config=oci_config)
#     return object_storage


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


def _load_config() -> Dict:
    """Loads the OCI Config for accessing the object store from memory

    Returns:
        dict: Config as dictionary
    """
    from json import loads
    import os

    try:
        return loads(os.environ["OCI_CONFIG"])
    except KeyError:
        raise ValueError(
            "Please ensure the oci config file is stored as a secret in JSON format for this Fn app."
        )


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


def get_bucket(bucket: str) -> Dict:
    """Get a bucket within the tenancy

    Args:
        bucket: Name of bucket
    Returns:
        dict: Bucket data
    Raises:
        ObjectStoreError: If bucket not found
    """
    from openghg.types import ObjectStoreError

    oci_config = _load_config()

    object_storage = ObjectStorageClient(config=oci_config)

    namespace = object_storage.get_namespace().data

    try:
        bucket = object_storage.get_bucket(namespace_name=namespace, bucket=bucket)
    except Exception as e:
        raise ObjectStoreError(f"Error retrieving bucket {bucket}: {str(e)}")

    return bucket


def create_bucket(bucket: str) -> Dict:
    """Create a bucket"""
    from oci.object_storage.models import CreateBucketDetails

    oci_config = _load_config()
    object_storage = ObjectStorageClient(config=oci_config)
    namespace = object_storage.get_namespace().data

    try:
        compartment_id = oci_config["tenancy"]
    except KeyError:
        raise KeyError("Can't read compartment_id from tenancy in config file.")

    request = CreateBucketDetails()
    request.compartment_id = compartment_id
    request.name = bucket

    bucket = object_storage.create_bucket(namespace, request)

    return bucket


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
    from oci.object_storage import ObjectStorageClient
    from oci.object_storage import UploadManager
    from oci.object_storage.transfer.constants import MEBIBYTE

    # See
    # https://github.com/oracle/oci-python-sdk/blob/master/examples/multipart_object_upload.py

    oci_config = _load_config()
    object_storage = ObjectStorageClient(config=oci_config)

    upload_manager = UploadManager(
        object_storage_client=object_storage, allow_parallel_uploads=True, parallel_process_count=3
    )

    namespace = object_storage.get_namespace().data

    if data is not None and filepath is None:
        byte_stream = BytesIO(data)

        response = upload_manager.upload_stream(
            namespace_name=namespace, bucket=bucket, object_name=key, stream_ref=byte_stream
        )
    elif filepath is not None and data is None:
        # We'll upload in 10 MiB chunks
        part_size = 10 * MEBIBYTE

        response = upload_manager.upload_file(
            namespace_name=namespace,
            bucket=bucket,
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
    from oci.object_storage.models import CreatePreauthenticatedRequestDetails
    from datetime import timedelta
    from uuid import uuid4
    from openghg.objectstore import get_datetime_now, PAR

    oci_config = _load_config()
    object_storage = ObjectStorageClient(config=oci_config)

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
    # Get the namespace
    namespace = object_storage.get_namespace().data

    if not is_bucket:
        request.object_name = key

    response = object_storage.create_preauthenticated_request(
        namespace_name=namespace, bucket=bucket, create_preauthenticated_request_details=request
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
    oci_config = _load_config()
    object_storage = ObjectStorageClient(config=oci_config)

    namespace = object_storage.get_namespace().data

    response = object_storage.delete_preauthenticated_request(
        namespace_name=namespace, bucket=bucket, par_id=par_id
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

    oci_config = _load_config()
    object_storage = ObjectStorageClient(config=oci_config)
    namespace = object_storage.get_namespace().data

    try:
        object_storage.delete_object(namespace_name=namespace, bucket=bucket, object_name=key)
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

    oci_config = _load_config()
    object_storage = ObjectStorageClient(config=oci_config)
    namespace = object_storage.get_namespace().data

    try:
        data = object_storage.get_object(namespace_name=namespace, bucket=bucket, object_name=key)
    except Exception:
        raise ObjectStoreError(f"Unable to get object at {key} in bucket {bucket}")

    return data


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

    oci_config = _load_config()
    object_storage = ObjectStorageClient(config=oci_config)
    namespace = object_storage.get_namespace().data

    data_buf = BytesIO(data)

    try:
        object_storage.put_object(
            namespace_name=namespace, bucket=bucket, object_name=key, put_object_body=data_buf
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
    filepath = Path(filename)
    data = filepath.read_bytes()
    set_object(bucket=bucket, key=key, data=data)
