from typing import Dict, List, Union
from pathlib import Path


def standardise_surface(filepaths: Union[str, Path, List[Union[str, Path]]], metadata: Dict) -> Dict:
    """Standardise data

    Args:
        filepaths: Path of file(s) to process
        data_type: Type of data i.e. GCWERKS, CRDS, ICOS
        metadata: Dictionary of associated metadata, note that this metadata must apply to each of the files
        given in filepaths.
    Returns:
        dict: Details confirmation of process
    """
    import gzip
    from openghg.cloud import call_function
    from openghg.util import hash_bytes

    # pass
    # If under specific size just post to standardisation function
    # else get PAR and upload and then get it to standardise that data?
    if not isinstance(filepaths, list):
        filepaths = [filepaths]

    # To convert bytes to megabytes
    MB = 1e6
    # The largest file we'll just directly POST to the standardisation
    # function will be this big (megabytes)
    post_limit = 20

    for fpath in filepaths:
        filepath = Path(fpath)
        # Get the file size in megabytes
        file_size = filepath.stat().st_size / MB

        if file_size < post_limit:
            # Read the file, compress it and send the data
            file_data = filepath.read_bytes()
            # Here we want the hash of the uncompressed data
            sha1_hash = hash_bytes(data=file_data)
            compressed_data = gzip.compress(data=file_data)

            file_metadata = {"compressed": True, "sha1_hash": sha1_hash, "filename": filepath.name}
            to_post = {"data": compressed_data, "metadata": metadata, "file_metadata": file_metadata}
        else:
            # If we want chunked uploading what do we do?
            raise NotImplementedError
            # tmp_dir = tempfile.TemporaryDirectory()
            # compressed_filepath = Path(tmp_dir.name).joinpath(f"{filepath.name}.tar.gz")
            # # Compress in place and then upload
            # with tarfile.open(compressed_filepath, mode="w:gz") as tar:
            #     tar.add(filepath)
            # compressed_data = compressed_filepath.read_bytes()

    response = call_function(fn_name="standardise", data=to_post)

    return response


# def upload(filepath: Optional[Union[str, Path]] = None, data: Optional[bytes] = None) -> None:
#     """Upload a file to the object store

#     Args:
#         filepath: Path of file to upload
#     Returns:
#         None
#     """
#     from gzip import compress
#     import tempfile
#     from openghg.objectstore import PAR
#     from openghg.client import get_function_url, get_auth_key

#     auth_key = get_auth_key()
#     fn_url = get_function_url(fn_name="get_par")
#     # First we need to get a PAR to write the data

#     response = _post(url=fn_url, auth_key=auth_key)
#     par_json = response.content

#     par = PAR.from_json(json_str=par_json)
#     # Get the URL to upload data to
#     par_url = par.uri

#     if filepath is not None and data is None:
#         filepath = Path(filepath)
#         MB = 1e6
#         file_size = Path("somefile.txt").stat().st_size / MB

#         mem_limit = 50  # MiB
#         if file_size < mem_limit:
#             # Read the file, compress it and send the data
#             file_data = filepath.read_bytes()
#             compressed_data = compress(data=file_data)
#         else:
#             tmp_dir = tempfile.TemporaryDirectory()
#             compressed_filepath = Path(tmp_dir.name).joinpath(f"{filepath.name}.tar.gz")
#             # Compress in place and then upload
#             with tarfile.open(compressed_filepath, mode="w:gz") as tar:
#                 tar.add(filepath)

#             compressed_data = compressed_filepath.read_bytes()
#     elif data is not None and filepath is None:
#         compressed_data = gzip.compress(data)
#     else:
#         raise ValueError("Either filepath or data must be passed.")

#     # Write the data to the object store
#     put_response = _put(url=par_url, data=compressed_data, auth_key=auth_key)

#     print(str(put_response))
