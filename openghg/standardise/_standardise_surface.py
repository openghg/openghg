from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from pandas import Timedelta

from openghg.store import ObsSurface
from openghg.objectstore import get_writable_bucket
from openghg.cloud import create_file_package
from openghg.util import running_on_hub
from openghg.types import optionalPathType, multiPathType


def standardise_surface(
    filepaths: Union[str, Path, List, Tuple],
    source_format: str,
    site: str,
    network: str,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    calibration_scale: Optional[str] = None,
    site_filepath: optionalPathType = None,
    overwrite: bool = False,
    store: Optional[str] = None,
) -> Optional[Dict]:
    """Standardise surface measurements and store the data in the object store.

    Args:
        filepaths: Path of file(s) to process
        source_format: Format of data i.e. GCWERKS, CRDS, ICOS
        site: Site code
        network: Network name
        inlet: Inlet height in metres
        instrument: Instrument name
        sampling_period: Sampling period as pandas time code, e.g. 1m for 1 minute, 1h for 1 hour
        site_filepath: Alternative site info file (see openghg/supplementary_data repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
        overwrite: Overwrite data currently present in the object store
        store: Name of object store to write to, required if user has access to more than one
        writable store
    Returns:
        dict: Dictionary containing confirmation of standardisation process.
    """
    from openghg.cloud import call_function

    if not isinstance(filepaths, list):
        filepaths = [filepaths]

    if running_on_hub():
        # TODO: Use input for site_filepath here? How to include this?

        # To convert bytes to megabytes
        MB = 1e6
        # The largest file we'll just directly POST to the standardisation
        # function will be this big (megabytes)
        post_limit = 40  # MB

        metadata = {}
        metadata["site"] = site
        metadata["source_format"] = source_format
        metadata["network"] = network
        metadata["data_type"] = "surface"

        if inlet is not None:
            metadata["inlet"] = inlet
        if instrument is not None:
            metadata["instrument"] = instrument
        if sampling_period is not None:
            metadata["sampling_period"] = sampling_period

        responses = {}
        for fpath in filepaths:
            gcwerks = False
            if source_format.lower() in ("gc", "gcwerks"):
                metadata["source_format"] = "gcwerks"

                try:
                    filepath = Path(fpath[0])
                except TypeError:
                    raise TypeError("We require both data and precision files for GCWERKS data.")
                else:
                    gcwerks = True
            else:
                filepath = Path(fpath)

            compressed_data, file_metadata = create_file_package(filepath=filepath, obs_type="surface")
            compressed_size = len(compressed_data) / MB

            if compressed_size > post_limit:
                raise NotImplementedError("Compressed size over 40 MB, not currently supported.")

            to_post = {
                "function": "standardise",
                "data": compressed_data,
                "metadata": metadata,
                "file_metadata": file_metadata,
            }

            if gcwerks:
                precision_filepath = Path(fpath[1])
                compressed_prec, prec_file_metadata = create_file_package(
                    filepath=precision_filepath, obs_type="surface"
                )

                to_post["precision_data"] = compressed_prec
                to_post["precision_file_metadata"] = prec_file_metadata

            # else:
            # If we want chunked uploading what do we do?
            # raise NotImplementedError
            # tmp_dir = tempfile.TemporaryDirectory()
            # compressed_filepath = Path(tmp_dir.name).joinpath(f"{filepath.name}.tar.gz")
            # # Compress in place and then upload
            # with tarfile.open(compressed_filepath, mode="w:gz") as tar:
            #     tar.add(filepath)
            # compressed_data = compressed_filepath.read_bytes()

            fn_response = call_function(data=to_post)

            responses[filepath.name] = fn_response["content"]

        return responses
    else:
        return _local_standardise_surface(
            filepath=filepaths,
            source_format=source_format,
            site=site,
            network=network,
            instrument=instrument,
            sampling_period=sampling_period,
            inlet=inlet,
            site_filepath=site_filepath,
            overwrite=overwrite,
            store=store,
        )


def _local_standardise_surface(
    filepath: multiPathType,
    source_format: str,
    network: str,
    site: str,
    inlet: Optional[str] = None,
    height: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[Union[Timedelta, str]] = None,
    calibration_scale: Optional[str] = None,
    measurement_type: str = "insitu",
    overwrite: bool = False,
    verify_site_code: bool = True,
    site_filepath: optionalPathType = None,
    store: Optional[str] = None,
):
    """The local version of standardise_surface

    TODO - do we want to pass all these args in?

    Args:
        filepath: Filepath(s)
        source_format: Data format, for example CRDS, GCWERKS
        site: Site code/name
        network: Network name
        inlet: Inlet height. Format 'NUMUNIT' e.g. "10m".
            If retrieve multiple files pass None, OpenGHG will attempt to
            extract this from the file.
        height: Alias for inlet.
        read inlets from data.
        instrument: Instrument name
        sampling_period: Sampling period in pandas style (e.g. 2H for 2 hour period, 2m for 2 minute period).
        measurement_type: Type of measurement e.g. insitu, flask
        overwrite: Overwrite previously uploaded data
        verify_site_code: Verify the site code
        site_filepath: Alternative site info file (see openghg/supplementary_data repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
        store: Name of object store to write to, required if user has access to more than one
        writable store
    Returns:
        dict: Dictionary of result data

    TODO - do we still want to return this dictionary of UUIDs the user might not care about?
    """
    bucket = get_writable_bucket(name=store)

    with ObsSurface(bucket=bucket) as obs:
        results = obs.read_file(
            filepath=filepath,
            source_format=source_format,
            network=network,
            site=site,
            inlet=inlet,
            height=height,
            instrument=instrument,
            sampling_period=sampling_period,
            calibration_scale=calibration_scale,
            measurement_type=measurement_type,
            overwrite=overwrite,
            verify_site_code=verify_site_code,
            site_filepath=site_filepath,
        )

    return results
