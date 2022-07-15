from typing import Dict, List, Optional, Tuple, Union
from pathlib import Path
from openghg.cloud import create_file_package, create_post_dict
from openghg.util import running_in_cloud


def standardise_surface(
    filepaths: Union[str, Path, List, Tuple],
    data_type: str,
    site: str,
    network: str,
    inlet: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    overwrite: bool = False,
) -> Optional[Dict]:
    """Standardise surface measurements and store the data in the object store.

    Args:
        filepaths: Path of file(s) to process
        data_type: Type of data i.e. GCWERKS, CRDS, ICOS
        site: Site code
        network: Network name
        inlet: Inlet height in metres
        instrument: Instrument name
        sampling_period: Sampling period as pandas time code, e.g. 1m for 1 minute, 1h for 1 hour
        overwrite: Overwrite data currently present in the object store
    Returns:
        dict: Dictionary containing confirmation of standardisation process.
    """
    from openghg.cloud import call_function

    if not isinstance(filepaths, list):
        filepaths = [filepaths]

    # To convert bytes to megabytes
    MB = 1e6
    # The largest file we'll just directly POST to the standardisation
    # function will be this big (megabytes)
    post_limit = 40

    cloud = running_in_cloud()

    if cloud:
        metadata = {}
        metadata["site"] = site
        metadata["data_type"] = data_type
        metadata["network"] = network
        metadata["data_type"]

        if inlet is not None:
            metadata["inlet"] = inlet
        if instrument is not None:
            metadata["instrument"] = instrument
        if sampling_period is not None:
            metadata["sampling_period"] = sampling_period

        responses = {}
        for fpath in filepaths:
            gcwerks = False
            if data_type.lower() in ("gc", "gcwerks"):
                metadata["data_type"] = "gcwerks"

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
        from openghg.store import ObsSurface

        results = ObsSurface.read_file(
            filepath=filepaths,
            data_type=data_type,
            site=site,
            network=network,
            instrument=instrument,
            sampling_period=sampling_period,
            inlet=inlet,
            overwrite=overwrite,
        )

        return results


def standardise_bc(
    filepath: Union[str, Path],
    species: str,
    bc_input: str,
    domain: str,
    period: Optional[Union[str, tuple]] = None,
    continuous: bool = True,
    overwrite: bool = False,
) -> Dict:
    """Standardise boundary condition data and store it in the object store.

    Args:
        filepath: Path of boundary conditions file
        species: Species name
        bc_input: Input used to create boundary conditions. For example:
            - a model name such as "MOZART" or "CAMS"
            - a description such as "UniformAGAGE" (uniform values based on AGAGE average)
        domain: Region for boundary conditions
        period: Period of measurements, if not passed this is inferred from the time coords
        overwrite: Should this data overwrite currently stored data.
    returns:
        dict: Dictionary containing confirmation of standardisation process.
    """
    from openghg.cloud import call_function
    from openghg.store import BoundaryConditions

    cloud = running_in_cloud()
    filepath = Path(filepath)

    if cloud:
        compressed_data, file_metadata = create_file_package(filepath=filepath, obs_type="bc")

        metadata = {
            "species": species,
            "bc_input": bc_input,
            "domain": domain,
            "continuous": continuous,
            "overwrite": overwrite,
        }

        if period is not None:
            metadata["period"] = period

        to_post = create_post_dict(
            function_name="standardise", data=compressed_data, metadata=metadata, file_metadata=file_metadata
        )

        fn_response = call_function(data=to_post)
        response_content: Dict = fn_response["content"]
        return response_content
    else:
        return BoundaryConditions.read_file(
            filepath=filepath,
            species=species,
            bc_input=bc_input,
            domain=domain,
            period=period,
            continuous=continuous,
            overwrite=overwrite,
        )


def standardise_footprint(
    filepath: Union[str, Path],
    site: str,
    height: str,
    domain: str,
    model: str,
    metmodel: Optional[str] = None,
    species: Optional[str] = None,
    network: Optional[str] = None,
    period: Optional[Union[str, tuple]] = None,
    continuous: bool = True,
    retrieve_met: bool = False,
    high_spatial_res: bool = False,
    high_time_res: bool = False,
    overwrite: bool = False,
) -> Dict:
    """Reads footprint data files and returns the UUIDs of the Datasources
    the processed data has been assigned to

    Args:
        filepath: Path of file to load
        site: Site name
        network: Network name
        height: Height above ground level in metres
        domain: Domain of footprints
        model_params: Model run parameters
        retrieve_met: Whether to also download meterological data for this footprints area
        high_spatial_res : Indicate footprints include both a low and high spatial resolution.
        high_time_res: Indicate footprints are high time resolution (include H_back dimension)
                        Note this will be set to True automatically for Carbon Dioxide data.
        overwrite: Overwrite any currently stored data
    Returns:
        dict: Dictionary containing confirmation of standardisation process.
    """
    from openghg.cloud import call_function
    from openghg.store import Footprints

    cloud = running_in_cloud()
    filepath = Path(filepath)

    if cloud:
        compressed_data, file_metadata = create_file_package(filepath=filepath, obs_type="footprints")

        metadata = {
            "site": site,
            "height": height,
            "domain": domain,
            "model": model,
            "continuous": continuous,
            "retrieve_met": retrieve_met,
            "high_spatial_res": high_spatial_res,
            "high_time_res": high_time_res,
            "overwrite": overwrite,
        }

        if metmodel is not None:
            metadata["metmodel"] = metmodel

        if species is not None:
            metadata["species"] = species

        if network is not None:
            metadata["network"] = network

        if period is not None:
            metadata["period"] = period

        to_post = create_post_dict(
            function_name="standardise", data=compressed_data, metadata=metadata, file_metadata=file_metadata
        )

        fn_response = call_function(data=to_post)
        response_content: Dict = fn_response["content"]
        return response_content
    else:
        return Footprints.read_file(
            filepath=filepath,
            site=site,
            height=height,
            domain=domain,
            model=model,
            metmodel=metmodel,
            species=species,
            network=network,
            period=period,
            continuous=continuous,
            retrieve_met=retrieve_met,
            high_spatial_res=high_spatial_res,
            high_time_res=high_time_res,
            overwrite=overwrite,
        )


def standardise_flux(
    filepath: Union[str, Path],
    species: str,
    source: str,
    domain: str,
    date: Optional[str] = None,
    high_time_resolution: Optional[bool] = False,
    period: Optional[Union[str, tuple]] = None,
    continuous: bool = True,
    overwrite: bool = False,
) -> Dict:
    """Process flux data

    Args:
        filepath: Path of emissions file
        species: Species name
        domain: Emissions domain
        source: Emissions source
        high_time_resolution: If this is a high resolution file
        period: Period of measurements, if not passed this is inferred from the time coords
        overwrite: Should this data overwrite currently stored data.
    returns:
        dict: Dictionary of Datasource UUIDs data assigned to
    """
    from openghg.cloud import call_function
    from openghg.store import Emissions

    cloud = running_in_cloud()
    filepath = Path(filepath)

    if cloud:
        compressed_data, file_metadata = create_file_package(filepath=filepath, obs_type="flux")

        metadata = {
            "species": species,
            "source": source,
            "domain": domain,
            "high_time_resolution": high_time_resolution,
            "continuous": continuous,
            "overwrite": overwrite,
        }

        if date is None:
            metadata["date"] = date

        if period is None:
            metadata["period"] = period

        to_post = create_post_dict(
            function_name="standardise", data=compressed_data, metadata=metadata, file_metadata=file_metadata
        )

        fn_response = call_function(data=to_post)
        response_content: Dict = fn_response["content"]
        return response_content
    else:
        return Emissions.read_file(
            filepath=filepath,
            species=species,
            source=source,
            domain=domain,
            date=date,
            high_time_resolution=high_time_resolution,
            period=period,
            continuous=continuous,
            overwrite=overwrite,
        )


# def upload_to_par(filepath: Optional[Union[str, Path]] = None, data: Optional[bytes] = None) -> None:
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
