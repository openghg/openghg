from pathlib import Path
from typing import Dict, Literal, Optional, Union

from openghg.cloud import create_file_package, create_post_dict
from openghg.objectstore import get_writable_bucket
from openghg.util import running_on_hub


def standardise_column(
    filepath: Union[str, Path],
    satellite: Optional[str] = None,
    domain: Optional[str] = None,
    selection: Optional[str] = None,
    site: Optional[str] = None,
    species: Optional[str] = None,
    network: Optional[str] = None,
    instrument: Optional[str] = None,
    platform: str = "satellite",
    source_format: str = "openghg",
    overwrite: bool = False,
    store: Optional[str] = None,
) -> Optional[Dict]:
    """Read column observation file

    Args:
        filepath: Path of observation file
        satellite: Name of satellite (if relevant)
        domain: For satellite only. If data has been selected on an area include the
            identifier name for domain covered. This can map to previously defined domains
            (see openghg_defs "domain_info.json" file) or a newly defined domain.
        selection: For satellite only, identifier for any data selection which has been
            performed on satellite data. This can be based on any form of filtering, binning etc.
            but should be unique compared to other selections made e.g. "land", "glint", "upperlimit".
            If not specified, domain will be used.
        site : Site code/name (if relevant). Can include satellite OR site.
        species: Species name or synonym e.g. "ch4"
        instrument: Instrument name e.g. "TANSO-FTS"
        network: Name of in-situ or satellite network e.g. "TCCON", "GOSAT"
        platform: Type of platform. Should be one of:
            - "satellite"
            - "site"
        source_format : Type of data being input e.g. openghg (internal format)
        overwrite: Should this data overwrite currently stored data.
        store: Name of store to write to
    Returns:
        dict: Dictionary containing confirmation of standardisation process.
    """
    from openghg.cloud import call_function
    from openghg.store import ObsColumn

    filepath = Path(filepath)

    if running_on_hub():
        compressed_data, file_metadata = create_file_package(filepath=filepath, obs_type="footprints")

        metadata = {
            "site": site,
            "satellite": satellite,
            "domain": domain,
            "selection": selection,
            "site": site,
            "species": species,
            "network": network,
            "instrument": instrument,
            "platform": platform,
            "source_format": source_format,
            "overwrite": overwrite,
        }

        metadata = {k: v for k, v in metadata.items() if v is not None}

        to_post = create_post_dict(
            function_name="standardise", data=compressed_data, metadata=metadata, file_metadata=file_metadata
        )

        fn_response = call_function(data=to_post)
        response_content: Dict = fn_response["content"]
        return response_content
    else:
        bucket = get_writable_bucket(name=store)

        with ObsColumn(bucket=bucket) as obs_col:
            result = obs_col.read_file(
                filepath=filepath,
                satellite=satellite,
                domain=domain,
                selection=selection,
                site=site,
                species=species,
                network=network,
                instrument=instrument,
                platform=platform,
                source_format=source_format,
                overwrite=overwrite,
            )

        return result


def standardise_bc(
    filepath: Union[str, Path],
    species: str,
    bc_input: str,
    domain: str,
    period: Optional[Union[str, tuple]] = None,
    continuous: bool = True,
    overwrite: bool = False,
    store: Optional[str] = None,
) -> Optional[Dict]:
    """Standardise boundary condition data and store it in the object store.

    Args:
        filepath: Path of boundary conditions file
        species: Species name
        bc_input: Input used to create boundary conditions. For example:
            - a model name such as "MOZART" or "CAMS"
            - a description such as "UniformAGAGE" (uniform values based on AGAGE average)
        domain: Region for boundary conditions
        period: Period of measurements, if not passed this is inferred from the time coords
        continuous: Whether time stamps have to be continuous.
        overwrite: Should this data overwrite currently stored data.
        store: Name of store to write to
    returns:
        dict: Dictionary containing confirmation of standardisation process.
    """
    from openghg.cloud import call_function
    from openghg.store import BoundaryConditions

    filepath = Path(filepath)

    if running_on_hub():
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
        bucket = get_writable_bucket(name=store)
        with BoundaryConditions(bucket=bucket) as bcs:
            result = bcs.read_file(
                filepath=filepath,
                species=species,
                bc_input=bc_input,
                domain=domain,
                period=period,
                continuous=continuous,
                overwrite=overwrite,
            )

        return result


def standardise_footprint(
    filepath: Union[str, Path],
    site: str,
    domain: str,
    model: str,
    inlet: Optional[str] = None,
    height: Optional[str] = None,
    metmodel: Optional[str] = None,
    species: Optional[str] = None,
    network: Optional[str] = None,
    period: Optional[Union[str, tuple]] = None,
    chunks: Union[int, Dict, Literal["auto"], None] = None,
    continuous: bool = True,
    retrieve_met: bool = False,
    high_spatial_res: bool = False,
    high_time_res: bool = False,
    overwrite: bool = False,
    store: Optional[str] = None,
) -> Optional[Dict]:
    """Reads footprint data files and returns the UUIDs of the Datasources
    the processed data has been assigned to

    Args:
        filepath: Path of file to load
        site: Site name
        domain: Domain of footprints
        model: Model used to create footprint (e.g. NAME or FLEXPART)
        inlet: Height above ground level in metres. Format 'NUMUNIT' e.g. "10m"
        height: Alias for inlet. One of height or inlet must be included.
        metmodel: Underlying meteorlogical model used (e.g. UKV)
        species: Species name. Only needed if footprint is for a specific species e.g. co2 (and not inert)
        network: Network name
        period: Period of measurements. Only needed if this can not be inferred from the time coords
        chunks: Chunk size to use when opening the NetCDF. Set to "auto" for automated chunk sizing
        continuous: Whether time stamps have to be continuous.
        retrieve_met: Whether to also download meterological data for this footprints area
        high_spatial_res : Indicate footprints include both a low and high spatial resolution.
        high_time_res: Indicate footprints are high time resolution (include H_back dimension)
                        Note this will be set to True automatically for Carbon Dioxide data.
        overwrite: Overwrite any currently stored data
        store: Name of store to write to
    Returns:
        dict / None: Dictionary containing confirmation of standardisation process. None
        if file already processed.
    """
    from openghg.cloud import call_function
    from openghg.store import Footprints

    filepath = Path(filepath)

    if running_on_hub():
        compressed_data, file_metadata = create_file_package(filepath=filepath, obs_type="footprints")

        metadata = {
            "site": site,
            "domain": domain,
            "model": model,
            "inlet": inlet,
            "height": height,
            "continuous": continuous,
            "retrieve_met": retrieve_met,
            "high_spatial_res": high_spatial_res,
            "high_time_res": high_time_res,
            "overwrite": overwrite,
            "metmodel": metmodel,
            "species": species,
            "network": network,
            "period": period,
            "chunks": chunks,
        }

        metadata = {k: v for k, v in metadata.items() if v is not None}

        to_post = create_post_dict(
            function_name="standardise", data=compressed_data, metadata=metadata, file_metadata=file_metadata
        )

        fn_response = call_function(data=to_post)
        response_content: Dict = fn_response["content"]
        return response_content
    else:
        bucket = get_writable_bucket(name=store)
        with Footprints(bucket=bucket) as fps:
            result = fps.read_file(
                filepath=filepath,
                site=site,
                domain=domain,
                model=model,
                inlet=inlet,
                height=height,
                metmodel=metmodel,
                species=species,
                network=network,
                period=period,
                chunks=chunks,
                continuous=continuous,
                retrieve_met=retrieve_met,
                high_spatial_res=high_spatial_res,
                high_time_res=high_time_res,
                overwrite=overwrite,
            )

        return result


def standardise_flux(
    filepath: Union[str, Path],
    species: str,
    source: str,
    domain: str,
    database: Optional[str] = None,
    database_version: Optional[str] = None,
    model: Optional[str] = None,
    high_time_resolution: Optional[bool] = False,
    period: Optional[Union[str, tuple]] = None,
    chunks: Union[int, Dict, Literal["auto"], None] = None,
    continuous: bool = True,
    overwrite: bool = False,
    store: Optional[str] = None,
) -> Optional[Dict]:
    """Process flux data

    Args:
        filepath: Path of emissions file
        species: Species name
        source: Emissions source
        domain: Emissions domain
        date : Date as a string e.g. "2012" or "201206" associated with emissions as a string.
               Only needed if this can not be inferred from the time coords
        high_time_resolution: If this is a high resolution file
        period: Period of measurements, if not passed this is inferred from the time coords
        continuous: Whether time stamps have to be continuous.
        overwrite: Should this data overwrite currently stored data.
        store: Name of store to write to
    returns:
        dict: Dictionary of Datasource UUIDs data assigned to
    """
    from openghg.cloud import call_function
    from openghg.store import Emissions

    filepath = Path(filepath)

    if running_on_hub():
        compressed_data, file_metadata = create_file_package(filepath=filepath, obs_type="flux")

        metadata = {
            "species": species,
            "source": source,
            "domain": domain,
            "high_time_resolution": high_time_resolution,
            "continuous": continuous,
            "overwrite": overwrite,
            "chunks": chunks,
            "period": period,
        }

        optional_keywords = {"database": database, "database_version": database_version, "model": model}
        for key, value in optional_keywords.items():
            if value is not None:
                metadata[key] = value

        metadata = {k: v for k, v in metadata.items()}

        to_post = create_post_dict(
            function_name="standardise", data=compressed_data, metadata=metadata, file_metadata=file_metadata
        )

        fn_response = call_function(data=to_post)
        response_content: Dict = fn_response["content"]
        return response_content
    else:
        bucket = get_writable_bucket(name=store)
        with Emissions(bucket=bucket) as ems:
            return ems.read_file(
                filepath=filepath,
                species=species,
                source=source,
                domain=domain,
                database=database,
                database_version=database_version,
                model=model,
                high_time_resolution=high_time_resolution,
                period=period,
                continuous=continuous,
                chunks=chunks,
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
