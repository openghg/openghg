from pathlib import Path
from typing import Dict, Literal, Optional, Union, Any
from pandas import Timedelta
import warnings

from openghg.store.base import get_data_class
from openghg.cloud import create_file_package, create_post_dict
from openghg.objectstore import get_writable_bucket
from openghg.util import running_on_hub
from openghg.types import optionalPathType, multiPathType


def standardise(data_type: str, filepath: multiPathType, store: Optional[str] = None, **kwargs: Any) -> dict:
    """Generic standardise function, used by data-type specific versions.

    Args:
        bucket: object store bucket to use
        store: Name of object store to write to, required if user has access to more than one
        writable store

        data_type: type of data to standardise
        filepath: path to file(s) to standardise
        **kwargs: data type specific arguments, see specific implementations below.

    Returns:
        dict: Dictionary of result data.
    """
    dclass = get_data_class(data_type)
    bucket = get_writable_bucket(name=store)

    with dclass(bucket=bucket) as dc:
        result = dc.read_file(filepath=filepath, **kwargs)
    return result


def standardise_surface(
    source_format: str,
    network: str,
    site: str,
    filepath: Optional[multiPathType] = None,
    filepaths: Optional[multiPathType] = None,
    inlet: Optional[str] = None,
    height: Optional[str] = None,
    instrument: Optional[str] = None,
    sampling_period: Optional[Union[Timedelta, str]] = None,
    calibration_scale: Optional[str] = None,
    update_mismatch: str = "never",
    measurement_type: str = "insitu",
    overwrite: bool = False,
    verify_site_code: bool = True,
    site_filepath: optionalPathType = None,
    store: Optional[str] = None,
    **kwargs: Any
) -> dict:
    """Standardise surface measurements and store the data in the object store.

    Args:
        filepath: Filepath(s)
        source_format: Data format, for example CRDS, GCWERKS
        site: Site code/name
        network: Network name
        inlet: Inlet height. Format 'NUMUNIT' e.g. "10m".
            If retrieve multiple files pass None, OpenGHG will attempt to
            extract this from the file.
        height: Alias for inlet.
        instrument: Instrument name
        sampling_period: Sampling period in pandas style (e.g. 2H for 2 hour period, 2m for 2 minute period).
        calibration_scale: Calibration scale for data
        update_mismatch: This determines how mismatches between the internal data
            "attributes" and the supplied / derived "metadata" are handled.
            This includes the options:
                - "never" - don't update mismatches and raise an AttrMismatchError
                - "from_source" / "attributes" - update mismatches based on input attributes
                - "from_definition" / "metadata" - update mismatches based on input metadata
        measurement_type: Type of measurement e.g. insitu, flask
        overwrite: Overwrite previously uploaded data
        verify_site_code: Verify the site code
        site_filepath: Alternative site info file (see openghg/supplementary_data repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
        store: Name of object store to write to, required if user has access to more than one
        writable store
        kwargs: To pass in additional tag as metadata

    Returns:
        dict: Dictionary of result data
    """
    from openghg.cloud import call_function

    if filepath is None and filepaths is None:
        raise ValueError("One of `filepath` and `filepaths` must be specified.")
    elif filepath is None:
        filepath = filepaths
        warnings.warn(
            "The argument 'filepaths' will be deprecated in a future release. Please use 'filepath' instead.",
            FutureWarning,
        )

    if not isinstance(filepath, list):
        filepath = [filepath]

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

        metadata.update(kwargs) if kwargs else None

        responses = {}
        for fpath in filepath:
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
        return standardise(
            store=store,
            data_type="surface",
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
            update_mismatch=update_mismatch,
        )


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
    **kwargs: Any
) -> dict:
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
        kwargs: To pass in additional tag as metadata


    Returns:
        dict: Dictionary containing confirmation of standardisation process.
    """
    from openghg.cloud import call_function

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
        metadata.update(kwargs) if kwargs else None

        to_post = create_post_dict(
            function_name="standardise", data=compressed_data, metadata=metadata, file_metadata=file_metadata
        )

        fn_response = call_function(data=to_post)
        response_content: Dict = fn_response["content"]
        return response_content
    else:
        return standardise(
            store=store,
            data_type="column",
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


def standardise_bc(
    filepath: Union[str, Path],
    species: str,
    bc_input: str,
    domain: str,
    period: Optional[Union[str, tuple]] = None,
    continuous: bool = True,
    overwrite: bool = False,
    store: Optional[str] = None,
    **kwargs: Any
) -> dict:
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
        kwargs: To pass in additional tag as metadata


    Returns:
        dict: Dictionary containing confirmation of standardisation process.
    """
    from openghg.cloud import call_function

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
        metadata.update(kwargs) if kwargs else None

        if period is not None:
            metadata["period"] = period

        to_post = create_post_dict(
            function_name="standardise", data=compressed_data, metadata=metadata, file_metadata=file_metadata
        )

        fn_response = call_function(data=to_post)
        response_content: Dict = fn_response["content"]
        return response_content
    else:
        return standardise(
            store=store,
            data_type="boundary_conditions",
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
    high_spatial_resolution: bool = False,
    high_time_resolution: bool = False,
    overwrite: bool = False,
    store: Optional[str] = None,
    bucket: Optional[str] = None,
    **kwargs: Any
) -> dict:
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
        high_spatial_resolution : Indicate footprints include both a low and high spatial resolution.
        high_time_resolution: Indicate footprints are high time resolution (include H_back dimension)
                        Note this will be set to True automatically for Carbon Dioxide data.
        overwrite: Overwrite any currently stored data
        store: Name of store to write to
        bucket: object store bucket to use; this takes precendence over 'store'
        kwargs: To pass in additional tag as metadata

    Returns:
        dict / None: Dictionary containing confirmation of standardisation process. None
        if file already processed.
    """
    from openghg.cloud import call_function

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
            "high_spatial_resolution": high_spatial_resolution,
            "high_time_resolution": high_time_resolution,
            "overwrite": overwrite,
            "metmodel": metmodel,
            "species": species,
            "network": network,
            "period": period,
            "chunks": chunks,
        }

        metadata = {k: v for k, v in metadata.items() if v is not None}
        metadata.update(kwargs) if kwargs else None

        to_post = create_post_dict(
            function_name="standardise", data=compressed_data, metadata=metadata, file_metadata=file_metadata
        )

        fn_response = call_function(data=to_post)
        response_content: Dict = fn_response["content"]
        return response_content
    else:
        return standardise(
            store=store,
            data_type="footprints",
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
            high_spatial_resolution=high_spatial_resolution,
            high_time_resolution=high_time_resolution,
            overwrite=overwrite,
        )


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
    **kwargs: Any
) -> dict:
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
        kwargs: To pass in additional tag as metadata

    Returns:
        dict: Dictionary of Datasource UUIDs data assigned to
    """
    from openghg.cloud import call_function

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
        metadata.update(kwargs) if kwargs else None

        to_post = create_post_dict(
            function_name="standardise", data=compressed_data, metadata=metadata, file_metadata=file_metadata
        )

        fn_response = call_function(data=to_post)
        response_content: Dict = fn_response["content"]
        return response_content
    else:
        return standardise(
            data_type="emissions",
            store=store,
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


def standardise_eulerian(
    filepath: Union[str, Path],
    model: str,
    species: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    setup: Optional[str] = None,
    overwrite: bool = False,
    store: Optional[str] = None,
    **kwargs: Any
) -> dict:
    """Read Eulerian model output

    Args:
        filepath: Path of Eulerian model species output
        model: Eulerian model name
        species: Species name
        start_date: Start date (inclusive) associated with model run
        end_date: End date (exclusive) associated with model run
        setup: Additional setup details for run
        overwrite: Should this data overwrite currently stored data.
        store: Name of object store to write to, required if user has access to more than one
        writable store
        kwargs: To pass in additional tag as metadata

    Returns:
        dict: Dictionary of result data
    """
    if running_on_hub():
        metadata = {}
        metadata.update(kwargs) if kwargs else None
        raise NotImplementedError("Serverless not implemented yet for Eulerian model.")

    else:
        return standardise(
            store=store,
            data_type="eulerian_model",
            filepath=filepath,
            model=model,
            species=species,
            start_date=start_date,
            end_date=end_date,
            setup=setup,
            overwrite=overwrite,
        )


def standardise_from_binary_data(
    store: str, data_type: str, binary_data: bytes, metadata: dict, file_metadata: dict, **kwargs: Any
) -> Optional[dict]:
    """Standardise binary data from serverless function.
        The data dictionary should contain sub-dictionaries that contain
        data and metadata keys.

    args:
        store: Name of object store to write to, required if user has access to more than one
        writable store
        data_type: type of data to standardise
        binary_data: Binary measurement data
        metadata: Metadata
        file_metadata: File metadata such as original filename
        **kwargs: data type specific arguments, see specific implementations in data classes.

    returns:
        Dictionary of result data.
    """
    dclass = get_data_class(data_type)
    bucket = get_writable_bucket(name=store)

    with dclass(bucket) as dc:
        result = dc.read_data(
            binary_data=binary_data, metadata=metadata, file_metadata=file_metadata, **kwargs
        )
    return result
