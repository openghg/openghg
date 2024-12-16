from pathlib import Path
from typing import Any
from pandas import Timedelta
import warnings

from openghg.objectstore import get_writable_bucket
from openghg.util import sort_by_filenames
from openghg.types import optionalPathType, multiPathType
from numcodecs import Blosc
import logging

logger = logging.getLogger("openghg.standardise")


def standardise(data_type: str, filepath: multiPathType, store: str | None = None, **kwargs: Any) -> dict:
    """Generic standardise function, used by data-type specific versions.

    Args:
        data_type: type of data to standardise
        filepath: path to file(s) to standardise
        store: Name of object store to write to, required if user has access to more than one
        writable store
        **kwargs: data type specific arguments, see specific implementations below.
    Returns:
        dict: Dictionary of result data.
    """
    from openghg.store import get_data_class

    dclass = get_data_class(data_type)
    bucket = get_writable_bucket(name=store)

    compression = kwargs.get("compression", True)
    compressor = kwargs.get("compressor")

    if compression:
        if compressor is None:
            compressor = Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)
    else:
        logger.info("Compression disabled")
        compressor = None

    kwargs["compressor"] = compressor

    try:
        del kwargs["compression"]
    except KeyError:
        pass

    with dclass(bucket=bucket) as dc:
        result = dc.read_file(filepath=filepath, **kwargs)
    return result


def standardise_surface(
    source_format: str,
    network: str,
    site: str,
    filepath: multiPathType,
    inlet: str | None = None,
    height: str | None = None,
    instrument: str | None = None,
    data_level: str | int | float | None = None,
    data_sublevel: str | float | None = None,
    dataset_source: str | None = None,
    sampling_period: Timedelta | str | None = None,
    calibration_scale: str | None = None,
    measurement_type: str = "insitu",
    verify_site_code: bool = True,
    site_filepath: optionalPathType = None,
    store: str | None = None,
    update_mismatch: str = "never",
    if_exists: str = "auto",
    save_current: str = "auto",
    overwrite: bool = False,
    force: bool = False,
    compression: bool = True,
    compressor: Any | None = None,
    filters: Any | None = None,
    chunks: dict | None = None,
    optional_metadata: dict | None = None,
    sort_files: bool = False,
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
        data_level: The level of quality control which has been applied to the data.
            This should follow the convention of:
                - "0": raw sensor output
                - "1": automated quality assurance (QA) performed
                - "2": final data set
                - "3": elaborated data products using the data
        data_sublevel: Typically used for "L1" data depending on different QA performed
            before data is finalised.
        dataset_source: Dataset source name, for example "ICOS", "InGOS", "European ObsPack", "CEDA 2023.06".
        sampling_period: Sampling period as pandas time code, e.g. 1m for 1 minute, 1h for 1 hour
        calibration_scale: Calibration scale for data
        measurement_type: Type of measurement e.g. insitu, flask
        verify_site_code: Verify the site code
        site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
        store: Name of object store to write to, required if user has access to more than one
            writable store
        update_mismatch: This determines how mismatches between the internal data
            "attributes" and the supplied / derived "metadata" are handled.
            This includes the options:
                - "never" - don't update mismatches and raise an AttrMismatchError
                - "from_source" / "attributes" - update mismatches based on input attributes
                - "from_definition" / "metadata" - update mismatches based on input metadata
        if_exists: What to do if existing data is present.
            - "auto" - checks new and current data for timeseries overlap
                - adds data if no overlap
                - raises DataOverlapError if there is an overlap
            - "new" - just include new data and ignore previous
            - "combine" - replace and insert new data into current timeseries
        save_current: Whether to save data in current form and create a new version.
             - "auto" - this will depend on if_exists input ("auto" -> False), (other -> True)
             - "y" / "yes" - Save current data exactly as it exists as a separate (previous) version
             - "n" / "no" - Allow current data to updated / deleted
        overwrite: Deprecated. This will use options for if_exists="new".
        force: Force adding of data even if this is identical to data stored.
        compression: Enable compression in the store
        compressor: A custom compressor to use. If None, this will default to
            `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
            See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
        filters: Filters to apply to the data on storage, this defaults to no filtering. See
            https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
            for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
            See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
            To disable chunking pass an empty dictionary.
        optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
        sort_files: Sorts multiple files date-wise.
    Returns:
        dict: Dictionary of result data
    """
    if not isinstance(filepath, list):
        filepath = [filepath]

    if sort_files:
        if source_format.lower() != "gcwerks":
            filepath = sort_by_filenames(filepath=filepath)

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
        data_level=data_level,
        data_sublevel=data_sublevel,
        dataset_source=dataset_source,
        sampling_period=sampling_period,
        calibration_scale=calibration_scale,
        measurement_type=measurement_type,
        overwrite=overwrite,
        verify_site_code=verify_site_code,
        site_filepath=site_filepath,
        update_mismatch=update_mismatch,
        if_exists=if_exists,
        save_current=save_current,
        force=force,
        compression=compression,
        compressor=compressor,
        filters=filters,
        chunks=chunks,
        optional_metadata=optional_metadata,
    )


def standardise_column(
    filepath: str | Path,
    species: str,
    platform: str = "satellite",
    site: str | None = None,
    satellite: str | None = None,
    domain: str | None = None,
    selection: str | None = None,
    network: str | None = None,
    instrument: str | None = None,
    source_format: str = "openghg",
    store: str | None = None,
    if_exists: str = "auto",
    save_current: str = "auto",
    overwrite: bool = False,
    force: bool = False,
    compression: bool = True,
    compressor: Any | None = None,
    filters: Any | None = None,
    chunks: dict | None = None,
    optional_metadata: dict | None = None,
) -> dict:
    """Read column observation file

    Args:
        filepath: Path of observation file
        species: Species name or synonym e.g. "ch4"
        platform: Type of platform. Should be one of:
            - "satellite"
            - "site"
        satellite: Name of satellite (if relevant). Should include satellite OR site.
        domain: For satellite only. If data has been selected on an area include the
            identifier name for domain covered. This can map to previously defined domains
            (see openghg_defs "domain_info.json" file) or a newly defined domain.
        selection: For satellite only, identifier for any data selection which has been
            performed on satellite data. This can be based on any form of filtering, binning etc.
            but should be unique compared to other selections made e.g. "land", "glint", "upperlimit".
            If not specified, domain will be used.
        site : Site code/name (if relevant). Should include satellite OR site.
        instrument: Instrument name e.g. "TANSO-FTS"
        network: Name of in-situ or satellite network e.g. "TCCON", "GOSAT"        source_format : Type of data being input e.g. openghg (internal format)
        store: Name of store to write to
        if_exists: What to do if existing data is present.
            - "auto" - checks new and current data for timeseries overlap
                - adds data if no overlap
                - raises DataOverlapError if there is an overlap
            - "new" - just include new data and ignore previous
            - "combine" - replace and insert new data into current timeseries
        save_current: Whether to save data in current form and create a new version.
             - "auto" - this will depend on if_exists input ("auto" -> False), (other -> True)
             - "y" / "yes" - Save current data exactly as it exists as a separate (previous) version
             - "n" / "no" - Allow current data to updated / deleted
        overwrite: Deprecated. This will use options for if_exists="new".
        force: Force adding of data even if this is identical to data stored.
        compression: Enable compression in the store
        compressor: A custom compressor to use. If None, this will default to
            `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
            See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
        filters: Filters to apply to the data on storage, this defaults to no filtering. See
            https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
            for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
            See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking
            To disable chunking pass an empty dictionary.
        optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
    Returns:
        dict: Dictionary containing confirmation of standardisation process.
    """
    filepath = Path(filepath)
    return standardise(
        store=store,
        data_type="column",
        filepath=filepath,
        species=species,
        platform=platform,
        satellite=satellite,
        domain=domain,
        selection=selection,
        site=site,
        network=network,
        instrument=instrument,
        source_format=source_format,
        overwrite=overwrite,
        if_exists=if_exists,
        save_current=save_current,
        force=force,
        compression=compression,
        compressor=compressor,
        filters=filters,
        chunks=chunks,
        optional_metadata=optional_metadata,
    )


def standardise_bc(
    filepath: str | Path,
    species: str,
    bc_input: str,
    domain: str,
    source_format: str = "openghg",
    period: str | tuple | None = None,
    continuous: bool = True,
    store: str | None = None,
    if_exists: str = "auto",
    save_current: str = "auto",
    overwrite: bool = False,
    force: bool = False,
    compression: bool = True,
    compressor: Any | None = None,
    filters: Any | None = None,
    chunks: dict | None = None,
    optional_metadata: dict | None = None,
) -> dict:
    """Standardise boundary condition data and store it in the object store.

    Args:
        filepath: Path of boundary conditions file
        species: Species name
        bc_input: Input used to create boundary conditions. For example:
            - a model name such as "MOZART" or "CAMS"
            - a description such as "UniformAGAGE" (uniform values based on AGAGE average)
        domain: Region for boundary conditions
        source_format : Type of data being input e.g. openghg (internal format).
        period: Period of measurements, if not passed this is inferred from the time coords
        continuous: Whether time stamps have to be continuous.
        store: Name of store to write to
        if_exists: What to do if existing data is present.
            - "auto" - checks new and current data for timeseries overlap
                - adds data if no overlap
                - raises DataOverlapError if there is an overlap
            - "new" - just include new data and ignore previous
            - "combine" - replace and insert new data into current timeseries
        save_current: Whether to save data in current form and create a new version.
             - "auto" - this will depend on if_exists input ("auto" -> False), (other -> True)
             - "y" / "yes" - Save current data exactly as it exists as a separate (previous) version
             - "n" / "no" - Allow current data to updated / deleted
        overwrite: Deprecated. This will use options for if_exists="new".
        force: Force adding of data even if this is identical to data stored.
        compression: Enable compression in the store
        compressor: A custom compressor to use. If None, this will default to
            `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
            See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
        filters: Filters to apply to the data on storage, this defaults to no filtering. See
            https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
            for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
            See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking
            To disable chunking pass an empty dictionary.
        optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
    returns:
        dict: Dictionary containing confirmation of standardisation process.
    """
    filepath = Path(filepath)
    return standardise(
        store=store,
        data_type="boundary_conditions",
        filepath=filepath,
        species=species,
        bc_input=bc_input,
        domain=domain,
        source_format=source_format,
        period=period,
        continuous=continuous,
        overwrite=overwrite,
        if_exists=if_exists,
        save_current=save_current,
        force=force,
        compression=compression,
        compressor=compressor,
        filters=filters,
        chunks=chunks,
        optional_metadata=optional_metadata,
    )


def standardise_footprint(
    filepath: str | Path | list,
    site: str,
    domain: str,
    model: str,
    inlet: str | None = None,
    height: str | None = None,
    met_model: str | None = None,
    species: str | None = None,
    network: str | None = None,
    source_format: str = "acrg_org",
    period: str | tuple | None = None,
    chunks: dict | None = None,
    continuous: bool = True,
    retrieve_met: bool = False,
    store: str | None = None,
    if_exists: str = "auto",
    save_current: str = "auto",
    overwrite: bool = False,
    force: bool = False,
    high_spatial_resolution: bool = False,
    time_resolved: bool = False,
    high_time_resolution: bool = False,
    short_lifetime: bool = False,
    sort: bool = False,
    drop_duplicates: bool = False,
    compression: bool = True,
    compressor: Any | None = None,
    filters: Any | None = None,
    optional_metadata: dict | None = None,
    sort_files: bool = False,
) -> dict:
    """Reads footprint data files and returns the UUIDs of the Datasources
    the processed data has been assigned to

    Args:
        filepath: Path(s) of file to standardise
        site: Site name
        domain: Domain of footprints
        model: Model used to create footprint (e.g. NAME or FLEXPART)
        inlet: Height above ground level in metres. Format 'NUMUNIT' e.g. "10m"
        height: Alias for inlet. One of height or inlet must be included.
        met_model: Underlying meteorlogical model used (e.g. UKV)
        species: Species name. Only needed if footprint is for a specific species e.g. co2 (and not inert)
        network: Network name
        source_format: Format of the input data format, for example acrg_org
        period: Period of measurements. Only needed if this can not be inferred from the time coords
        chunks: Chunk schema to use when storing data. It expects a dictionary of dimension name and chunk size,
            for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
            See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking
            by OpenGHG as per the TODO RELEASE: add link to documentation. To disable chunking pass an empty dictionary.
        continuous: Whether time stamps have to be continuous.
        retrieve_met: Whether to also download meterological data for this footprints area
        high_spatial_resolution : Indicate footprints include both a low and high spatial resolution.
        time_resolved: Indicate footprints are high time resolution (include H_back dimension)
            Note this will be set to True automatically for Carbon Dioxide data.
        short_lifetime: Indicate footprint is for a short-lived species. Needs species input.
            Note this will be set to True if species has an associated lifetime.
        high_time_resolution: This argument is deprecated and will be replaced in future versions with time_resolved.
        store: Name of store to write to
        if_exists: What to do if existing data is present.
            - "auto" - checks new and current data for timeseries overlap
                - adds data if no overlap
                - raises DataOverlapError if there is an overlap
            - "new" - just include new data and ignore previous
            - "combine" - replace and insert new data into current timeseries
        save_current: Whether to save data in current form and create a new version.
             - "auto" - this will depend on if_exists input ("auto" -> False), (other -> True)
             - "y" / "yes" - Save current data exactly as it exists as a separate (previous) version
             - "n" / "no" - Allow current data to updated / deleted        overwrite: Deprecated. This will use options for if_exists="new".
        force: Force adding of data even if this is identical to data stored.
        sort: Sort data in by time
        drop_duplicates: Drop duplicate timestamps, keeping the first value
        compression: Enable compression in the store
        compressor: A custom compressor to use. If None, this will default to
            `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
            See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
        filters: Filters to apply to the data on storage, this defaults to no filtering. See
            https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
        optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
        sort_files: Sort multiple files datewise
    Returns:
        dict / None: Dictionary containing confirmation of standardisation process. None
        if file already processed.
    """
    if high_time_resolution:
        warnings.warn(
            "This argument is deprecated and will be replaced in future versions with time_resolved.",
            DeprecationWarning,
        )
        time_resolved = high_time_resolution

    if not isinstance(filepath, list):
        filepath = [filepath]

    if sort_files:
        filepath = sort_by_filenames(filepath=filepath)

    return standardise(
        store=store,
        data_type="footprints",
        filepath=filepath,
        site=site,
        domain=domain,
        model=model,
        inlet=inlet,
        height=height,
        met_model=met_model,
        species=species,
        network=network,
        source_format=source_format,
        period=period,
        chunks=chunks,
        continuous=continuous,
        retrieve_met=retrieve_met,
        high_spatial_resolution=high_spatial_resolution,
        time_resolved=time_resolved,
        short_lifetime=short_lifetime,
        overwrite=overwrite,
        if_exists=if_exists,
        save_current=save_current,
        force=force,
        compression=compression,
        compressor=compressor,
        filters=filters,
        sort=sort,
        drop_duplicates=drop_duplicates,
        optional_metadata=optional_metadata,
    )


def standardise_flux(
    filepath: str | Path,
    species: str,
    source: str,
    domain: str,
    database: str | None = None,
    source_format: str = "openghg",
    database_version: str | None = None,
    model: str | None = None,
    time_resolved: bool = False,
    high_time_resolution: bool = False,
    period: str | tuple | None = None,
    chunks: dict | None = None,
    continuous: bool = True,
    store: str | None = None,
    if_exists: str = "auto",
    save_current: str = "auto",
    overwrite: bool = False,
    force: bool = False,
    compression: bool = True,
    compressor: Any | None = None,
    filters: Any | None = None,
    optional_metadata: dict | None = None,
) -> dict:
    """Process flux / emissions data

    Args:
        filepath: Path of flux / emissions file
        species: Species name
        source: Flux / Emissions source
        domain: Flux / Emissions domain
        source_format: Data format, for example openghg, intem
        date: Date as a string e.g. "2012" or "201206" associated with emissions as a string.
               Only needed if this can not be inferred from the time coords
        time_resolved: If this is a high resolution file
        high_time_resolution: This argument is deprecated and will be replaced in future versions with time_resolved.
        period: Period of measurements, if not passed this is inferred from the time coords
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
            for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
            See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
            To disable chunking pass an empty dictionary.
        continuous: Whether time stamps have to be continuous.
        store: Name of store to write to
        if_exists: What to do if existing data is present.
            - "auto" - checks new and current data for timeseries overlap
                - adds data if no overlap
                - raises DataOverlapError if there is an overlap
            - "new" - just include new data and ignore previous
            - "combine" - replace and insert new data into current timeseries
        save_current: Whether to save data in current form and create a new version.
             - "auto" - this will depend on if_exists input ("auto" -> False), (other -> True)
             - "y" / "yes" - Save current data exactly as it exists as a separate (previous) version
             - "n" / "no" - Allow current data to updated / deleted
        overwrite: Deprecated. This will use options for if_exists="new".
        force: Force adding of data even if this is identical to data stored.
        compression: Enable compression in the store
        compressor: A custom compressor to use. If None, this will default to
            `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
            See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
        filters: Filters to apply to the data on storage, this defaults to no filtering. See
            https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
        optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
    returns:
        dict: Dictionary of Datasource UUIDs data assigned to
    """
    filepath = Path(filepath)

    if high_time_resolution:
        warnings.warn(
            "This argument is deprecated and will be replaced in future versions with time_resolved.",
            DeprecationWarning,
        )
        time_resolved = high_time_resolution
    return standardise(
        data_type="flux",
        store=store,
        filepath=filepath,
        species=species,
        source=source,
        domain=domain,
        database=database,
        database_version=database_version,
        model=model,
        time_resolved=time_resolved,
        period=period,
        continuous=continuous,
        chunks=chunks,
        overwrite=overwrite,
        if_exists=if_exists,
        save_current=save_current,
        force=force,
        compression=compression,
        compressor=compressor,
        filters=filters,
        optional_metadata=optional_metadata,
    )


def standardise_eulerian(
    filepath: str | Path,
    model: str,
    species: str,
    source_format: str = "openghg",
    start_date: str | None = None,
    end_date: str | None = None,
    setup: str | None = None,
    if_exists: str = "auto",
    save_current: str = "auto",
    overwrite: bool = False,
    store: str | None = None,
    force: bool = False,
    compression: bool = True,
    compressor: Any | None = None,
    filters: Any | None = None,
    chunks: dict | None = None,
    optional_metadata: dict | None = None,
) -> dict:
    """Read Eulerian model output

    Args:
        filepath: Path of Eulerian model species output
        model: Eulerian model name
        species: Species name
        source_format: Data format, for example openghg (internal format)
        start_date: Start date (inclusive) associated with model run
        end_date: End date (exclusive) associated with model run
        setup: Additional setup details for run
        if_exists: What to do if existing data is present.
            - "auto" - checks new and current data for timeseries overlap
                - adds data if no overlap
                - raises DataOverlapError if there is an overlap
            - "new" - just include new data and ignore previous
            - "combine" - replace and insert new data into current timeseries
        save_current: Whether to save data in current form and create a new version.
            - "auto" - this will depend on if_exists input ("auto" -> False), (other -> True)
            - "y" / "yes" - Save current data exactly as it exists as a separate (previous) version
            - "n" / "no" - Allow current data to updated / deleted
        overwrite: Deprecated. This will use options for if_exists="new".
        store: Name of object store to write to, required if user has access to more than one
        writable store
        force: Force adding of data even if this is identical to data stored.
        compression: Enable compression in the store
        compressor: A custom compressor to use. If None, this will default to
            `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
            See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
        filters: Filters to apply to the data on storage, this defaults to no filtering. See
            https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
            for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
            See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
            To disable chunking pass an empty dictionary.
        optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
    Returns:
        dict: Dictionary of result data
    """
    return standardise(
        store=store,
        data_type="eulerian_model",
        filepath=filepath,
        source_format=source_format,
        model=model,
        species=species,
        start_date=start_date,
        end_date=end_date,
        setup=setup,
        overwrite=overwrite,
        if_exists=if_exists,
        force=force,
        save_current=save_current,
        compression=compression,
        compressor=compressor,
        filters=filters,
        chunks=chunks,
        optional_metadata=optional_metadata,
    )


def standardise_from_binary_data(
    store: str,
    data_type: str,
    binary_data: bytes,
    metadata: dict,
    file_metadata: dict,
    **kwargs: Any,
) -> dict | None:
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
    from openghg.store import get_data_class

    dclass = get_data_class(data_type)
    bucket = get_writable_bucket(name=store)

    with dclass(bucket) as dc:
        result = dc.read_data(
            binary_data=binary_data, metadata=metadata, file_metadata=file_metadata, **kwargs
        )
    return result


def standardise_flux_timeseries(
    filepath: str | Path,
    species: str,
    source: str,
    region: str = "UK",
    source_format: str = "crf",
    domain: str | None = None,
    database: str | None = None,
    database_version: str | None = None,
    model: str | None = None,
    store: str | None = None,
    if_exists: str = "auto",
    save_current: str = "auto",
    overwrite: bool = False,
    force: bool = False,
    compressor: Any | None = None,
    filters: Any | None = None,
    period: str | tuple | None = None,
    continuous: bool | None = None,
    optional_metadata: dict | None = None,
) -> dict:
    """Process one dimension timeseries file

    Args:
        filepath: Path of flux timeseries file
        species: Species name
        source: Flux / Emissions source
        region: Region/Country of the CRF data
        source_format : Type of data being input e.g. openghg (internal format)
        period: Period of measurements. Only needed if this can not be inferred from the time coords
        domain: If flux is related to pre-existing domain (e.g. "EUROPE") with defined latitude-longitude bounds this can be used to flag that. Otherwise, use `region` input to describe the name of a region (e.g. "UK").
        database: Name of database source for this input (if relevant)
        database_version: Name of database version (if relevant)
        model: Model name (if relevant)
        If specified, should be one of:
            - "yearly", "monthly"
            - suitable pandas Offset Alias
            - tuple of (value, unit) as would be passed to pandas.Timedelta function
        chunks: Chunking schema to use when storing data. It expects a dictionary of dimension name and chunk size,
            for example {"time": 100}. If None then a chunking schema will be set automatically by OpenGHG.
            See documentation for guidance on chunking: https://docs.openghg.org/tutorials/local/Adding_data/Adding_ancillary_data.html#chunking.
            To disable chunking pass in an empty dictionary.
        continuous: Whether time stamps have to be continuous.
        if_exists: What to do if existing data is present.
            - "auto" - checks new and current data for timeseries overlap
                - adds data if no overlap
                - raises DataOverlapError if there is an overlap
            - "new" - just include new data and ignore previous
            - "combine" - replace and insert new data into current timeseries
        save_current: Whether to save data in current form and create a new version.
            - "auto" - this will depend on if_exists input ("auto" -> False), (other -> True)
            - "y" / "yes" - Save current data exactly as it exists as a separate (previous) version
            - "n" / "no" - Allow current data to updated / deleted
        overwrite: Deprecated. This will use options for if_exists="new".
        force: Force adding of data even if this is identical to data stored.
        compressor: A custom compressor to use. If None, this will default to
            `Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)`.
            See https://zarr.readthedocs.io/en/stable/api/codecs.html for more information on compressors.
        filters: Filters to apply to the data on storage, this defaults to no filtering. See
            https://zarr.readthedocs.io/en/stable/tutorial.html#filters for more information on picking filters.
        optional_metadata: Allows to pass in additional tags to distinguish added data. e.g {"project":"paris", "baseline":"Intem"}
    Returns:
        dict: Dictionary of datasource UUIDs data assigned to
    """

    if domain is not None:
        logger.warning(
            "Geographic domain, default is 'None'. Instead region is used to identify area,"
            "Please supply region in future instances"
        )
        region = domain
    return standardise(
        data_type="flux_timeseries",
        store=store,
        filepath=filepath,
        species=species,
        source=source,
        source_format=source_format,
        domain=domain,
        region=region,
        database=database,
        database_version=database_version,
        model=model,
        overwrite=overwrite,
        if_exists=if_exists,
        save_current=save_current,
        force=force,
        compressor=compressor,
        filters=filters,
        period=period,
        continuous=continuous,
        optional_metadata=optional_metadata,
    )
