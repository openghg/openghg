from typing import Any

from openghg.dataobjects import ObsData
from openghg.objectstore import get_writable_bucket
import logging

logger = logging.getLogger("openghg.retrieve")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def retrieve_surface(
    site: str | None = None,
    species: str | None = None,
    inlet: str | None = None,
    url: str | None = None,
    force_retrieval: bool = False,
    additional_metadata: dict | None = None,
    store: str | None = None,
) -> list[ObsData] | ObsData | None:
    """Retrieve surface measurements from the CEDA archive. This function will route the call
    to either local or cloud functions based on the environment.

    Args:
        site: Site name
        species: Species name
        inlet: Inlet height
        url: URL of data in CEDA archive
        force_retrieval: Force the retrieval of data from a URL
        additional_metadata: Additional metadata to pass if the returned data
        doesn't contain everythging we need. At the moment we try and find site and inlet
        keys if they aren't found in the dataset's attributes.
        For example:
            {"site": "AAA", "inlet": "10m"}
        store: Name of object store to use
    Returns:
        ObsData or None: ObsData if data found / retrieved successfully.

    Example:
        To retrieve new data from the CEDA archive using a URL
        >>> retrieve_surface(url=https://dap.ceda.ac.uk/badc/...)
        To retrieve already cached data from the object store
        >>> retrieve_surface(site="BSD", species="ch4)

    """
    return retrieve(
        site=site,
        species=species,
        inlet=inlet,
        url=url,
        force_retrieval=force_retrieval,
        additional_metadata=additional_metadata,
        store=store,
    )


def retrieve(
    site: str | None = None,
    species: str | None = None,
    inlet: str | None = None,
    url: str | None = None,
    force_retrieval: bool = False,
    additional_metadata: dict | None = None,
    store: str | None = None,
    **kwargs: Any,
) -> list[ObsData] | ObsData | None:
    """Retrieve surface observations data from the CEDA archive. You can pass
    search terms and the object store will be searched. To retrieve data from the
    CEDA Archive please browse the website (https://data.ceda.ac.uk/badc) to find
    the URL of the dataset to retrieve.

    Args:
        site: Site name
        species: Species name
        inlet: Inlet height
        url: URL of data in CEDA archive
        force_retrieval: Force the retrieval of data from a URL
        additional_metadata: Additional metadata to pass if the returned data
        doesn't contain everythging we need. At the moment we try and find site and inlet
        keys if they aren't found in the dataset's attributes.
        For example:
            {"site": "AAA", "inlet": "10m"}
        store: Name of object store
    Returns:
        ObsData or None: ObsData if data found / retrieved successfully.

    Example:
        To retrieve new data from the CEDA archive using a URL
        >>> retrieve_surface(url=https://dap.ceda.ac.uk/badc/...)
        To retrieve already cached data from the object store
        >>> retrieve_surface(site="BSD", species="ch4)
    """
    import io

    import xarray as xr
    from openghg.retrieve import search_surface
    from openghg.store import ObsSurface
    from openghg.util import download_data, parse_url_filename, site_code_finder, timestamp_now

    if additional_metadata is None:
        additional_metadata = {}

    results = search_surface(site=site, species=species, inlet=inlet, data_source="ceda_archive", store=store)

    if results and not force_retrieval or url is None:
        return results.retrieve_all()

    filename = parse_url_filename(url=url)
    extension = filename.split(".")[-1].lower()

    if extension != "nc":
        logger.warning("We can only currently retrieve and process NetCDF files.")
        return None

    binary_data = download_data(url=url)

    if binary_data is None:
        logger.error("No data retrieved.")
        return None

    with io.BytesIO(binary_data) as buf:
        # Type ignored as buf is file-like which should be accepted by xarray
        # open_dataset - https://docs.xarray.dev/en/stable/generated/xarray.open_dataset.html
        # 27/07/2022: file-like (including BytesIO) isn't included in the accepted types
        #  - Union[str, PathLike[Any], AbstractDataStore]
        dataset = xr.open_dataset(buf).load()  # type:ignore

    now = str(timestamp_now())

    key = f"{filename}_{now}"
    # We expect to be dealing with timeseries data here
    # We'll take the attributes as metadata
    metadata = dataset.attrs.copy()

    metadata["data_type"] = "surface"
    metadata["data_source"] = "ceda_archive"
    # TODO - how should we find these? Need to change how we're retrieving Datasources
    # using metadata
    metadata["network"] = metadata.get("network", "CEDA_RETRIEVED")
    metadata["sampling_period"] = metadata.get("sampling_period", "NA")

    # If we're going to be using site, species and inlet here we should check that that
    # information is in the metadata
    if not {"site", "inlet"} <= metadata.keys():
        site_name = metadata["station_long_name"]
        site_code = site_code_finder(site_name=site_name)

        if site_code is not None:
            metadata["site"] = site_code
        else:
            if additional_metadata:
                try:
                    metadata["site"] = additional_metadata["site"]
                except KeyError:
                    logger.error("Unable to read site from additional_metadata.")
                    return None
            else:
                logger.error("Error: cannot find site code, please pass additional metadata.")
                return None

        try:
            metadata["inlet"] = f"{int(metadata['inlet_height_magl'])}m"
        except KeyError:
            try:
                metadata["inlet"] = additional_metadata["inlet"]
            except KeyError:
                logger.error("Unable to read inlet from data or additional_metadata.")
                return None

    to_store = {key: {"data": dataset, "metadata": metadata}}

    bucket = get_writable_bucket(name=store)
    with ObsSurface(bucket=bucket) as obs:
        obs.store_data(data=to_store)

    return ObsData(data=dataset, metadata=metadata)
