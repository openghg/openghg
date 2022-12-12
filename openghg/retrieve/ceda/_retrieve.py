from typing import Any, Dict, List, Optional, Union

from openghg.dataobjects import ObsData
from openghg.util import running_on_hub


def retrieve_surface(
    site: Optional[str] = None,
    species: Optional[str] = None,
    inlet: Optional[str] = None,
    url: Optional[str] = None,
    force_retrieval: bool = False,
    additional_metadata: Optional[Dict] = None,
) -> Union[List[ObsData], ObsData, None]:
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
    )


def retrieve(**kwargs: Any) -> Union[List[ObsData], ObsData, None]:
    """Retrieve surface from the CEDA Archive. This function
    should not be used directly and is called by the retrieve_* functions,
    such as retrieve_surface, that retrieve specific data from the archive.

    To retrieve data from the CEDA Archive please browse the
    website (https://data.ceda.ac.uk/badc) to find the URL of the dataset to retrieve.

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
    Returns:
        ObsData or None: ObsData if data found / retrieved successfully.
    """
    from io import BytesIO

    from openghg.cloud import call_function, unpackage
    from xarray import load_dataset

    if running_on_hub():
        post_data: Dict[str, Union[str, Dict]] = {}
        post_data["function"] = "retrieve_ceda"
        post_data["arguments"] = kwargs

        call_result = call_function(data=post_data)

        content = call_result["content"]
        found = content["found"]

        if not found:
            return None

        observations = content["data"]

        obs_data = []
        for package in observations.values():
            unpackaged = unpackage(data=package)
            buf = BytesIO(unpackaged["data"])
            ds = load_dataset(buf)
            obs = ObsData(data=ds, metadata=unpackaged["metadata"])

            obs_data.append(obs)

        if len(obs_data) == 1:
            return obs_data[0]
        else:
            return obs_data
    else:
        return local_retrieve_surface(**kwargs)


def local_retrieve_surface(
    site: Optional[str] = None,
    species: Optional[str] = None,
    inlet: Optional[str] = None,
    url: Optional[str] = None,
    force_retrieval: bool = False,
    additional_metadata: Optional[Dict] = None,
    **kwargs: Any,
) -> Union[List[ObsData], ObsData, None]:
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

    results = search_surface(site=site, species=species, inlet=inlet, data_source="ceda_archive")

    if results and not force_retrieval or url is None:
        return results.retrieve_all()

    filename = parse_url_filename(url=url)
    extension = filename.split(".")[-1].lower()

    if extension != "nc":
        print("We can only currently retrieve and process NetCDF files.")
        return None

    binary_data = download_data(url=url)

    if binary_data is None:
        print("Error: No data retrieved.")
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
                    print("Unable to read site from additional_metadata.")
                    return None
            else:
                print("Error: cannot find site code, please pass additional metadata.")
                return None

        try:
            metadata["inlet"] = f"{int(metadata['inlet_height_magl'])}m"
        except KeyError:
            try:
                metadata["inlet"] = additional_metadata["inlet"]
            except KeyError:
                print("Unable to read inlet from data or additional_metadata.")
                return None

    to_store = {key: {"data": dataset, "metadata": metadata}}

    ObsSurface.store_data(data=to_store)

    return ObsData(data=dataset, metadata=metadata)
