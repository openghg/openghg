from openghg.dataobjects import ObsData
from typing import Dict, List, Optional, Union


def retrieve_surface(
    site: Optional[str] = None,
    species: Optional[str] = None,
    inlet: Optional[str] = None,
    url: Optional[str] = None,
    force_retrieval: bool = False,
    additional_metadata: Optional[Dict] = None,
) -> Union[ObsData, List[ObsData], None]:
    """Retrieve surface observations data from the CEDA archive. You can pass
    search terms and the object store will be searched. To retrieve data from th
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
    """
    import io
    import xarray as xr
    from openghg.util import download_data
    from openghg.store import ObsSurface
    from openghg.retrieve import search
    from openghg.util import parse_url_filename, timestamp_now, site_code_finder

    if additional_metadata is None:
        additional_metadata = {}

    results = search(site=site, species=species, inlet=inlet, data_source="ceda_archive")

    if results and not force_retrieval or url is None:
        obs_data: Union[ObsData, List[ObsData]] = results.retrieve_all()
        return obs_data

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
        dataset = xr.open_dataset(buf).load()

    now = str(timestamp_now())

    key = f"{filename}_{now}"
    # We expect to be dealing with timeseries data here
    # We'll take the attributes as metadata
    metadata = dataset.attrs.copy()

    metadata["data_type"] = "timeseries"
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
