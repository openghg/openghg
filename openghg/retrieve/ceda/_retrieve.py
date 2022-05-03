from openghg.dataobjects import ObsData
from typing import List, Optional, Union


def retrieve_surface(
    site: Optional[str] = None,
    species: Optional[str] = None,
    inlet: Optional[str] = None,
    url: Optional[str] = None,
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
    Returns:
        ObsData or None: ObsData if data found / retrieved successfully.

    """
    import io
    import xarray as xr
    from openghg.util import download_data
    from openghg.store import ObsSurface
    from openghg.retrieve import search
    from openghg.util import parse_url_filename, timestamp_now

    if url is None:
        results = search(site=site, species=species, inlet=inlet, data_source="ceda_archive")

        if results:
            obs_data: Union[ObsData, List[ObsData]] = results.retrieve_all()
            return obs_data
        else:
            print(
                "No results found, please try with other search terms or pass the data "
                + "URL from the CEDA website to retrieve this data"
            )
            return None

    filename = parse_url_filename(url=url)
    extension = filename.split(".")[-1].lower()

    if extension != "nc":
        print("We can only currently retrieve and process NetCDF files.")
        return None

    binary_data = download_data(url=url)

    if binary_data is None:
        return None

    with io.BytesIO(binary_data) as buf:
        dataset = xr.open_dataset(buf)

    now = str(timestamp_now())

    key = f"{filename}_{now}"
    # We expect to be dealing with timeseries data here
    # We'll take the attributes as metadata
    metadata = dataset.attrs.copy()

    metadata["data_type"] = "timeseries"
    metadata["data_source"] = "ceda_archive"

    # If we're going to be using site, species and inlet here we should check that that
    # information is in the metadata
    to_store = {key: {"data": dataset, "metadata": metadata}}

    ObsSurface.store_data(data=to_store)

    return ObsData(data=dataset, metadata=metadata)
