from typing import Dict, List, Optional, Union

from openghg.dataobjects import ObsData
from openghg.retrieve.icos import retrieve as icos_retrieve
from openghg.retrieve.ceda import retrieve_surface
from openghg.util import running_in_cloud


def retrieve_ceda(
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
    Examples:
        To retrieve data from CEDA using a url

        >>> url = https://dap.ceda.ac.uk/badc/gauge/data/tower/heathfield/co2/100m/bristol-crds_heathfield_20130101_co2-100m.nc?download=1
        >>> data = retrieve_ceda(url=url)

        To retrieve previously downloaded data

        >>> data = retrieve_ceda(site="hfd", species="co2")
    """
    cloud = running_in_cloud()

    if cloud:
        raise NotImplementedError
    else:
        return retrieve_surface(
            site=site,
            species=species,
            inlet=inlet,
            url=url,
            force_retrieval=force_retrieval,
            additional_metadata=additional_metadata,
        )


def retrieve_icos(
    site: str,
    species: Optional[Union[str, List]] = None,
    sampling_height: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    data_level: int = 2,
    force_retrieval: bool = False,
) -> Union[ObsData, List[ObsData], None]:
    """Retrieve ICOS atmospheric measurement data. If data is found in the object store it is returned. Otherwise
    data will be retrieved from the ICOS Carbon Portal. Data retrieval from the Carbon Portal may take a short time.
    If only a single data source is found an ObsData object is returned, if multiple a list of ObsData objects
    if returned, if nothing then None.

    Args:
        site: Site code
        species: Species name
        start_date: Start date
        end_date: End date
        force_retrieval: Force the retrieval of data from the ICOS Carbon Portal
    Returns:
        ObsData, list[ObsData], None: ObsData or a list of ObsData objects if data found, else None
    """
    cloud = running_in_cloud()

    if cloud:
        raise NotImplementedError
    else:
        return icos_retrieve(
            site=site,
            species=species,
            sampling_height=sampling_height,
            start_date=start_date,
            end_date=end_date,
            force_retrieval=force_retrieval,
            data_level=data_level,
        )
