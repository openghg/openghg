from typing import List, Optional, Union

from openghg.dataobjects import ObsData
from openghg.retrieve.icos import retrieve as icos_retrieve
from openghg.util import running_in_cloud


def retrieve_icos(
    site: str,
    species: Optional[Union[str, List]] = None,
    sampling_height: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
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
        ObsData, list[ObsData] or None
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
        )
