from typing import Union
from pandas import Timestamp
from openghg.dataobjects import ObsData
from openghg.retrieve import get_obs_surface as proc_get_obs_surface
from openghg.util import running_in_cloud


def get_obs_surface(
    site: str,
    species: str,
    inlet: str = None,
    start_date: Union[str, Timestamp] = None,
    end_date: Union[str, Timestamp] = None,
    average: str = None,
    network: str = None,
    instrument: str = None,
    calibration_scale: str = None,
    keep_missing: bool = False,
    skip_ranking: bool = False,
) -> ObsData:
    """This is the equivalent of the get_obs function from the ACRG repository.

    Usage and return values are the same whilst implementation may differ.

    Args:
        site: Site of interest e.g. MHD for the Mace Head site.
        species: Species identifier e.g. ch4 for methane.
        start_date: Output start date in a format that Pandas can interpret
        end_date: Output end date in a format that Pandas can interpret
        inlet: Inlet label
        average: Averaging period for each dataset. Each value should be a string of
        the form e.g. "2H", "30min" (should match pandas offset aliases format).
        keep_missing: Keep missing data points or drop them.
        network: Network for the site/instrument (must match number of sites).
        instrument: Specific instrument for the sipte (must match number of sites).
        calibration_scale: Convert to this calibration scale
    Returns:
        list: List of ObsData objects
    """
    cloud = running_in_cloud()

    if cloud:
        raise NotImplementedError
    else:
        return _get_obs_surface_local(
            site=site,
            species=species,
            inlet=inlet,
            start_date=start_date,
            end_date=end_date,
            average=average,
            network=network,
            instrument=instrument,
            calibration_scale=calibration_scale,
            keep_missing=keep_missing,
            skip_ranking=skip_ranking,
        )


def _get_obs_surface_local(
    site: str,
    species: str,
    inlet: str = None,
    start_date: Union[str, Timestamp] = None,
    end_date: Union[str, Timestamp] = None,
    average: str = None,
    network: str = None,
    instrument: str = None,
    calibration_scale: str = None,
    keep_missing: bool = False,
    skip_ranking: bool = False,
) -> ObsData:

    return proc_get_obs_surface(
        site=site,
        species=species,
        start_date=start_date,
        end_date=end_date,
        inlet=inlet,
        average=average,
        network=network,
        instrument=instrument,
        calibration_scale=calibration_scale,
        keep_missing=keep_missing,
        skip_ranking=skip_ranking,
    )
