from typing import List, Optional, Union
from pandas import Timestamp
from openghg.dataobjects import ObsData
from openghg.processing import get_obs_surface as proc_get_obs_surface

__all__ = ["get_obs_surface"]


def get_obs_surface(
    site: str,
    species: str,
    start_date: Optional[Union[str, Timestamp]] = None,
    end_date: Optional[Union[str, Timestamp]] = None,
    inlet: Optional[str] = None,
    average: Optional[str] = None,
    network: Optional[str] = None,
    instrument: Optional[str] = None,
    calibration_scale: Optional[str] = None,
    keep_missing: Optional[bool] = False,
) -> List[ObsData]:
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
        instrument: Specific instrument for the site (must match number of sites).
        calibration_scale: Convert to this calibration scale
    Returns:
        list: List of ObsData objects
    """
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
    )
