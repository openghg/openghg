import logging
from pathlib import Path
from typing import Any, Dict, Hashable, Optional, Union, cast
import xarray as xr

from openghg.standardise.meta import dataset_formatter
from openghg.types import optionalPathType
from openghg.util import check_and_set_null_variable, not_set_metadata_values

logger = logging.getLogger("openghg.standardise.surface")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler


def parse_co2_games(
    filepath: Union[str, Path],
    site: str,
    measurement_type: str,
    inlet: Optional[str] = None,
    network: str = "paris_simulation",
    instrument: Optional[str] = None,
    sampling_period: Optional[str] = None,
    update_mismatch: str = "never",
    site_filepath: optionalPathType = None,
    ** kwarg: Dict,
    ) -> Dict:
    """Read co2 verification games files.
       Current scope is for Paris required by Eric
    Args:
        filepath: Data filepath
        site: Three letter site code
        inlet: Inlet height (as value unit e.g. "10m")
        measurement_type: One of ("flask", "insitu", "pfp")
        network: Network, defaults to paris_simulation
        instrument: Instrument name
        sampling_period: Sampling period
        update_mismatch: This determines how mismatches between the internal data
            attributes and the supplied / derived metadata are handled.
            This includes the options:
                - "never" - don't update mismatches and raise an AttrMismatchError
                - "attributes" - update mismatches based on input attributes
                - "metadata" - update mismatches based on input metadata
        site_filepath: Alternative site info file (see openghg/openghg_defs repository for format).
            Otherwise will use the data stored within openghg_defs/data/site_info JSON file by default.
    Returns:
        dict: Dictionary of data and metadata
    """