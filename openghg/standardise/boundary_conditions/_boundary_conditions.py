import logging
from pathlib import Path
from typing import Any, Dict, Hashable, Optional, Union, cast
import xarray as xr

from openghg.standardise.meta import dataset_formatter
from openghg.types import optionalPathType
from openghg.util import check_and_set_null_variable, not_set_metadata_values

logger = logging.getLogger("openghg.standardise.boundary_conditions")
logger.setLevel(logging.DEBUG)  # Have to set level for logger as well as handler



def parse_boundary_conditions(filepath: Union[str, Path],
                              species: str,
                              bc_input: str,
                              domain: str,
                              source_format = "boundary_conditions"
                              ):


    try:


        bc_data = xr.open_dataset(filepath)

        parser_fn = load_standardise_parser(data_type=self._data_type, source_format=source_format)