import cfchecker
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Union
import xarray as xr


def test_cf_compliance(dataset: xr.Dataset) -> bool:
    """Tests the compliance of the written NetCDF

    Args:
        dataset: xarray.Dataset
    Returns:
        bool: True if compliant
    """
    checker = cfchecker.cfchecks.CFChecker(debug=True)

    with NamedTemporaryFile(suffix=".nc") as tmpfile:
        dataset.to_netcdf(tmpfile.name)
        result = checker.checker(file=tmpfile.name)

        return result
