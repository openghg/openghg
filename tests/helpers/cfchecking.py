from logging import warning
import cfchecker
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Union
import xarray as xr
import warnings


def check_cf_compliance(dataset: xr.Dataset, debug: bool = False) -> bool:
    """Tests the compliance of the written NetCDF

    Args:
        dataset: xarray.Dataset
    Returns:
        bool: True if compliant
    """
    checker = cfchecker.cfchecks.CFChecker(debug=debug)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with NamedTemporaryFile(suffix=".nc") as tmpfile:
            dataset.to_netcdf(tmpfile.name)
            result = checker.checker(file=tmpfile.name)

            results = result["global"]

            fatal = results["FATAL"]
            errors = results["ERROR"]
            warn = results["WARN"]

            if fatal or errors:
                return False
            else:
                return True
