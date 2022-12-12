import warnings
from tempfile import NamedTemporaryFile

import xarray as xr


def check_cf_compliance(dataset: xr.Dataset, debug: bool = False) -> bool:
    """Tests the compliance of the written NetCDF

    Args:
        dataset: xarray.Dataset
    Returns:
        bool: True if compliant
    """
    from cfchecker import CFChecker

    checker = CFChecker(debug=debug, version="1.8")

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with NamedTemporaryFile(suffix=".nc") as tmpfile:
            dataset.to_netcdf(tmpfile.name)
            result = checker.checker(file=tmpfile.name)

            results = result["global"]

            fatal = results["FATAL"]
            errors = results["ERROR"]

            if fatal or errors:
                return False
            else:
                return True
