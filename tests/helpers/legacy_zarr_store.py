"""Write and reopen a real TAC datasource using the pre-upgrade dependencies."""

import sys

import xarray as xr
import zarr

from openghg.objectstore import Datasource
from openghg.standardise.surface import parse_crds


def main():
    action, bucket, filepath, uuid = sys.argv[1:]
    assert zarr.__version__ == "2.18.3"
    assert xr.__version__ == "2025.9.1"
    parsed = parse_crds(filepath=filepath, site="tac", network="DECC")["ch4"]
    observations = parsed["data"].isel(time=slice(0, 24))

    if action == "write":
        datasource = Datasource(bucket=bucket, uuid=uuid, data_type="surface")
        datasource._store.encoding = {name: {"chunks": (7,)} for name in observations.data_vars}
        datasource.add_data(
            data=observations.isel(time=slice(0, 12)),
            metadata=parsed["metadata"],
            data_type="surface",
        )
        datasource.save()
    else:
        datasource = Datasource.load(bucket=bucket, uuid=uuid, mode="r")
        xr.testing.assert_equal(datasource.get_data("v1"), observations.isel(time=slice(0, 12)))
        # Version 2 was appended and corrected with rolling averages after upgrading.
        expected = observations.copy(deep=True)
        for name in expected.data_vars:
            if observations[name].dtype.kind != "f":
                continue
            expected[name][dict(time=[1, 2, 8, 9])] = (
                observations[name].rolling(time=3, min_periods=1).mean().isel(time=[1, 2, 8, 9])
            )
        xr.testing.assert_equal(datasource.get_data("v2"), expected)


if __name__ == "__main__":
    main()
