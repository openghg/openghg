import xarray as xr

from openghg.types import HasMetadataAndData
from openghg.dataobjects._basedata import _BaseData


def test_base_data_has_metadata_and_data():
    bd = _BaseData(metadata={}, data=xr.Dataset())
    assert isinstance(bd, HasMetadataAndData)
