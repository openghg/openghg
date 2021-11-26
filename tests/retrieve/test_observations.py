import pytest
from openghg.standardise.surface import parse_gcwerks
from openghg.retrieve._access import get_obs_surface
from openghg.dataobjects._obsdata import ObsData


from helpers import get_datapath


def obs_setup():

    cgo_path = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
    cgo_prec_path = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

    parse_gcwerks(
        data_filepath=cgo_path,
        precision_filepath=cgo_prec_path,
        site="CGO",
        instrument="medusa",
        network="agage",
    )


def test_get_obs_surface_one_inlet():

    obs_setup()

    data = get_obs_surface(site="cgo", species="cfc112")

    assert isinstance(data, ObsData)
