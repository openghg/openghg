from pathlib import Path
from openghg.modules import FOOTPRINTS


def get_datapath(filename):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/footprints/{filename}")


def test_read_footprint():
    # Take ever
    datapath = get_datapath("footprint_test.nc")
    model_params = {"simulation_params": "123"}
    res = FOOTPRINTS.read_file(data_filepath=datapath, site="TMB", network="LGHG", height="10magl", model_params=model_params)

    print(res)

def test_read_same_footprint_twice_raises():
    # Take ever
    datapath = get_datapath("footprint_test.nc")
    model_params = {"simulation_params": "123"}
    res = FOOTPRINTS.read_file(data_filepath=datapath, site="TMB", network="LGHG", height="10magl", model_params=model_params)

    res = FOOTPRINTS.read_file(data_filepath=datapath, site="TMB", network="LGHG", height="10magl", model_params=model_params)

    print(res)