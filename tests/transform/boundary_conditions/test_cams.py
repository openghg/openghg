from helpers import get_bc_datapath
import logging
import numpy as np

from openghg.transform.boundary_conditions import parse_cams

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


def test_parse_cams():
    """
    To test the parser for boundary condititons
    """
    bc_input = "cams_test"
    cams_version = "v22r1"
    domain = "europe"
    species = "n2o"
    period = "daily"

    data_path = get_bc_datapath(filename="cams73_v22r1_n2o_test_202201.nc")

    results = parse_cams(
        filepath=data_path,
        species=species,
        bc_input=bc_input,
        period=period,
        cams_version=cams_version,
        domain=domain,
    )

    # test metadata
    metadata = results[f"{species}_{bc_input}_{domain}"]["metadata"]
    expected_str_metadata = {
        "bc_input": bc_input,
        "CAMS_version": cams_version,
        "domain": domain,
        "species": species,
    }
    for k, v in expected_str_metadata.items():
        assert metadata[k].lower() == v.lower()
    expected_float_metadata = {
        "min_longitude": -97.9,
        "max_longitude": 39.38,
        "min_latitude": 10.729,
        "max_latitude": 79.057,
        "min_height": 500,
        "max_height": 19500,
    }
    for k, v in expected_float_metadata.items():
        assert float(metadata[k]) == float(v)

    # test data
    bc_data = results[f"{species}_{bc_input}_{domain}"]["data"].compute()
    assert np.isclose(bc_data["vmr_n"].values.mean(), 313.4994)
    assert np.isclose(bc_data["vmr_s"].values.mean(), 335.2557)
    assert np.isclose(bc_data["vmr_w"].values.mean(), 323.5323)
    assert np.isclose(bc_data["vmr_e"].values.mean(), 324.0306)

    assert bc_data.time.size == 3
    assert bc_data.time.values[0] == np.datetime64("2022-01-01T00:00")
