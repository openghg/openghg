import logging
import pytest
import tempfile

from openghg.standardise.surface import parse_crds
from helpers import check_cf_compliance

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

from helpers import get_datapath, parsed_surface_metachecker


@pytest.fixture(scope="session")
def crds_data():
    hfd_filepath = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")

    gas_data = parse_crds(data_filepath=hfd_filepath, site="hfd", network="DECC")

    return gas_data


def test_read_file(crds_data):
    ch4_data = crds_data["ch4"]["data"]
    co2_data = crds_data["co2"]["data"]
    co_data = crds_data["co"]["data"]

    assert ch4_data["ch4"][0].values == pytest.approx(1993.83)
    assert ch4_data["ch4_variability"][0].values == pytest.approx(1.555)
    assert ch4_data["ch4_number_of_observations"][0].values == pytest.approx(19.0)

    assert co2_data["co2"][0] == pytest.approx(414.21)
    assert co2_data["co2_variability"][0] == pytest.approx(0.109)
    assert co2_data["co2_number_of_observations"][0] == pytest.approx(19.0)

    assert co_data["co"][0] == pytest.approx(214.28)
    assert co_data["co_variability"][0] == pytest.approx(4.081)
    assert co_data["co_number_of_observations"][0] == pytest.approx(19.0)

    parsed_surface_metachecker(data=crds_data)

@pytest.mark.skip_if_no_cfchecker
@pytest.mark.cfchecks
def test_crds_cf_compliance(crds_data):
    ch4_data = crds_data["ch4"]["data"]
    assert check_cf_compliance(dataset=ch4_data)


def test_bad_file_raises():
    with tempfile.NamedTemporaryFile() as tmpfile:
        filepath = str(tmpfile)

        with pytest.raises(ValueError):
            parse_crds(
                data_filepath=filepath,
                site="tac",
                network="DECC",
                inlet="30m",
                instrument="inst",
            )
