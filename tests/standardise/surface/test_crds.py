import logging
import pytest
import tempfile

from openghg.standardise.surface import parse_crds

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)

from helpers import get_datapath, metadata_checker_obssurface, attributes_checker_obssurface


def test_read_file():
    hfd_filepath = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")

    gas_data = parse_crds(data_filepath=hfd_filepath, site="hfd", network="DECC")

    ch4_data = gas_data["ch4"]["data"]
    co2_data = gas_data["co2"]["data"]
    co_data = gas_data["co"]["data"]

    assert ch4_data["ch4"][0].values == pytest.approx(1993.83)
    assert ch4_data["ch4_variability"][0].values == pytest.approx(1.555)
    assert ch4_data["ch4_number_of_observations"][0].values == pytest.approx(19.0)

    assert co2_data["co2"][0] == pytest.approx(414.21)
    assert co2_data["co2_variability"][0] == pytest.approx(0.109)
    assert co2_data["co2_number_of_observations"][0] == pytest.approx(19.0)

    assert co_data["co"][0] == pytest.approx(214.28)
    assert co_data["co_variability"][0] == pytest.approx(4.081)
    assert co_data["co_number_of_observations"][0] == pytest.approx(19.0)

    for species, data in gas_data.items():
        metadata = data["metadata"]
        attributes = data["data"].attrs

        assert metadata_checker_obssurface(metadata=metadata)
        assert attributes_checker_obssurface(attrs=attributes)


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
