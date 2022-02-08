from openghg.client import process_files
from openghg.objectstore import get_local_bucket
from helpers import get_datapath


def test_process_files():
    hfd_path = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")

    results = process_files(
        files=hfd_path, site="hfd", instrument="picarro", network="DECC", data_type="CRDS", overwrite=True
    )

    results = results["processed"]["hfd.picarro.1minute.100m.min.dat"]

    assert "error" not in results
    assert "ch4" in results
    assert "co2" in results
