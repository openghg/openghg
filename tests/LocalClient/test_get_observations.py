import pytest
from pathlib import Path

from HUGS.LocalClient import get_single_site
from HUGS.Modules import ObsSurface
from HUGS.ObjectStore import get_local_bucket


def get_datapath(filename, data_type):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")


@pytest.fixture(scope="session", autouse=True)
def crds():
    get_local_bucket(empty=True)

    filename = "hfd.picarro.1minute.100m.min.dat"
    filepath = get_datapath(filename=filename, data_type="CRDS")

    ObsSurface.read_file(filepath=filepath, data_type="CRDS")


def test_get_single_site_few_args():
    data = get_single_site(site="hfd", species="co2")

    print(data)

