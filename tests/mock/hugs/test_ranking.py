from pathlib import Path
import pytest

from HUGS.Client import RankSources
from HUGS.Modules import CRDS
from HUGS.ObjectStore import get_local_bucket


@pytest.fixture(autouse=True)
def crds():
    get_local_bucket(empty=True)
    filename = "hfd.picarro.1minute.100m_min.dat"

    filepath = (
        Path(__file__)
        .resolve()
        .parent.joinpath("../data/proc_test_data/CRDS/")
        .joinpath(filename)
    )

    CRDS.read_file(data_filepath=filepath, source_name="hfd_picarro_100m", site="hfd")
    crds = CRDS.load()

    return crds


def test_get_sources(crds):
    r = RankSources()
    sources = r.get_sources(site="hfd", species="co2")
    print(sources)
