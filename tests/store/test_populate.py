from openghg.store import add_noaa_obspack
from helpers import get_datapath
import pytest

@pytest.mark.xfail(reason="Select which ObsPack attributes we want - currently have non JSON serialisable data - see #219 and #193")
def test_read_noaa_obspack():
    data_directory = get_datapath("ObsPack/data/nc", data_type="NOAA")
    print("***data_directory***", data_directory)
    add_noaa_obspack(data_directory)

    # TODO: Add checks on any output when added