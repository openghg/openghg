import os
import pytest

from openghg.store import ObsSurface
from openghg.objectstore import get_local_bucket
from openghg.client import search


@pytest.mark.skip(reason="Search class needs updating")
def test_search_and_download(crds):
    results = search(species="co2", site="hfd")

    raw_results = results.raw()

    assert len(raw_results["co2_hfd_100m_picarro"]["keys"]) == 5

    expected_metadata = {
        "site": "hfd",
        "instrument": "picarro",
        "time_resolution": "1_minute",
        "inlet": "100m",
        "port": "10",
        "type": "air",
        "species": "co2",
        "data_type": "timeseries",
        "scale": "wmo-x2007",
    }

    assert raw_results["co2_hfd_100m_picarro"]["metadata"] == expected_metadata

    data = results.retrieve(site="hfd", species="co2", inlet="100m")

    data = data["co2_hfd_100m_picarro"]

    assert data["co2"][0] == pytest.approx(414.21)
    assert data["co2_variability"][-1] == pytest.approx(0.247)
    assert data["co2_number_of_observations"][10] == 19.0
