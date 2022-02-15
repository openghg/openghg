import os
import pytest
from openghg.client import rank_sources, search


def test_set_rank(process_crds):
    r = rank_sources(site="bsd", species="co2")

    results = r.raw()

    expected_results = {
        "248m": {"rank_data": "NA", "data_range": "2014-01-30 11:12:30+00:00_2020-12-01 22:31:30+00:00"},
        "42m": {"rank_data": "NA", "data_range": "2014-01-30 11:12:30+00:00_2020-12-01 22:31:30+00:00"},
        "108m": {"rank_data": "NA", "data_range": "2014-01-30 11:12:30+00:00_2020-12-01 22:31:30+00:00"},
    }

    assert results == expected_results

    r.set_rank(inlet="42m", rank=1, start_date="2001-01-01", end_date="2007-01-01")

    specific_results = r.get_specific_source(inlet="42m")

    assert specific_results == {"2001-01-01-00:00:00+00:00_2007-01-01-00:00:00+00:00": 1}


def test_clear_rank(process_crds):
    r = rank_sources(site="bsd", species="co2")

    r.set_rank(inlet="42m", rank=1, start_date="2001-01-01", end_date="2007-01-01")

    r.clear_rank(inlet="42m")

    specific_result = r.get_specific_source(inlet="42m")

    assert specific_result == "NA"

    results = r.get_sources(site="bsd", species="co2")

    assert results["42m"]["rank_data"] == "NA"

    with pytest.raises(ValueError):
        r.clear_rank(inlet="42m")
