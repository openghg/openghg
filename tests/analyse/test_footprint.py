import pytest
import numpy as np
from openghg.analyse import footprints_data_merge


def test_ch4_tac_coarsest_merge():
    """Test to check overall footprint data merge functionality
    when resampling to coarsest (footprint in this case)."""
    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    domain = "EUROPE"
    species = "ch4"
    network = "DECC"
    height = "100m"
    source = "anthro"

    CombinedData = footprints_data_merge(
        site=site,
        height=height,
        network=network,
        domain=domain,
        start_date=start_date,
        end_date=end_date,
        species=species,
        flux_sources=source,
        resample_to="coarsest",
        load_flux=True,
        calc_timeseries=True,
    )

    data = CombinedData.data

    assert "mf" in data
    assert "mf_mod" in data

    time_start = data["time"].values[0]
    time_end = data["time"].values[-1]

    assert time_start == np.datetime64("2012-08-01T00:00:00.000000000")
    assert time_end == np.datetime64("2012-08-31T22:00:00.000000000")


@pytest.mark.skip(reason="Takes a long time to run with current data - skip for now")
def test_ch4_tac_obs_merge():
    """Test to check overall footprint data merge functionality
    when resampling to observation data."""
    start_date = "2012-01-01"
    end_date = "2013-01-01"

    site = "tac"
    domain = "EUROPE"
    species = "ch4"
    network = "DECC"
    height = "100m"
    source = "anthro"

    CombinedData = footprints_data_merge(
        site=site,
        height=height,
        network=network,
        domain=domain,
        start_date=start_date,
        end_date=end_date,
        species=species,
        flux_sources=source,
        resample_to="obs",
        load_flux=True,
        calc_timeseries=True,
    )

    data = CombinedData.data

    assert "mf" in data
    assert "mf_mod" in data

    time_start = data["time"].values[0]
    time_end = data["time"].values[-1]

    assert time_start == np.datetime64("2012-08-01T00:00:30.000000000")
    assert time_end == np.datetime64("2012-08-31T23:47:30.000000000")
