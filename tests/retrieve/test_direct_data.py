import xarray as xr

from helpers import get_flux_datapath
from openghg.retrieve import get_flux
from openghg.standardise import standardise_flux


def test_retrieve_flux_added_from_direct_dataset():
    """Test data supplied directly can be stored and retrieved from the object store."""
    filepath = get_flux_datapath("co2-gpp-cardamom_EUROPE_2013.nc")
    source = "direct-dataset-test"

    with xr.open_dataset(filepath) as dataset:
        expected_time = dataset.time[0].values
        standardise_flux(
            data=dataset.load(),
            store="user",
            species="co2",
            source=source,
            domain="EUROPE",
        )

    retrieved = get_flux(species="co2", source=source, domain="europe")

    assert retrieved.metadata["source"] == source
    assert retrieved.data.time[0].values == expected_time
    assert "flux" in retrieved.data
