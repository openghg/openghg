from pathlib import Path
import pytest

from openghg.modules import Emissions
from openghg.processing import search, recombine_datasets
from openghg.objectstore import get_local_bucket
from xarray import open_dataset


def get_datapath(filename):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/emissions/{filename}")


def test_read_file():
    get_local_bucket(empty=True)

    test_datapath = get_datapath("co2-gpp-cardamom-mth_EUROPE_2012.nc")

    proc_results = Emissions.read_file(
        filepath=test_datapath, species="co2", source="gpp-cardamom", date="2012", domain="europe", high_time_resolution=False
    )

    assert "co2_gppcardamom_europe_2012" in proc_results

    search_results = search(species="co2", source="gpp-cardamom", date="2012", domain="europe", data_type="emissions")

    key = list(search_results.keys())[0]

    data_keys = search_results[key]["keys"]
    emissions_data = recombine_datasets(keys=data_keys, sort=False)

    metadata = search_results[key]["metadata"]

    orig_data = open_dataset(test_datapath)

    assert orig_data.lat.equals(emissions_data.lat)
    assert orig_data.lon.equals(emissions_data.lon)
    assert orig_data.time.equals(emissions_data.time)
    assert orig_data.flux.equals(emissions_data.flux)

    expected_metadata = {
        "title": "gross primary productivity co2",
        "author": "openghg cloud",
        "date_created": "2018-05-20 19:44:14.968710",
        "number_of_prior_files_used": 1,
        "prior_file_1": "cardamom gpp",
        "prior_file_1_raw_resolution": "25x25km",
        "prior_file_1_reference": "t.l. smallman, jgr biogeosciences, 2017",
        "regridder_used": "acrg_grid.regrid.regrid_3d",
        "comments": "fluxes copied from year 2013. december 2012 values copied from january 2013 values.",
        "species": "co2",
        "domain": "europe",
        "source": "gppcardamom",
        "date": "2012",
        "start_date": "2012-12-01 00:00:00+00:00",
        "end_date": "2012-12-01 00:00:00+00:00",
        "max_longitude": 39.38,
        "min_longitude": -97.9,
        "max_latitude": 79.057,
        "min_latitude": 10.729,
        "time_resolution": "standard_resolution",
        "data_type": "emissions",
    }

    del metadata["processed"]
    del metadata["prior_file_1_version"]

    assert metadata == expected_metadata


def test_set_lookup_uuids():
    e = Emissions()

    fake_uuid = "123456789"

    species = "test_species"
    source = "test_source"
    domain = "test_domain"
    date = "test_date"

    e.set_uuid(species=species, source=source, domain=domain, date=date, uuid=fake_uuid)

    found_uid = e.lookup_uuid(species=species, source=source, domain=domain, date=date)

    assert e._datasource_table[species][source][domain][date] == found_uid == fake_uuid


def test_datasource_add_lookup():
    e = Emissions()

    fake_datasource = {"co2_gppcardamom_europe_2012": "mock-uuid-123456"}

    fake_metadata = {
        "co2_gppcardamom_europe_2012": {
            "species": "co2",
            "domain": "europe",
            "source": "gppcardamom",
            "date": "2012",
        }
    }

    e.add_datasources(datasource_uuids=fake_datasource, metadata=fake_metadata)

    assert e.datasources() == ['mock-uuid-123456']

    lookup = e.datasource_lookup(fake_metadata)

    assert lookup == {'co2_gppcardamom_europe_2012': 'mock-uuid-123456'}


def test_wrong_uuid_raises():
    e = Emissions()

    fake_datasource = {"co2_gppcardamom_europe_2012": "mock-uuid-123456"}

    fake_metadata = {
        "co2_gppcardamom_europe_2012": {
            "species": "co2",
            "domain": "europe",
            "source": "gppcardamom",
            "date": "2012",
        }
    }

    e.add_datasources(datasource_uuids=fake_datasource, metadata=fake_metadata)

    assert e.datasources() == ['mock-uuid-123456']

    changed_datasource = {"co2_gppcardamom_europe_2012": "mock-uuid-8888888"}

    with pytest.raises(ValueError):
        e.add_datasources(datasource_uuids=changed_datasource, metadata=fake_metadata)
