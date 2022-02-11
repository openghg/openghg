from pathlib import Path
from openghg.store import EulerianModel
from openghg.retrieve import search
from openghg.store import recombine_datasets
from openghg.objectstore import get_local_bucket
from xarray import open_dataset


def get_datapath(filename):
    return Path(__file__).resolve(strict=True).parent.joinpath(f"../data/eulerian_model/{filename}")


def test_read_file():
    get_local_bucket(empty=True)

    test_datapath = get_datapath("GEOSChem.SpeciesConc.20150101_0000z_reduced.nc4")

    proc_results = EulerianModel.read_file(filepath=test_datapath, model="GEOSChem", species="ch4")

    assert "geoschem_ch4_2015-01-01" in proc_results

    search_results = search(
        species="ch4", model="geoschem", start_date="2015-01-01", data_type="eulerian_model"
    )

    key = list(search_results.keys())[0]

    data_keys = search_results[key]["keys"]
    eulerian_data = recombine_datasets(keys=data_keys, sort=False)

    metadata = search_results[key]["metadata"]

    orig_data = open_dataset(test_datapath)

    assert orig_data["lat"].equals(eulerian_data["lat"])
    assert orig_data["lon"].equals(eulerian_data["lon"])
    assert orig_data["time"].equals(eulerian_data["time"])
    assert orig_data["lev"].equals(eulerian_data["lev"])
    assert orig_data["SpeciesConc_CH4"].equals(eulerian_data["SpeciesConc_CH4"])

    expected_metadata_values = {
        "species": "ch4",
        "date": "2015-01-01",
        "start_date": "2015-01-01 00:00:00+00:00",
        "end_date": "2016-01-01 00:00:00+00:00",
        "max_longitude": 175.0,
        "min_longitude": -180.0,
        "max_latitude": 89.0,
        "min_latitude": -89.0,
    }

    for key, expected_value in expected_metadata_values.items():
        assert metadata[key] == expected_value
