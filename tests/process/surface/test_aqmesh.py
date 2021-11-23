from openghg.process.surface import read_aqmesh
from pandas import Timestamp
from helpers import get_datapath


def test_aqmesh_read():
    datafile = get_datapath(filename="co2_data.csv", data_type="AQMesh")
    metafile = get_datapath(filename="co2_metadata.csv", data_type="AQMesh")

    data = read_aqmesh(data_filepath=datafile, metadata_filepath=metafile)

    site_data = data["briarroadclydebank"]

    dataset = site_data["data"]
    metadata = site_data["metadata"]

    assert dataset.time[0] == Timestamp("2021-06-16T01:00:00")
    assert dataset.co2[0] == 413.76
    assert dataset.time[-1] == Timestamp("2021-10-01")
    assert dataset.co2[1] == 415.11

    expected_metadata = {
        "site": "briarroadclydebank",
        "pod_id": 11245,
        "start_date": "2021-06-16 01:00:00",
        "end_date": "2021-10-04 00:59:00",
        "relocate_date": "NA",
        "long_name": "Briar Road Clydebank",
        "borough": "Glasgow",
        "site_type": "Roadside",
        "in_ulez": "No",
        "latitude": 55.91796,
        "longitude": -4.406231,
        "inlet": 1,
        "network": "aqmesh_glasgow",
        "sampling_period": "NA",
        "species": "co2",
        "units": "ppm"
    }

    assert metadata == expected_metadata
