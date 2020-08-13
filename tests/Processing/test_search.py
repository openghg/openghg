import os
import pytest

from HUGS.Modules import CRDS, GC
from HUGS.Processing import search
from HUGS.ObjectStore import get_local_bucket
# from HUGS.Util import get_datetime


@pytest.fixture(scope="session")
def gc_obj():
    data_file = "capegrim-medusa.18.C"
    prec_file = "capegrim-medusa.18.precisions.C"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/GC"
    data_filepath = os.path.join(dir_path, test_data, data_file)
    prec_filepath = os.path.join(dir_path, test_data, prec_file)

    GC.read_file(
        data_filepath=data_filepath,
        precision_filepath=prec_filepath,
        site="capegrim",
        source_name="capegrim-medusa.18",
        instrument_name="medusa",
    )


@pytest.fixture(scope="session")
def crds_obj():
    filename = "bsd.picarro.1minute.248m.dat"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/CRDS"
    filepath = os.path.join(dir_path, test_data, filename)

    return CRDS.read_file(filepath, source_name="bsd.picarro.1minute.248m")


@pytest.fixture(scope="session")
def crds_read():
    get_local_bucket(empty=True)
    test_data = "../data/search_data"
    folder_path = os.path.join(os.path.dirname(__file__), test_data)
    CRDS.read_folder(folder_path=folder_path)


def test_search_GC():
    data_file = "capegrim-medusa.18.C"
    prec_file = "capegrim-medusa.18.precisions.C"
    dir_path = os.path.dirname(__file__)
    test_data = "../data/proc_test_data/GC"
    data_filepath = os.path.join(dir_path, test_data, data_file)
    prec_filepath = os.path.join(dir_path, test_data, prec_file)

    GC.read_file(
        data_filepath=data_filepath,
        precision_filepath=prec_filepath,
        site="CGO",
        source_name="capegrim-medusa",
        instrument_name="medusa",
    )

    results = search(
        species=["NF3"], locations="capegrim", data_type="GC", find_all=False,
    )

    nf3_results = results["NF3_CGO_75m_4"]

    metadata = {
        "site": "cgo",
        "instrument": "medusa",
        "species": "nf3",
        "units": "ppt",
        "scale": "sio-12",
        "inlet": "75m_4",
        "data_type": "timeseries",
    }

    assert "2018-01-01-02:24:00_2018-01-31-23:33:00" in nf3_results["keys"]
    assert nf3_results["metadata"] == metadata


def test_location_search(crds_read):
    species = ["co2", "ch4"]
    locations = ["bsd", "hfd", "tac"]

    data_type = "CRDS"
    start = None  # get_datetime(year=2016, month=1, day=1)
    end = None  # get_datetime(year=2017, month=1, day=1)

    results = search(
        species=species,
        locations=locations,
        data_type=data_type,
        find_all=False,
        start_datetime=start,
        end_datetime=end,
    )

    results_list = sorted(list(results.keys()))

    expected = sorted(
        [
            "ch4_bsd_248m",
            "ch4_bsd_108m",
            "co2_bsd_248m",
            "co2_bsd_108m",
            "ch4_hfd_100m",
            "co2_hfd_100m",
            "ch4_tac_100m",
            "co2_tac_100m",
        ]
    )

    assert results_list == expected

    # assert len(results["co2_bsd_108m"]["keys"]) == 23
    # assert len(results["co2_hfd_100m"]["keys"]) == 25
    # assert len(results["co2_tac_100m"]["keys"]) == 30
    # assert len(results["ch4_bsd_108m"]["keys"]) == 23
    # assert len(results["ch4_hfd_100m"]["keys"]) == 25
    # assert len(results["ch4_tac_100m"]["keys"]) == 30


# def test_search_datetimes():
#     data_type = "CRDS"
#     species = ["co2"]
#     locations = ["bsd"]

#     start = get_datetime(year=2016, month=1, day=1)
#     end = get_datetime(year=2017, month=1, day=1)

#     results = search(
#         species=species,
#         locations=locations,
#         data_type=data_type,
#         find_all=False,
#         start_datetime=start,
#         end_datetime=end,
#     )

#     result_keys = results["co2_bsd_108m"]["keys"]

#     expected_date_strings = [
#         "2016-01-19-17:17:30+00:00_2016-12-31-23:52:30+00:00",
#         "2016-03-01-02:22:30+00:00_2016-05-31-22:15:30+00:00",
#         "2016-06-01-00:23:30+00:00_2016-08-31-23:58:30+00:00",
#         "2016-09-01-02:48:30+00:00_2016-11-30-22:57:30+00:00",
#     ]

#     date_strings = sorted([v.split("/")[-1] for v in result_keys])

#     assert date_strings == expected_date_strings

#     metadata = results["co2_bsd_108m"]["metadata"]

#     expected_metadata = {
#         "site": "bsd",
#         "instrument": "picarro",
#         "time_resolution": "1_minute",
#         "inlet": "108m",
#         "port": "9",
#         "type": "air",
#         "species": "co2",
#         "data_type": "timeseries",
#     }

#     assert metadata == expected_metadata


# def test_search_find_all():
#     data_type = "CRDS"
#     species = ["co2"]
#     locations = ["bsd"]
#     inlet = "108m"
#     instrument = "picarro"

#     start = get_datetime(year=2016, month=1, day=1)
#     end = get_datetime(year=2017, month=1, day=1)

#     results = search(
#         species=species,
#         locations=locations,
#         data_type=data_type,
#         find_all=True,
#         start_datetime=start,
#         end_datetime=end,
#         inlet=inlet,
#         instrument=instrument
#     )

#     bsd_results = results["co2_bsd_picarro_108m"]

#     assert bsd_results["metadata"]["site"] == "bsd"
#     assert bsd_results["metadata"]["species"] == "co2"
#     assert bsd_results["metadata"]["time_resolution"] == "1_minute"

#     key_dates = sorted([daterange.split("/")[-1] for daterange in bsd_results["keys"]])

#     assert key_dates == sorted(["2016-01-19-17:17:30+00:00_2016-12-31-23:52:30+00:00",
#                                 "2016-06-01-00:23:30+00:00_2016-08-31-23:58:30+00:00",
#                                 "2016-03-01-02:22:30+00:00_2016-05-31-22:15:30+00:00",
#                                 "2016-09-01-02:48:30+00:00_2016-11-30-22:57:30+00:00"])


def test_search_bad_datatype_raises():
    data_type = "foo"
    species = ["spam", "eggs", "terry"]
    locations = ["capegrim"]

    with pytest.raises(KeyError):
        search(species=species, locations=locations, data_type=data_type)


def test_search_bad_site_raises():
    data_type = "foo"
    species = ["spam", "eggs", "terry"]
    locations = ["tintagel"]

    with pytest.raises(ValueError):
        search(species=species, locations=locations, data_type=data_type)


def test_search_nonsense_terms():
    data_type = "CRDS"
    species = ["spam", "eggs", "terry"]
    locations = ["capegrim"]

    results = search(species=species, locations=locations, data_type=data_type)

    assert not results


# def test_search_footprints():
#     test_data = "../data/emissions"
#     filename = "WAO-20magl_EUROPE_201306_downsampled.nc"
#     filepath = os.path.join(os.path.dirname(__file__), test_data, filename)
#     source_name = "WAO-20magl_EUROPE"
#     Footprint.read_file(filepath=filepath, source_name=source_name)

#     data_type = "footprint"
#     species = ["WAO"]
#     locations = []

#     expected_metadata = {
#         "name": "WAO-20magl_EUROPE",
#         "data_variables": [
#             "fp",
#             "temperature",
#             "pressure",
#             "wind_speed",
#             "wind_direction",
#             "PBLH",
#             "release_lon",
#             "release_lat",
#             "particle_locations_n",
#             "particle_locations_e",
#             "particle_locations_s",
#             "particle_locations_w",
#         ],
#         "coordinates": ["time", "lon", "lat", "lev", "height"],
#         "data_type": "footprint",
#     }

#     results = search(
#         species=species, locations=locations, data_type=data_type, find_all=True,
#     )

#     expected_start = "2013-06-02-00:00:00+00:00"
#     expected_end = "2013-06-30-00:00:00+00:00"

#     assert results["WAO"]["metadata"] == expected_metadata
#     assert results["WAO"]["start_date"] == expected_start
#     assert results["WAO"]["end_date"] == expected_end
