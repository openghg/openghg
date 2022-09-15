from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from helpers import get_datapath
from openghg.dataobjects import SearchResults
from openghg.retrieve import search
from openghg.store import ObsSurface, rank_sources
from openghg.util import split_daterange_str
from pandas import Timestamp


@pytest.fixture(scope="session")
def load_CRDS():
    tac_100m = get_datapath("tac.picarro.1minute.100m.min.dat", data_type="CRDS")
    hfd_50m = get_datapath("hfd.picarro.1minute.50m.min.dat", data_type="CRDS")
    bsd_42m = get_datapath("bsd.picarro.1minute.42m.min.dat", data_type="CRDS")
    bsd_108m = get_datapath("bsd.picarro.1minute.108m.min.dat", data_type="CRDS")
    bsd_248m = get_datapath("bsd.picarro.1minute.248m.min.dat", data_type="CRDS")

    ObsSurface.read_file(filepath=tac_100m, data_type="CRDS", site="tac", network="DECC")
    ObsSurface.read_file(filepath=hfd_50m, data_type="CRDS", site="hfd", network="DECC")
    ObsSurface.read_file(filepath=[bsd_42m, bsd_108m, bsd_248m], data_type="CRDS", site="bsd", network="DECC")


def test_retrieve_all_bsd():
    results = search(site="bsd")

    print(results)

# @pytest.fixture(scope="session", autouse=False)
# def with_ranking():
#     pass


# def test_retrieve_unranked():
#     results = search(species="ch4", skip_ranking=True)

#     assert results.ranked_data is False
#     assert results.hub is False

#     raw_results = results.raw()
#     assert raw_results["tac"]["ch4"]["100m"]
#     assert raw_results["hfd"]["ch4"]["50m"]
#     assert raw_results["bsd"]["ch4"]["42m"]


# def test_inlet_retrieve_only_unranked():
#     results = search(species="ch4")

#     data_50m = results.retrieve(inlet="50m")

#     metadata = data_50m.metadata
#     assert metadata["inlet_height_magl"] == "50m"
#     assert metadata["site"] == "hfd"


# def test_site_retrieve_only_unranked():
#     results = search(species="ch4")

#     data_bsd = results.retrieve(site="bsd")

#     assert len(data_bsd) == 3

#     for obs in data_bsd:
#         assert obs.metadata["site"] == "bsd"


# def test_species_retrieve_only_unranked():
#     sites = ["tac", "bsd"]
#     results = search(site=sites)

#     co2_data = results.retrieve(species="co2")

#     assert len(co2_data) == 4

#     for obs in co2_data:
#         assert obs.metadata["site"] in sites


# def test_species_site_retrieve_only_unranked():
#     results = search(site="bsd")

#     bsd_data = results.retrieve(site="bsd", species="co2")

#     heights = ["108m", "248m", "42m"]

#     assert len(bsd_data) == 3

#     for obs in bsd_data:
#         inlet = obs.metadata["inlet"]
#         heights.remove(inlet)


# def test_site_inlet_retrieve_only_unranked():
#     results = search(site="tac")

#     tac_data = results.retrieve(site="tac", inlet="100m")

#     species = ["co2", "ch4"]

#     assert len(tac_data) == 2

#     for obs in tac_data:
#         s = obs.metadata["species"]

#         species.remove(s)


# def test_species_inlet_retrieve_only_unranked():
#     results = search(site=["bsd", "tac", "hfd"])

#     data_42m = results.retrieve(species="co2", inlet="42m")

#     assert len(data_42m) == 1

#     metadata = data_42m.metadata

#     assert metadata["site"] == "bsd"
#     assert metadata["inlet"] == "42m"
#     assert metadata["species"] == "co2"


# def test_retrieve_bad_search_terms():
#     results = search(site=["bsd", "hfd", "tac"])

#     data_invalid = results.retrieve(inlet="888m")

#     assert data_invalid is None

#     data_invalid = results.retrieve(site="londinium")

#     assert data_invalid is None

#     data_invalid = results.retrieve(species="sparrow")

#     assert data_invalid is None

#     data_invalid = results.retrieve(species="co2", site="londinium")

#     assert data_invalid is None

#     data_invalid = results.retrieve(species="co2", inlet="888m")

#     assert data_invalid is None

#     data_invalid = results.retrieve(site="londinium", inlet="42m")

#     assert data_invalid is None

#     data_invalid = results.retrieve(species="co2", site="bsd", inlet="-1m")

#     assert data_invalid is None


# def test_retrieve_all_unranked():
#     results = search(site=["bsd", "tac"], species=["ch4", "co"], inlet=["100m", "108m"])

#     all_data = results.retrieve_all()

#     expected = [("tac", "ch4", "100m"), ("bsd", "ch4", "108m"), ("bsd", "co", "108m")]
#     for obs in all_data:
#         site = obs.metadata["site"]
#         species = obs.metadata["species"]
#         inlet = obs.metadata["inlet"]

#         assert (site, species, inlet) in expected


# def test_retrieve_complex_ranked():
#     obs = ObsSurface.load()
#     obs._rank_data.clear()
#     obs.save()

#     rank = rank_sources(site="bsd", species="co")

#     rank.set_rank(inlet="42m", rank=1, start_date="2014-01-01", end_date="2015-03-01")
#     rank.set_rank(inlet="108m", rank=1, start_date="2015-03-02", end_date="2016-08-01")
#     rank.set_rank(inlet="42m", rank=1, start_date="2016-08-02", end_date="2017-03-01")
#     rank.set_rank(inlet="248m", rank=1, start_date="2017-03-02", end_date="2019-03-01")
#     rank.set_rank(inlet="108m", rank=1, start_date="2019-03-02", end_date="2021-12-01")

#     rank = rank_sources(site="bsd", species="co")
#     updated_res = rank.get_sources(site="bsd", species="co")

#     expected = {
#         "42m": {
#             "rank_data": {
#                 "2014-01-01-00:00:00+00:00_2015-03-01-00:00:00+00:00": 1,
#                 "2016-08-02-00:00:00+00:00_2017-03-01-00:00:00+00:00": 1,
#             },
#             "data_range": "2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00",
#         },
#         "108m": {
#             "rank_data": {
#                 "2015-03-02-00:00:00+00:00_2016-08-01-00:00:00+00:00": 1,
#                 "2019-03-02-00:00:00+00:00_2021-12-01-00:00:00+00:00": 1,
#             },
#             "data_range": "2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00",
#         },
#         "248m": {
#             "rank_data": {"2017-03-02-00:00:00+00:00_2019-03-01-00:00:00+00:00": 1},
#             "data_range": "2014-01-30-11:12:30+00:00_2020-12-01-22:32:29+00:00",
#         },
#     }

#     assert updated_res == expected

#     search_res = search(site="bsd", species="co")

#     obsdata = search_res.retrieve(site="bsd", species="co")

#     metadata = obsdata.metadata

#     expected_rank_metadata = {
#         "ranked": {
#             "2014-01-01-00:00:00+00:00_2015-03-01-00:00:00+00:00": "42m",
#             "2016-08-02-00:00:00+00:00_2017-03-01-00:00:00+00:00": "42m",
#             "2015-03-02-00:00:00+00:00_2016-08-01-00:00:00+00:00": "108m",
#             "2019-03-02-00:00:00+00:00_2021-12-01-00:00:00+00:00": "108m",
#             "2017-03-02-00:00:00+00:00_2019-03-01-00:00:00+00:00": "248m",
#         },
#         "unranked": {},
#     }
#     assert metadata["rank_metadata"] == expected_rank_metadata

#     measurement_data = obsdata.data

#     unique, count = np.unique(measurement_data.time, return_counts=True)
#     # Ensure there are no duplicates
#     assert unique[count > 1].size == 0

#     # Make sure the inlets have been written to the Dataset correctly
#     for daterange, inlet in expected_rank_metadata["ranked"].items():
#         start, end = split_daterange_str(daterange, date_only=True)
#         d = measurement_data.sel(time=slice(str(start), str(end)))

#         all_inlets = np.unique(d["inlet"])

#         assert all_inlets.size == 1
#         assert all_inlets[0] == inlet

#     assert measurement_data.time.size == 126

#     # Now we test that retrieve_all works correctly with ranked data
#     results = search(site="bsd", species="co")

#     all_data = results.retrieve_all()

#     metadata = all_data.metadata

#     expected_ranking_data = {
#         "ranked": {
#             "2014-01-01-00:00:00+00:00_2015-03-01-00:00:00+00:00": "42m",
#             "2016-08-02-00:00:00+00:00_2017-03-01-00:00:00+00:00": "42m",
#             "2015-03-02-00:00:00+00:00_2016-08-01-00:00:00+00:00": "108m",
#             "2019-03-02-00:00:00+00:00_2021-12-01-00:00:00+00:00": "108m",
#             "2017-03-02-00:00:00+00:00_2019-03-01-00:00:00+00:00": "248m",
#         },
#         "unranked": {},
#     }

#     assert metadata["rank_metadata"] == expected_ranking_data

#     ranking_data = results.rankings()

#     expected_rankings = {
#         "bsd": {
#             "co": {
#                 "2014-01-01-00:00:00+00:00_2015-03-01-00:00:00+00:00": "42m",
#                 "2016-08-02-00:00:00+00:00_2017-03-01-00:00:00+00:00": "42m",
#                 "2015-03-02-00:00:00+00:00_2016-08-01-00:00:00+00:00": "108m",
#                 "2019-03-02-00:00:00+00:00_2021-12-01-00:00:00+00:00": "108m",
#                 "2017-03-02-00:00:00+00:00_2019-03-01-00:00:00+00:00": "248m",
#             }
#         }
#     }

#     assert ranking_data == expected_rankings


# def test_inlet_gets_data_with_ranking():
#     # Clear the ObsSurface ranking data
#     obs = ObsSurface.load()
#     obs._rank_data.clear()
#     obs.save()

#     rank = rank_sources(site="bsd", species="co")

#     rank.set_rank(inlet="42m", rank=1, start_date="2014-01-01", end_date="2015-03-01")
#     rank.set_rank(inlet="108m", rank=1, start_date="2015-03-02", end_date="2016-08-01")
#     rank.set_rank(inlet="42m", rank=1, start_date="2016-08-02", end_date="2017-03-01")
#     rank.set_rank(inlet="248m", rank=1, start_date="2017-03-02", end_date="2019-03-01")
#     rank.set_rank(inlet="108m", rank=1, start_date="2019-03-02", end_date="2021-12-01")

#     results = search(species="co")

#     assert results.ranked_data is True

#     obsdata = results.retrieve(site="bsd", inlet="42m", species="co")

#     co_data = obsdata.data
#     metadata = obsdata.metadata

#     assert metadata["inlet"] == "42m"
#     assert metadata["site"] == "bsd"
#     assert metadata["species"] == "co"

#     assert co_data.time[0] == Timestamp("2014-01-30T11:12:30")
#     assert co_data.time[-1] == Timestamp("2020-12-01T22:31:30")


# def test_retrieve_empty_object():
#     empty = SearchResults(results={}, ranked_data=False)

#     result = empty.retrieve(site="test", species="hawk", inlet="888m")

#     assert result is None

#     result = empty.retrieve_all()

#     assert result is None

#     empty = SearchResults(results={}, ranked_data=True)

#     result_ranked = empty.retrieve(site="test", species="hawk")

#     assert result_ranked is None

#     result_ranked = empty.retrieve_all()

#     assert result_ranked is None


# def test_create_obsdata_no_data_raises():
#     empty = SearchResults(results={}, ranked_data=True)

#     with pytest.raises(ValueError):
#         _ = empty._create_obsdata(site="rome", inlet="-85m", species="ptarmigan")


# def test_search_result_retrieve_cloud_and_hub(monkeypatch, mocker, tmpdir):
#     monkeypatch.setenv("OPENGHG_HUB", "1")

#     df = pd.DataFrame({"A": range(0, 64), "B": range(0, 64)}, columns=list("AB"))
#     ds = df.to_xarray()

#     p = Path(tmpdir).joinpath("test.nc")
#     ds.to_netcdf(p)

#     ds_bytes = Path(str(tmpdir)).joinpath("test.nc").read_bytes()

#     function_reply = {}
#     function_reply["content"] = {"data": ds_bytes}
#     function_reply["status"] = 200

#     mock_call = mocker.patch("openghg.cloud.call_function", return_value=function_reply)

#     s = SearchResults(
#         results={
#             "site": {"species": {"inlet": {"keys": {"unranked": "other"}, "metadata": {"some": "metadata"}}}}
#         },
#         ranked_data=False,
#     )

#     retrieved = s.retrieve_all()

#     assert retrieved.data.equals(ds)
#     assert mock_call.call_count == 1

#     s = SearchResults(
#         results={
#             "site": {"species": {"inlet": {"keys": {"unranked": "other"}, "metadata": {"some": "metadata"}}}}
#         },
#         ranked_data=False,
#     )

#     retrieved = s.retrieve_all()

#     assert retrieved.data.equals(ds)
#     assert mock_call.call_count == 2
