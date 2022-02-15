# import pytest
# import os

# from openghg.client import process_files
# from openghg.objectstore import get_local_bucket

# from helpers import get_datapath


# # @pytest.fixture(autouse=True, scope="module")
# # def run_before_tests(monkeypatch):
# #     # get_local_bucket(empty=True)
# #     monkeypatch.setenv("OPENGHG_CLOUD", "TESTCLOUD")

# @pytest.fixture()
# def set_env(monkeypatch):
#     monkeypatch.setenv("OPENGHG_CLOUD", "TESTCLOUD")


# @pytest.mark.skip("Marked for removal")
# def test_process_CRDS_files(authenticated_user):
#     get_local_bucket(empty=True)

#     service_url = "openghg"

#     bsd_file = get_datapath(filename="bsd.picarro.1minute.248m.min.dat", data_type="CRDS")

#     response = process_files(
#         user=authenticated_user,
#         files=bsd_file,
#         data_type="CRDS",
#         site="bsd",
#         network="DECC",
#         service_url=service_url,
#     )

#     processed_species = response["processed"]["bsd.picarro.1minute.248m.min.dat"]

#     assert sorted(list(processed_species.keys())) == ["ch4", "co", "co2"]

# @pytest.mark.skip("Marked for removal")
# def test_process_CRDS_incorrect_args(authenticated_user, set_env):
#     hfd_file = get_datapath(filename="hfd.picarro.1minute.100m.min.dat", data_type="CRDS")

#     response = process_files(
#         user=authenticated_user,
#         files=hfd_file,
#         data_type="CRDS",
#         site="bsd",
#         network="DECC",
#         service_url="openghg",
#     )

#     assert "ValueError: Site mismatch between passed site code and that read from filename." in (
#         response["hfd.picarro.1minute.100m.min.dat"]
#     )

#     with pytest.raises(TypeError):
#         response = process_files(
#             user=authenticated_user,
#             files=hfd_file,
#             data_type="GCWERKS",
#             site="bsd",
#             network="DECC",
#             service_url="openghg",
#         )

#     response = process_files(
#         user=authenticated_user,
#         files=hfd_file,
#         data_type="CRDS",
#         site="hfd",
#         network="DECC",
#         service_url="openghg",
#     )

#     processed_species = response["processed"]["hfd.picarro.1minute.100m.min.dat"]

#     assert sorted(list(processed_species.keys())) == ["ch4", "co", "co2"]

# @pytest.mark.skip("Marked for removal")
# def test_process_GCWERKS_files(authenticated_user):
#     # Get the precisin filepath
#     data = get_datapath("capegrim-medusa.18.C", "GC")
#     precisions = get_datapath("capegrim-medusa.18.precisions.C", "GC")

#     filepaths = [(data, precisions)]

#     response = process_files(
#         user=authenticated_user,
#         files=filepaths,
#         data_type="GCWERKS",
#         site="cgo",
#         network="AGAGE",
#         instrument="medusa",
#         service_url="openghg",
#     )

#     cgo_response = response["processed"]["capegrim-medusa.18.C"]

#     partial_expected_keys = ["c2cl4_70m", "c2f6_70m", "c2h2_70m", "c2h6_70m", "c2hcl3_70m"]

#     assert len(cgo_response.keys()) == 56
#     assert sorted(cgo_response.keys())[:5] == partial_expected_keys
