# import pytest
# from openghg.client import process_files, search
# from openghg.objectstore import get_local_bucket
# from helpers import get_datapath, glob_files, metadata_checker_obssurface


# @pytest.fixture(scope="session")
# def read_data(authenticated_user):
#     get_local_bucket(empty=True)

#     data = get_datapath(filename="capegrim-medusa.18.C", data_type="GC")
#     precision = get_datapath(filename="capegrim-medusa.18.precisions.C", data_type="GC")

#     gc_files = (data, precision)

#     process_files(
#         user=authenticated_user,
#         files=gc_files,
#         site="cgo",
#         network="AGAGE",
#         data_type="GCWERKS",
#         service_url="openghg",
#     )

<<<<<<< HEAD
#     bsd_file = get_datapath(
#         filename="bsd.picarro.1minute.248m.min.dat", data_type="CRDS"
#     )
#     hfd_files = glob_files(search_str="hfd.picarro.1minute", data_type="CRDS")
=======
    bsd_file = get_datapath(filename="bsd.picarro.1minute.248m.min.dat", data_type="CRDS")
    hfd_files = glob_files(search_str="hfd.picarro.1minute", data_type="CRDS")
>>>>>>> devel

#     process_files(
#         user=authenticated_user,
#         files=bsd_file,
#         site="bsd",
#         network="DECC",
#         data_type="CRDS",
#         service_url="openghg",
#     )

#     process_files(
#         user=authenticated_user,
#         files=hfd_files,
#         site="hfd",
#         network="DECC",
#         data_type="CRDS",
#         service_url="openghg",
#     )

<<<<<<< HEAD
# @pytest.mark.skip("Marked for removal")
# def test_search(read_data):
#     species = "co2"
#     site = "bsd"
=======

@pytest.mark.skip("Marked for removal")
def test_search(read_data):
    species = "co2"
    site = "bsd"
>>>>>>> devel

#     results = search(species=species, site=site, inlet="248m")

#     raw_results = results.raw()

#     assert len(raw_results["bsd"]["co2"]["248m"]["keys"]) == 7

#     metadata = raw_results["bsd"]["co2"]["248m"]["metadata"]

#     metadata_checker_obssurface(metadata=metadata, species="co2")

#     results = search.search(site="hfd", species="co", skip_ranking=True)

#     raw_results = results.raw()

#     metadata = raw_results["hfd"]["co"]["50m"]["metadata"]

#     metadata_checker_obssurface(metadata=metadata, species="co")

#     metadata = raw_results["hfd"]["co"]["100m"]["metadata"]

#     metadata_checker_obssurface(metadata=metadata, species="co")

#     assert len(raw_results["hfd"]["co"]["50m"]["keys"]) == 3
#     assert len(raw_results["hfd"]["co"]["100m"]["keys"]) == 6

#     results = search.search(species=["NF3"], site="CGO", skip_ranking=True)

#     raw_results = results.raw()

#     metadata = raw_results["cgo"]["nf3"]["70m"]["metadata"]

#     metadata_checker_obssurface(metadata=metadata, species="nf3")

#     assert len(raw_results["cgo"]["nf3"]["70m"]["keys"]) == 1

<<<<<<< HEAD
# @pytest.mark.skip("Marked for removal")
# def test_search_and_retrieve(read_data, monkeypatch):
#     # def fixed_init(self):
#     #     from Acquire.Client import Wallet
=======

@pytest.mark.skip("Marked for removal")
def test_search_and_retrieve(read_data, monkeypatch):
    # def fixed_init(self):
    #     from Acquire.Client import Wallet
>>>>>>> devel

#     #     self._service_url = "openghg"
#     #     wallet = Wallet()
#     #     self._service = wallet.get_service(service_url=f"{self._service_url}/openghg")

#     # monkeypatch.setattr(Retrieve, "__init__", fixed_init)

#     search_results = search(species="nf3", site="cgo", inlet="70m")

#     assert search_results.cloud is True

#     data = search_results.retrieve(site="cgo", species="nf3", inlet="70m")

#     metadata = data.metadata

#     metadata_checker_obssurface(metadata=metadata, species="nf3")
