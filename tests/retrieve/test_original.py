from openghg.retrieve import search_footprints, retrieve_original_files, check_file_processed
from openghg.util import hash_file
from helpers import get_footprint_datapath


def test_retrieve_original_footprint_file(tmp_path):
    tac_fp_results = search_footprints(
        site="tac", height="100m", met_model="UKV", store="user", high_time_resolution=False
    )

    uid = next(iter(tac_fp_results.metadata))
    metadata = tac_fp_results.metadata[uid]

    original_file_hashes = metadata["original_file_hashes"]["v1"]

    assert "3920587db1d5e5c1455842d54238eaaa8a47b3df" in original_file_hashes
    assert "944374a2bf570f54c9066ed4a7bb7e4108a31280" in original_file_hashes

    retrieve_original_files(
        store="user", data_type="footprints", hash_data=original_file_hashes, output_folderpath=tmp_path
    )

    assert len(list(tmp_path.iterdir())) == 2

    fp1 = tmp_path / "TAC-100magl_UKV_TEST_201607.nc"
    fp2 = tmp_path / "TAC-100magl_UKV_TEST_201608.nc"
    assert hash_file(fp1) == "3920587db1d5e5c1455842d54238eaaa8a47b3df"
    assert hash_file(fp2) == "944374a2bf570f54c9066ed4a7bb7e4108a31280"


def test_check_if_file_already_processed(tmp_path):
    fp_datapath1 = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")

    assert check_file_processed(store="user", data_type="footprints", filepath=fp_datapath1)

    fp_datapath2 = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")

    assert check_file_processed(store="user", data_type="footprints", filepath=fp_datapath2)

    test_str = "this is a test string"

    test_filepath = tmp_path.joinpath("testing_123.txt")
    test_filepath.write_text(test_str)

    assert not check_file_processed(store="user", data_type="footprints", filepath=test_filepath)
