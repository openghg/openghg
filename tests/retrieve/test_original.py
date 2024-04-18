from openghg.retrieve import search_footprints, retrieve_original_files
from openghg.util import hash_file


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
