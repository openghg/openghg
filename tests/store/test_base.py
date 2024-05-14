from openghg.store.base import BaseStore
from openghg.objectstore import get_writable_bucket
from helpers import get_footprint_datapath


def test_files_checked_and_hashed():
    file1 = get_footprint_datapath("TAC-100magl_UKV_TEST_201607.nc")
    file2 = get_footprint_datapath("TAC-100magl_UKV_TEST_201608.nc")

    bucket = get_writable_bucket(name="user")

    b = BaseStore(bucket=bucket)

    filepaths = [file1, file2]

    seen, unseen = b.check_hashes(filepaths=filepaths, force=False)

    assert "3920587db1d5e5c1455842d54238eaaa8a47b3df" in unseen
    assert "944374a2bf570f54c9066ed4a7bb7e4108a31280" in unseen

    b._file_hashes.update({"3920587db1d5e5c1455842d54238eaaa8a47b3df": file1})

    seen, unseen = b.check_hashes(filepaths=filepaths, force=False)

    assert "3920587db1d5e5c1455842d54238eaaa8a47b3df" in seen
    assert "944374a2bf570f54c9066ed4a7bb7e4108a31280" in unseen

    b._file_hashes.update(unseen)

    seen, unseen = b.check_hashes(filepaths=filepaths, force=False)

    assert "3920587db1d5e5c1455842d54238eaaa8a47b3df" in seen
    assert "944374a2bf570f54c9066ed4a7bb7e4108a31280" in seen

    seen, unseen = b.check_hashes(filepaths=filepaths, force=True)

    assert "3920587db1d5e5c1455842d54238eaaa8a47b3df" in seen
    assert "944374a2bf570f54c9066ed4a7bb7e4108a31280" in seen
