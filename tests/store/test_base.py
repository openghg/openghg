import pytest
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


#%% Test update_metadata method on base store
# For this have to define a specific data type but tests should be relevant for all
# subclasses.


@pytest.fixture
def surface_data():
    """
    Define input data relevant to an ObsSurface object.
    Note to pass checks this must contain all the required keys for the relevant data type.
     - See openghg/data/config/objectstore/defaults.json
    """

    # Define data to include most necessary required keys for data_type="surface"
    # Deliberately missing data_sublevel, data_source
    data = {"ch4":
                {"metadata":
                    {"site": "tac",
                     "inlet": "10m",
                     "species": "ch4",
                     "sampling_period": "300",
                     "instrument": "medusa",
                     "dataset_source": "not_set",
                     "network": "decc",
                     "source_format": "agage",
                     "data_level": "1",
                     }
                }
           }

    return data


@pytest.mark.parametrize(
        "input_parameters,additional_metadata,add_expected",
        [
            (
                {"data_sublevel": "1.1"},
                {"data_source": "internal"},
                {"data_sublevel": "1.1", "data_source": "internal"}
             ),
             (
                {"site": "TaC", "data_sublevel": "1.1"},
                {"data_source": "internal"},
                {"data_sublevel": "1.1", "data_source": "internal"}
             ),
        ]
)
def test_base_store_update_metadata(surface_data, input_parameters, additional_metadata, add_expected):
    """
    Test update_metadata method on BaseStore to ensure this follows expected logic
    when combining metadata from different sources.
    1. Check missing metadata can be added through input_parameters and additional_metadata
    2. Check overlapping key can be safely ignored when this is the same value (case-insensitive)
     - also includes implicit check that value from the original metadata is used rather than the
       new value in input_parameters as we expect the case to match metadata NOT input_parameters.
    """

    bucket = get_writable_bucket(name="user")

    base_store = BaseStore(bucket=bucket)

    base_store._data_type = "surface"

    expected_metadata = surface_data["ch4"]["metadata"].copy()
    expected_metadata.update(add_expected)

    updated_data = base_store.update_metadata(surface_data, input_parameters, additional_metadata)

    assert updated_data["ch4"]["metadata"] == expected_metadata
