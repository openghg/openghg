from pathlib import Path

import pytest
from openghg.objectstore import get_bucket, query_store
from openghg.store import ObsSurface


def get_surface_datapath(filename, data_type):
    return (
        Path(__file__).resolve(strict=True).parent.joinpath(f"../data/proc_test_data/{data_type}/{filename}")
    )


def hfd_filepath():
    return get_surface_datapath(filename="hfd.picarro.1minute.100m.min.dat", source_format="CRDS")


# Add some stuff to the object store?
@pytest.fixture(scope="session")
def populate_store():
    get_bucket(empty=True)
    filepath = hfd_filepath()
    ObsSurface.read_file(filepath=filepath, source_format="CRDS", site="hfd")


# @pytest.mark.skip(reason="Unfinished")
# def test_query_store(populate_store):
#     data = query_store()

# print(data)