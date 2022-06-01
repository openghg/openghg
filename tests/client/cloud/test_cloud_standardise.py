from openghg.client import standardise_surface
from pathlib import Path
import gzip


def test_standardise(set_envs, mocker, tmpdir):
    mocked = mocker.patch("openghg.cloud.call_function")
    test_string = "some_text"
    tmppath = Path(tmpdir).joinpath("test_file.txt")
    tmppath.write_text(test_string)

    packed = gzip.compress((tmppath.read_bytes()))

    metadata = {
        "site": "bsd",
        "instrument": "picarro",
        "sampling_period": "1m",
        "inlet": "248m",
        "data_type": "CRDS",
    }

    expected = {
        "data": packed,
        "metadata": metadata,
        "file_metadata": {
            "compressed": True,
            "sha1_hash": "56ba5dd8ea2fd49024b91792e173c70e08a4ddd1",
            "filename": "test_file.txt",
        },
    }

    standardise_surface(filepaths=tmppath, metadata=metadata)

    mocked.assert_called_with(fn_name="standardise", data=expected)
