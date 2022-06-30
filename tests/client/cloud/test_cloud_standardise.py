from openghg.client import standardise_surface
from pathlib import Path
import gzip


def test_standardise(set_envs, mocker, tmpdir, capfd):
    call_fn_mock = mocker.patch("openghg.cloud.call_function")
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
        "network": "decc",
    }

    expected = {
        "function": "standardise",
        "data": packed,
        "metadata": {
            "site": "bsd",
            "instrument": "picarro",
            "sampling_period": "1m",
            "inlet": "248m",
            "data_type": "CRDS",
            "network": "decc",
        },
        "file_metadata": {
            "compressed": True,
            "sha1_hash": "56ba5dd8ea2fd49024b91792e173c70e08a4ddd1",
            "filename": "test_file.txt",
            "obs_type": "surface",
        },
    }

    standardise_surface(filepaths=tmppath, metadata={"some": "data"})

    out, _ = capfd.readouterr()

    assert "Error: we require the following metadata at a minimum:" in out

    assert call_fn_mock.call_count == 0

    standardise_surface(filepaths=tmppath, metadata=metadata)

    call_fn_mock.assert_called_with(data=expected)
