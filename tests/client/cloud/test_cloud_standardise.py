from openghg.client import standardise_surface
from pathlib import Path
from openghg.util import compress


def test_standardise(set_envs, mocker, tmpdir):
    call_fn_mock = mocker.patch("openghg.cloud.call_function", autospec=True)
    test_string = "some_text"
    tmppath = Path(tmpdir).joinpath("test_file.txt")
    tmppath.write_text(test_string)

    packed = compress((tmppath.read_bytes()))

    standardise_surface(
        filepaths=tmppath,
        site="bsd",
        inlet="248m",
        network="decc",
        data_type="crds",
        sampling_period="1m",
        instrument="picarro",
    )

    assert call_fn_mock.call_args == mocker.call(
        data={
            "function": "standardise",
            "data": packed,
            "metadata": {
                "site": "bsd",
                "data_type": "crds",
                "network": "decc",
                "inlet": "248m",
                "instrument": "picarro",
                "sampling_period": "1m",
            },
            "file_metadata": {
                "compressed": True,
                "sha1_hash": "56ba5dd8ea2fd49024b91792e173c70e08a4ddd1",
                "filename": "test_file.txt",
                "obs_type": "surface",
            },
        }
    )
