from openghg.util import create_config, get_user_config_path, get_user_id
import toml
from pathlib import Path
from openghg.objectstore import get_bucket

def test_get_user_config():
    assert get_user_id() == "test-id-123"

def test_create_config(mocker):
    return
    # mock_uuids = [f"test-uuid-{n}" for n in range(100, 105)]
    # mocker.patch("uuid.uuid4", side_effect=mock_uuids)
    # mock_objstore_path = Path("/tmp/store")
    # mock_config_path =
    # mocker.patch(, return_value=mock_config_path)
    # mocker.patch("openghg.util.get_default_objectstore_path", return_value=mock_objstore_path)

    # path = get_user_config_path()

    print(get_user_config_path())
    print(get_user_config_path().read_text())

    return
    print(path)

    create_config(silent=True)

    # assert path.exists()
    user_obj_expected = Path.home().joinpath("openghg_store").absolute()
    assert user_obj_expected.exists()

    config = toml.loads(path.read_text())

    print(config["user_id"])
    assert config["object_store"]["local_store"] == str(user_obj_expected)
