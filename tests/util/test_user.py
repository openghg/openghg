from openghg.util import create_config, get_user_config_path
import toml


def test_create_config(mocker):
    mock_uuids = [f"test-uuid-{n}" for n in range(100, 105)]
    mocker.patch("uuid.uuid4", side_effect=mock_uuids)

    path = get_user_config_path()
    create_config(silent=True)

    assert path.exists()

    config = toml.loads(path.read_text())

    print(config)
    assert False
