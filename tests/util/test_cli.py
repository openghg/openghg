import json

import pytest

import openghg.util._cli as cli_module


def test_parse_search_terms_repeats_values():
    """Repeated KEY=VALUE terms should become OpenGHG OR-list searches."""
    assert cli_module._parse_search_terms(["species=co2", "species=ch4", "site=hfd"]) == {
        "species": ["co2", "ch4"],
        "site": "hfd",
    }


def test_parse_search_terms_rejects_invalid_terms():
    """Search terms should use explicit KEY=VALUE syntax."""
    with pytest.raises(ValueError, match="KEY=VALUE"):
        cli_module._parse_search_terms(["species"])


def test_cli_dispatches_search_subcommand(monkeypatch):
    """The top-level CLI should dispatch the search subcommand."""
    calls = []

    def fake_run_search_command(args):
        calls.append(args)

    monkeypatch.setattr(cli_module, "_run_search_command", fake_run_search_command)

    cli_module.cli(["search", "species=co2"])

    assert len(calls) == 1
    assert calls[0].terms == ["species=co2"]


def test_run_search_command_outputs_limited_json(capsys):
    """Search CLI JSON output should include selected fields and total count."""
    calls = []
    parser = cli_module._build_parser()
    args = parser.parse_args(
        [
            "search",
            "species=co2",
            "site=hfd",
            "--data-type",
            "surface",
            "--store",
            "user",
            "--fields",
            "uuid,species,site",
            "--limit",
            "1",
            "--json",
        ]
    )

    class FakeSearchResults:
        metadata = {
            "uuid-1": {"species": "co2", "site": "hfd", "data_type": "surface"},
            "uuid-2": {"species": "ch4", "site": "bsd", "data_type": "surface"},
        }

    def fake_search(**kwargs):
        calls.append(kwargs)
        return FakeSearchResults()

    cli_module._run_search_command(args, search_func=fake_search)

    payload = json.loads(capsys.readouterr().out)
    assert calls == [
        {
            "species": "co2",
            "site": "hfd",
            "store": "user",
            "data_type": "surface",
            "add_new_store": False,
        }
    ]
    assert payload == {
        "count": 1,
        "total": 2,
        "results": [{"uuid": "uuid-1", "species": "co2", "site": "hfd"}],
    }
