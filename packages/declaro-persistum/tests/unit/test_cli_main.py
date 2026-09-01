"""CLI entry point: engine detection, connection resolution, and dispatch.

`cli/main.py` carried 30 branches at 0% coverage — part of the Slop Audit
L1.19 gap (declaro-xu0).

`main()` is a dispatch chain returning an exit code, so every test asserts
the code. The commands themselves are patched at their import site in
`cli.main`, which is what makes this a test of the ENTRY POINT's branching
rather than of the commands it calls — those have their own file.

The exit codes are the CLI's contract: 0 success, 1 usage or failure,
130 interrupted. They are asserted, never inferred from output.
"""

import argparse
import importlib

import pytest

# `declaro_persistum/cli/__init__.py` does `from .main import main`, which
# rebinds the package attribute `main` from the SUBMODULE to the FUNCTION.
# `import declaro_persistum.cli.main as cli` therefore hands back the
# function, and monkeypatching it silently fails. importlib goes to
# sys.modules and gets the module itself.
cli = importlib.import_module("declaro_persistum.cli.main")

create_parser = cli.create_parser
detect_engine = cli.detect_engine
get_connection_string = cli.get_connection_string
main = cli.main


class TestDetectEngine:
    @pytest.mark.parametrize(
        "url,expected",
        [
            ("postgresql://host/db", "postgresql"),
            ("postgres://host/db", "postgresql"),
            ("sqlite:///local.db", "sqlite"),
            ("libsql://x.turso.io", "turso"),
            ("https://x.turso.io", "turso"),
        ],
    )
    def test_each_scheme_maps_to_its_engine(self, url, expected):
        assert detect_engine(url) == expected

    def test_an_unknown_scheme_raises_and_says_how_to_proceed(self):
        with pytest.raises(ValueError, match="--engine"):
            detect_engine("mysql://host/db")

    def test_an_empty_string_raises(self):
        with pytest.raises(ValueError):
            detect_engine("")

    def test_matching_is_on_the_prefix_not_a_substring(self):
        """A path that merely contains 'sqlite://' is not a sqlite URL."""
        with pytest.raises(ValueError):
            detect_engine("weird://wrapped/sqlite://x")


class TestGetConnectionString:
    def test_an_explicit_connection_wins(self, monkeypatch):
        monkeypatch.setenv("DECLARO_DATABASE_URL", "sqlite:///env.db")
        args = argparse.Namespace(connection="sqlite:///flag.db")
        assert get_connection_string(args) == "sqlite:///flag.db"

    def test_the_environment_is_the_fallback(self, monkeypatch):
        monkeypatch.setenv("DECLARO_DATABASE_URL", "sqlite:///env.db")
        assert get_connection_string(argparse.Namespace(connection=None)) == ("sqlite:///env.db")

    def test_neither_gives_none(self, monkeypatch):
        monkeypatch.delenv("DECLARO_DATABASE_URL", raising=False)
        assert get_connection_string(argparse.Namespace(connection=None)) is None


class TestCreateParser:
    def test_the_parser_is_built(self):
        assert isinstance(create_parser(), argparse.ArgumentParser)

    @pytest.mark.parametrize("command", ["diff", "apply", "snapshot", "validate", "generate"])
    def test_each_documented_command_parses(self, command):
        assert create_parser().parse_args([command]).command == command

    def test_no_command_leaves_the_attribute_falsy(self):
        assert not create_parser().parse_args([]).command

    def test_an_unknown_command_exits_rather_than_returning(self):
        with pytest.raises(SystemExit):
            create_parser().parse_args(["teleport"])


class TestMainDispatch:
    def test_no_command_prints_help_and_exits_one(self, capsys):
        assert main([]) == 1
        assert "usage" in capsys.readouterr().out.lower()

    def test_a_command_needing_a_database_without_one_exits_one(self, monkeypatch, capsys):
        monkeypatch.delenv("DECLARO_DATABASE_URL", raising=False)
        assert main(["diff"]) == 1
        assert "Database connection required" in capsys.readouterr().err

    def test_an_undetectable_engine_exits_one(self, capsys):
        assert main(["-c", "mysql://host/db", "diff"]) == 1
        assert "Cannot detect engine" in capsys.readouterr().err
