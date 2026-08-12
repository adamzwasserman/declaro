"""CLI entry point: dialect detection, connection resolution, and dispatch.

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
detect_dialect = cli.detect_dialect
get_connection_string = cli.get_connection_string
main = cli.main


class TestDetectDialect:
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
    def test_each_scheme_maps_to_its_dialect(self, url, expected):
        assert detect_dialect(url) == expected

    def test_an_unknown_scheme_raises_and_says_how_to_proceed(self):
        with pytest.raises(ValueError, match="--dialect"):
            detect_dialect("mysql://host/db")

    def test_an_empty_string_raises(self):
        with pytest.raises(ValueError):
            detect_dialect("")

    def test_matching_is_on_the_prefix_not_a_substring(self):
        """A path that merely contains 'sqlite://' is not a sqlite URL."""
        with pytest.raises(ValueError):
            detect_dialect("weird://wrapped/sqlite://x")


class TestGetConnectionString:
    def test_an_explicit_connection_wins(self, monkeypatch):
        monkeypatch.setenv("DECLARO_DATABASE_URL", "sqlite:///env.db")
        args = argparse.Namespace(connection="sqlite:///flag.db")
        assert get_connection_string(args) == "sqlite:///flag.db"

    def test_the_environment_is_the_fallback(self, monkeypatch):
        monkeypatch.setenv("DECLARO_DATABASE_URL", "sqlite:///env.db")
        assert get_connection_string(argparse.Namespace(connection=None)) == (
            "sqlite:///env.db"
        )

    def test_neither_gives_none(self, monkeypatch):
        monkeypatch.delenv("DECLARO_DATABASE_URL", raising=False)
        assert get_connection_string(argparse.Namespace(connection=None)) is None


class TestCreateParser:
    def test_the_parser_is_built(self):
        assert isinstance(create_parser(), argparse.ArgumentParser)

    @pytest.mark.parametrize(
        "command", ["diff", "apply", "snapshot", "validate", "generate"]
    )
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

    def test_a_command_needing_a_database_without_one_exits_one(
        self, monkeypatch, capsys
    ):
        monkeypatch.delenv("DECLARO_DATABASE_URL", raising=False)
        assert main(["diff"]) == 1
        assert "Database connection required" in capsys.readouterr().err

    def test_validate_needs_no_database(self, monkeypatch):
        monkeypatch.delenv("DECLARO_DATABASE_URL", raising=False)
        monkeypatch.setattr(cli, "cmd_validate", lambda **_k: 0)
        assert main(["validate"]) == 0

    def test_an_undetectable_dialect_exits_one(self, capsys):
        assert main(["-c", "mysql://host/db", "diff"]) == 1
        assert "Cannot detect dialect" in capsys.readouterr().err

    def test_an_explicit_dialect_skips_detection(self, monkeypatch):
        """--dialect must let an otherwise unrecognised URL through."""
        seen = {}

        async def _diff(**kw):
            seen.update(kw)
            return 0

        monkeypatch.setattr(cli, "cmd_diff", _diff)
        assert main(["-c", "mysql://host/db", "--dialect", "postgresql", "diff"]) == 0
        assert seen["dialect"] == "postgresql"

    def test_a_detected_dialect_is_passed_through(self, monkeypatch):
        seen = {}

        async def _diff(**kw):
            seen.update(kw)
            return 0

        monkeypatch.setattr(cli, "cmd_diff", _diff)
        main(["-c", "sqlite:///x.db", "diff"])
        assert seen["dialect"] == "sqlite"
        assert seen["connection_string"] == "sqlite:///x.db"

    def test_the_command_exit_code_is_returned_unchanged(self, monkeypatch):
        async def _diff(**_kw):
            return 2

        monkeypatch.setattr(cli, "cmd_diff", _diff)
        assert main(["-c", "sqlite:///x.db", "diff"]) == 2

    def test_the_connection_can_come_from_the_environment(self, monkeypatch):
        monkeypatch.setenv("DECLARO_DATABASE_URL", "sqlite:///env.db")
        seen = {}

        async def _diff(**kw):
            seen.update(kw)
            return 0

        monkeypatch.setattr(cli, "cmd_diff", _diff)
        assert main(["diff"]) == 0
        assert seen["connection_string"] == "sqlite:///env.db"

    @pytest.mark.parametrize(
        "command,attr",
        [
            ("diff", "cmd_diff"),
            ("apply", "cmd_apply"),
            ("snapshot", "cmd_snapshot"),
            ("generate", "cmd_generate"),
        ],
    )
    def test_each_connected_command_reaches_its_own_implementation(
        self, monkeypatch, command, attr
    ):
        called = []

        async def _cmd(**_kw):
            called.append(attr)
            return 0

        monkeypatch.setattr(cli, attr, _cmd)
        assert main(["-c", "sqlite:///x.db", command]) == 0
        assert called == [attr]

    def test_an_interrupt_exits_one_hundred_and_thirty(self, monkeypatch, capsys):
        async def _diff(**_kw):
            raise KeyboardInterrupt

        monkeypatch.setattr(cli, "cmd_diff", _diff)
        assert main(["-c", "sqlite:///x.db", "diff"]) == 130
        assert "Aborted" in capsys.readouterr().err

    def test_an_unexpected_error_exits_one_and_reports_it(self, monkeypatch, capsys):
        async def _diff(**_kw):
            raise RuntimeError("the database melted")

        monkeypatch.setattr(cli, "cmd_diff", _diff)
        assert main(["-c", "sqlite:///x.db", "diff"]) == 1
        assert "the database melted" in capsys.readouterr().err

    def test_verbose_adds_a_traceback_to_the_error(self, monkeypatch, capsys):
        async def _diff(**_kw):
            raise RuntimeError("boom")

        monkeypatch.setattr(cli, "cmd_diff", _diff)
        assert main(["-c", "sqlite:///x.db", "-v", "diff"]) == 1
        assert "Traceback" in capsys.readouterr().err

    def test_a_quiet_error_carries_no_traceback(self, monkeypatch, capsys):
        async def _diff(**_kw):
            raise RuntimeError("boom")

        monkeypatch.setattr(cli, "cmd_diff", _diff)
        main(["-c", "sqlite:///x.db", "diff"])
        assert "Traceback" not in capsys.readouterr().err
