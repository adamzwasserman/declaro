"""CLI command logic: exit codes, drift detection, and the operation printer.

`cli/commands.py` carried 170 branches at 0% coverage — the single largest
hole behind the Slop Audit L1.19 reading of 49.7% (declaro-xu0), and the
one module big enough to move the band on its own.

Three kinds of thing are covered here, and the split matters:

- `_check_drift` is pure. Schemas in, list of differences out.
- `_print_diff_result` and `_print_drift_error` are dispatch chains that
  write to stdout. `_print_diff_result` has twelve arms, one per operation
  type, and L1.19 counts every one. capsys reads them back.
- `cmd_validate` is a command: it returns an EXIT CODE, and the codes are
  a contract with whoever runs the CLI. 0 valid, 1 errors, 2 warnings under
  --strict. Each is asserted directly.

The schema loader is patched at its import site rather than mocked as an
object, so what is under test is the command's own branching.
"""

import pytest

from declaro_persistum.cli.commands import (
    _check_drift,
    _print_diff_result,
    _print_drift_error,
    cmd_validate,
)


def _schema(**tables):
    return {name: {"columns": cols} for name, cols in tables.items()}


def _op(op, table, **details):
    return {"op": op, "table": table, "details": details}


def _result(*ops):
    return {"operations": list(ops), "execution_order": list(range(len(ops)))}


class TestCheckDrift:
    def test_identical_schemas_have_no_drift(self):
        s = _schema(users={"id": {}})
        assert _check_drift(s, s) == []

    def test_two_empty_schemas_have_no_drift(self):
        assert _check_drift({}, {}) == []

    def test_a_table_only_in_the_database_is_reported_as_added(self):
        d = _check_drift(_schema(users={}), {})
        assert len(d) == 1
        assert d[0]["symbol"] == "+"
        assert "exists in DB but not in snapshot" in d[0]["description"]

    def test_a_table_only_in_the_snapshot_is_reported_as_removed(self):
        d = _check_drift({}, _schema(users={}))
        assert d[0]["symbol"] == "-"
        assert "in snapshot but not in DB" in d[0]["description"]

    def test_a_column_only_in_the_database_is_reported(self):
        d = _check_drift(_schema(users={"id": {}, "extra": {}}), _schema(users={"id": {}}))
        assert len(d) == 1
        assert d[0]["symbol"] == "+"
        assert "users.extra" in d[0]["description"]

    def test_a_column_only_in_the_snapshot_is_reported(self):
        d = _check_drift(_schema(users={"id": {}}), _schema(users={"id": {}, "gone": {}}))
        assert d[0]["symbol"] == "-"
        assert "users.gone" in d[0]["description"]

    def test_columns_are_only_compared_for_tables_present_in_both(self):
        """A table missing entirely is one finding, not one per column."""
        d = _check_drift({}, _schema(users={"a": {}, "b": {}, "c": {}}))
        assert len(d) == 1

    def test_a_table_with_no_columns_key_is_tolerated(self):
        assert _check_drift({"users": {}}, {"users": {}}) == []

    def test_table_and_column_drift_are_reported_together(self):
        d = _check_drift(
            _schema(users={"id": {}, "extra": {}}, only_db={}),
            _schema(users={"id": {}}, only_snap={}),
        )
        symbols = sorted(x["symbol"] for x in d)
        assert symbols == ["+", "+", "-"]


class TestPrintDriftError:
    def test_every_difference_is_printed(self, capsys):
        _print_drift_error([
            {"symbol": "+", "description": "one"},
            {"symbol": "-", "description": "two"},
        ])
        out = capsys.readouterr().out
        assert "+ one" in out and "- two" in out

    def test_the_three_remedies_are_offered(self, capsys):
        _print_drift_error([{"symbol": "+", "description": "x"}])
        out = capsys.readouterr().out
        assert "declaro snapshot" in out
        assert "--force" in out
        assert "Manually reconcile" in out


class TestPrintDiffResult:
    def test_an_empty_operation_list_says_so_and_stops(self, capsys):
        _print_diff_result(_result(), verbose=False)
        assert capsys.readouterr().out.strip() == "No changes needed."

    def test_the_header_counts_the_operations(self, capsys):
        _print_diff_result(_result(_op("drop_table", "t")), verbose=False)
        assert "Proposed changes (1 operations)" in capsys.readouterr().out

    def test_operations_are_numbered_from_one(self, capsys):
        _print_diff_result(
            _result(_op("drop_table", "a"), _op("drop_table", "b")), verbose=False
        )
        out = capsys.readouterr().out
        assert "1. DROP TABLE a" in out and "2. DROP TABLE b" in out

    def test_execution_order_drives_the_printing_not_list_order(self, capsys):
        result = {
            "operations": [_op("drop_table", "second"), _op("drop_table", "first")],
            "execution_order": [1, 0],
        }
        _print_diff_result(result, verbose=False)
        out = capsys.readouterr().out
        assert out.index("first") < out.index("second")

    @pytest.mark.parametrize(
        "op,expected",
        [
            (_op("create_table", "t", columns={"a": {}, "b": {}}),
             "CREATE TABLE t (2 columns)"),
            (_op("drop_table", "t"), "DROP TABLE t"),
            (_op("rename_table", "t", new_name="u"), "RENAME TABLE t -> u"),
            (_op("add_column", "t", column="c", definition={"type": "text"}),
             "ADD COLUMN t.c (text)"),
            (_op("drop_column", "t", column="c"), "DROP COLUMN t.c"),
            (_op("rename_column", "t", from_column="a", to_column="b"),
             "RENAME COLUMN t.a -> b"),
            (_op("alter_column", "t", column="c", changes={"type": 1, "nullable": 2}),
             "ALTER COLUMN t.c (type, nullable)"),
            (_op("add_index", "t", index="ix", definition={"columns": ["a", "b"]}),
             "CREATE INDEX ix ON t (a, b)"),
            (_op("drop_index", "t", index="ix"), "DROP INDEX ix"),
            (_op("add_foreign_key", "t", column="c", references="u.id"),
             "ADD FOREIGN KEY t.c -> u.id"),
            (_op("drop_foreign_key", "t", column="c"), "DROP FOREIGN KEY t.c"),
        ],
    )
    def test_each_operation_type_has_its_own_line(self, capsys, op, expected):
        """One case per arm of the dispatch chain — this is the decision
        space L1.19 measures, and it is the reason the module scored 0%."""
        _print_diff_result(_result(op), verbose=False)
        assert expected in capsys.readouterr().out

    def test_an_unrecognised_operation_falls_through_to_a_generic_line(self, capsys):
        _print_diff_result(_result(_op("teleport_table", "t")), verbose=False)
        assert "TELEPORT_TABLE on t" in capsys.readouterr().out

    def test_a_create_table_with_no_columns_key_reports_zero(self, capsys):
        _print_diff_result(_result(_op("create_table", "t")), verbose=False)
        assert "CREATE TABLE t (0 columns)" in capsys.readouterr().out

    def test_an_add_column_with_no_type_prints_a_question_mark(self, capsys):
        _print_diff_result(
            _result(_op("add_column", "t", column="c", definition={})), verbose=False
        )
        assert "ADD COLUMN t.c (?)" in capsys.readouterr().out

    def test_verbose_prints_the_remaining_detail_keys(self, capsys):
        _print_diff_result(
            _result(_op("drop_table", "t", reason="obsolete")), verbose=True
        )
        assert "reason: obsolete" in capsys.readouterr().out

    def test_verbose_suppresses_keys_already_shown_on_the_line(self, capsys):
        _print_diff_result(
            _result(_op("drop_column", "t", column="c")), verbose=True
        )
        out = capsys.readouterr().out
        assert "column: c" not in out, "already printed as t.c"

    def test_non_verbose_prints_no_detail_lines(self, capsys):
        _print_diff_result(
            _result(_op("drop_table", "t", reason="obsolete")), verbose=False
        )
        assert "reason" not in capsys.readouterr().out


class TestCmdValidate:
    """The exit code is the contract with whoever runs the CLI."""

    def _patch_loader(self, monkeypatch, schema=None, raises=None):
        import declaro_persistum.loader as loader

        def _load(_dir):
            if raises is not None:
                raise raises
            return schema

        monkeypatch.setattr(loader, "load_schema", _load)

    def test_a_valid_schema_exits_zero(self, monkeypatch, capsys):
        self._patch_loader(monkeypatch, _schema(users={"id": {"type": "integer"}}))
        assert cmd_validate(schema_dir="x", strict=False, verbose=False) == 0
        assert "Schema valid (1 tables)" in capsys.readouterr().out

    def test_a_schema_that_will_not_load_exits_one(self, monkeypatch, capsys):
        self._patch_loader(monkeypatch, raises=OSError("no such directory"))
        assert cmd_validate(schema_dir="x", strict=False, verbose=False) == 1
        assert "Failed to load schema: no such directory" in capsys.readouterr().out

    def test_a_validation_error_exits_one_and_lists_the_errors(
        self, monkeypatch, capsys
    ):
        bad = {"posts": {"columns": {"a": {"type": "integer",
                                           "references": "ghosts.id"}}}}
        self._patch_loader(monkeypatch, bad)
        assert cmd_validate(schema_dir="x", strict=False, verbose=False) == 1
        out = capsys.readouterr().out
        assert "1 error(s)" in out and "non-existent table" in out

    def test_warnings_alone_still_exit_zero(self, monkeypatch, capsys):
        self._patch_loader(monkeypatch, _schema(t={"c": {"type": "wat"}}))
        assert cmd_validate(schema_dir="x", strict=False, verbose=False) == 0
        assert "warning(s)" in capsys.readouterr().out

    def test_warnings_under_strict_exit_two(self, monkeypatch):
        self._patch_loader(monkeypatch, _schema(t={"c": {"type": "wat"}}))
        assert cmd_validate(schema_dir="x", strict=True, verbose=False) == 2

    def test_strict_does_not_change_the_code_when_there_are_no_warnings(
        self, monkeypatch
    ):
        self._patch_loader(monkeypatch, _schema(t={"c": {"type": "text"}}))
        assert cmd_validate(schema_dir="x", strict=True, verbose=False) == 0

    def test_errors_beat_strict_warnings(self, monkeypatch):
        """An error is exit 1 even under --strict, which would give 2."""
        bad = {"t": {"columns": {"c": {"type": "wat", "references": "ghosts.id"}}}}
        self._patch_loader(monkeypatch, bad)
        assert cmd_validate(schema_dir="x", strict=True, verbose=False) == 1

    def test_verbose_lists_each_table_and_its_column_count(
        self, monkeypatch, capsys
    ):
        self._patch_loader(
            monkeypatch,
            _schema(b={"x": {"type": "text"}}, a={"y": {"type": "text"},
                                                  "z": {"type": "text"}}),
        )
        cmd_validate(schema_dir="x", strict=False, verbose=True)
        out = capsys.readouterr().out
        assert "a: 2 columns" in out and "b: 1 columns" in out
        assert out.index("a: 2") < out.index("b: 1"), "tables are sorted"

    def test_an_empty_schema_is_valid(self, monkeypatch, capsys):
        self._patch_loader(monkeypatch, {})
        assert cmd_validate(schema_dir="x", strict=False, verbose=False) == 0
        assert "Schema valid (0 tables)" in capsys.readouterr().out
