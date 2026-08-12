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








