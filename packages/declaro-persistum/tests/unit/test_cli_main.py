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








