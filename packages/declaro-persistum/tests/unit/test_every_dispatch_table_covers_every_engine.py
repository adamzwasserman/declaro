"""Every table keyed by engine must hold all three, and be found automatically.

FOUR TIMES IN ONE SESSION A FIX LANDED ON ONE COPY OF A PAIR:

    generate_sql          lived in `applier/postgresql` alone, so
                          `declaro generate` raised on SQLite and Turso
    get_transaction_mode  missing from `applier/postgresql`, and `usage.md`
                          documents importing it from there
    create_table_sql      taught to write table-level constraints in
                          `applier/shared`, while `applier/postgresql` keeps
                          its own copy and kept dropping them
    the reconstructions   `table_reconstruction` taught to carry constraints
                          through a rebuild, `reconstruction` not, so SQLite
                          kept them and Turso did not

Each was found by running the thing on a second engine, and each was fixed by
hand. This is the shape rather than the instances: a package with one module
per engine drifts unless something counts.

WHAT THIS COUNTS. Every module-level dict in the package whose keys look like
engine names, found by reading the modules rather than listed here, because a
list is the thing that goes stale. Each must hold all three.

WHAT IT CANNOT SEE, said plainly. It checks that an entry EXISTS, not that the
three entries agree about anything. `create_table_sql` was present in both
renderers the whole time it was wrong in one of them. Divergence between two
present implementations is what the round-trip and convergence gates are for,
and there is no general test for it.
"""

from __future__ import annotations

import importlib
import pkgutil

import pytest

import declaro_persistum

pytestmark = [pytest.mark.precommit]

ENGINES = {"postgresql", "sqlite", "turso"}


def _dispatch_tables() -> dict[str, dict]:
    """Every module-level dict keyed by engine names, across the package."""
    found: dict[str, dict] = {}
    modules = [declaro_persistum]
    for info in pkgutil.walk_packages(declaro_persistum.__path__, prefix="declaro_persistum."):
        try:
            modules.append(importlib.import_module(info.name))
        except Exception:  # noqa: BLE001 - a module that will not import is
            continue  # not a dispatch table, and other gates cover it

    for module in modules:
        for name in dir(module):
            if name.startswith("_"):
                continue
            value = getattr(module, name)
            if not isinstance(value, dict) or not value:
                continue
            keys = set(value)
            # Keyed by engine: every key is one, and at least two are there.
            # One entry is a lookup that happens to use an engine name.
            if keys <= ENGINES and len(keys) >= 2:
                found[f"{module.__name__}.{name}"] = value
    return found


TABLES = _dispatch_tables()


def test_the_scan_finds_the_tables_this_package_is_known_to_have() -> None:
    """The floor, and it asserts a NUMBER rather than "some".

    A scan that silently found nothing would make every test below vacuous,
    which is how a gate comes to report zero offenders and zero coverage at
    the same time.
    """
    assert len(TABLES) >= 6, sorted(TABLES)

    names = " ".join(TABLES)
    for expected in ("APPLIERS", "INSPECTORS", "BULK_LOADERS", "WRITERS"):
        assert expected in names, f"{expected} was not found by the scan"


@pytest.mark.parametrize("name", sorted(TABLES))
def test_it_covers_every_engine(name: str) -> None:
    """An engine missing from a table is a feature missing from an engine."""
    missing = ENGINES - set(TABLES[name])

    assert not missing, (
        f"{name} has no entry for {sorted(missing)}. Every table keyed by "
        f"engine answers a question the package asks of all three engines."
    )


@pytest.mark.parametrize("name", sorted(TABLES))
def test_every_entry_is_callable_or_data(name: str) -> None:
    """The floor under the test above.

    A table holding `None` for an engine passes a key check and fails at the
    first call, which is the failure the key check exists to move earlier.
    """
    for engine, entry in TABLES[name].items():
        assert entry is not None, f"{name}[{engine!r}] is None"
