"""Two services sharing one database must not fight over the skip-if-clean stamp.

The stamp lives in _declaro_meta in the database being migrated, under a key
built from the schema file's *name* alone. Two services that both migrate the
same database therefore write the same row.

They disagree whenever their computed hash differs, and the hash covers both
the schema file's contents and the library version. So:

  - two services with their own models.py, different content, same filename
  - or two services on different declaro-persistum versions

each read the other's stamp, see a mismatch, re-introspect, and re-stamp.
Neither is wrong and neither can win. The result is a permanent
never-clean state with operations re-proposed on every boot, which is also
the symptom of a genuine schema drift — so it is easy to misdiagnose.

Reported by a consumer whose stage service pointed its central database at the
production central database.
"""

import pytest

from declaro_persistum.migrations import _get_stored_hash, _store_hash






