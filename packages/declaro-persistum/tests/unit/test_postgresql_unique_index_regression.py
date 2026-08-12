"""Regression tests for the Postgres unique-constraint / drop_index defects.

Reported against 0.1.8 by a downstream app whose startup died in
apply_migrations_async. Two distinct defects, both reachable from any
Postgres model declaring ``unique: True`` on a column:

1. PostgreSQLApplier._drop_index_sql took (self, details) while the
   dispatch in generate_operation_sql calls every generator as
   generator(table, details) -> TypeError on any drop_index op.

2. The Postgres inspector reported constraint-backed indexes (the index
   PostgreSQL builds to implement a UNIQUE constraint) as ordinary
   indexes. The model side declares that as unique: True on the column and
   never as an index, so the differ saw it in current - target and
   scheduled a drop_index against a constraint the model still declares.

A third case here covers the boolean-default churn: the loader emits
"FALSE" for a Python bool default while PostgreSQL introspects the same
default back as "false", so every migration re-emitted a no-op
alter_column.
"""

import pytest

from declaro_persistum.applier.postgresql import PostgreSQLApplier
from declaro_persistum.differ.core import diff
from declaro_persistum.types import Schema








