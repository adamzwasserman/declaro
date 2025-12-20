"""
Observability module for query monitoring.

Provides timing instrumentation and slow query recording.
"""

from .slow_queries import (
    SlowQueryConfig,
    SlowQueryObserver,
    cleanup_slow_queries_sql,
    create_slow_query_observer,
    get_slow_queries_by_fingerprint_sql,
    get_slow_queries_sql,
    get_slow_query_stats_sql,
    record_slow_query_sql,
    setup_slow_query_table_sql,
)
from .timing import (
    CompositeObserver,
    QueryObserver,
    Timer,
    execute_with_timing,
    fingerprint_query,
    measure_time,
)

__all__ = [
    # Timing
    "QueryObserver",
    "Timer",
    "measure_time",
    "fingerprint_query",
    "execute_with_timing",
    "CompositeObserver",
    # Slow queries
    "SlowQueryConfig",
    "SlowQueryObserver",
    "setup_slow_query_table_sql",
    "record_slow_query_sql",
    "get_slow_queries_sql",
    "get_slow_queries_by_fingerprint_sql",
    "cleanup_slow_queries_sql",
    "get_slow_query_stats_sql",
    "create_slow_query_observer",
]
