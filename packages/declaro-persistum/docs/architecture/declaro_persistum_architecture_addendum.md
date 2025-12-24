# declaro_persistum Architecture Addendum

---
**STATUS**: DESIGN
**VERSION**: 0.1.0
**DATE**: 2025-12-14
**PARENT**: declaro_persistum_architecture.md
---

## Overview

This addendum extends the core architecture with:

1. **Additional Schema Objects**: Triggers, stored procedures, views, enums
2. **Portable Abstractions**: Relational patterns that emulate PostgreSQL features across all dialects
3. **Query Layer Extensions**: Native function mapping and dialect translation
4. **Observability**: Performance monitoring and automatic index optimization

The guiding principle: honest abstraction. No performance lies. Portable patterns that work everywhere without hidden degradation.

---

## A1. Additional Schema Objects

### A1.1 Triggers

Triggers defined in Pydantic models, generated per dialect.

#### Schema Definition

```python
# models/orders.py
from declaro_persistum import table, trigger

@table("orders")
class Order(BaseModel):
    id: UUID = field(primary=True)
    # ... columns ...

    class Meta:
        triggers = [
            {
                "name": "set_updated_at",
                "timing": "before",
                "event": "update",
                "for_each": "row",
                "body": "NEW.updated_at = now(); RETURN NEW;",
            },
            {
                "name": "audit_changes",
                "timing": "after",
                "event": ["insert", "update", "delete"],
                "for_each": "row",
                "execute": "audit_order_change",  # references stored procedure
            },
        ]
```

#### Type Definition

```python
class Trigger(TypedDict, total=False):
    """Trigger definition."""
    timing: Literal["before", "after", "instead_of"]
    event: str | list[str]  # insert, update, delete, or list
    for_each: Literal["row", "statement"]
    when: str               # optional condition
    body: str               # inline trigger body
    execute: str            # reference to stored procedure
```

#### Generated SQL

```sql
-- PostgreSQL
CREATE OR REPLACE FUNCTION orders_set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION orders_set_updated_at();

-- SQLite
CREATE TRIGGER orders_set_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
BEGIN
    UPDATE orders SET updated_at = datetime('now') WHERE rowid = NEW.rowid;
END;
```

### A1.2 Stored Procedures

Procedures defined in Pydantic model files.

#### Schema Definition

```python
# models/procedures.py
from declaro_persistum import procedure

@procedure("audit_order_change")
class AuditOrderChange:
    language = "plpgsql"
    returns = "trigger"
    body = """
        INSERT INTO audit_log (table_name, operation, record_id, changed_at)
        VALUES ('orders', TG_OP, COALESCE(NEW.id, OLD.id), now());
        RETURN NEW;
    """

@procedure("calculate_order_total")
class CalculateOrderTotal:
    language = "sql"
    returns = "numeric"
    parameters = [{"name": "order_id", "type": "uuid"}]
    body = """
        SELECT COALESCE(SUM(quantity * unit_price), 0)
        FROM order_items
        WHERE order_id = $1;
    """
```

#### Type Definition

```python
class Parameter(TypedDict):
    """Procedure parameter."""
    name: str
    type: str
    default: str | None

class Procedure(TypedDict, total=False):
    """Stored procedure definition."""
    language: Literal["sql", "plpgsql"]
    returns: str
    parameters: list[Parameter]
    body: str
```

#### SQLite Limitation

SQLite does not support stored procedures. For SQLite/Turso:
- Simple SQL procedures: inline at call site
- Complex procedures: raise `NotSupportedError` with clear message

```
NotSupportedError: Stored procedure 'audit_order_change' requires PostgreSQL.
  
  SQLite does not support stored procedures.
  
  Options:
    1. Move logic to application layer
    2. Use SQLite triggers with inline logic
    3. Use PostgreSQL for this project
```

### A1.3 Views

Views as named queries, optionally materialized (PostgreSQL only).

#### Schema Definition

```python
# models/views.py
from declaro_persistum import view

@view("order_summaries")
class OrderSummariesView:
    query = """
        SELECT
            o.id,
            o.user_id,
            o.status,
            COUNT(i.id) AS item_count,
            SUM(i.quantity * i.unit_price) AS total
        FROM orders o
        LEFT JOIN order_items i ON i.order_id = o.id
        GROUP BY o.id
    """

@view("active_users")
class ActiveUsersView:
    query = """
        SELECT u.*
        FROM users u
        WHERE EXISTS (
            SELECT 1 FROM orders o
            WHERE o.user_id = u.id
            AND o.created_at > now() - interval '30 days'
        )
    """
    materialized = True           # PostgreSQL only
    refresh = "on_demand"         # or "on_commit" (PostgreSQL only)
```

#### Type Definition

```python
class View(TypedDict, total=False):
    """View definition."""
    query: str
    materialized: bool        # PostgreSQL only
    refresh: Literal["on_demand", "on_commit"]
```

#### Generated SQL

```sql
-- PostgreSQL (materialized)
CREATE MATERIALIZED VIEW active_users AS
    SELECT u.*
    FROM users u
    WHERE EXISTS (...);

-- SQLite (materialized not supported; falls back to regular view)
CREATE VIEW active_users AS
    SELECT u.*
    FROM users u
    WHERE EXISTS (...);
```

Warning emitted for SQLite:
```
⚠️ View 'active_users' requested materialized=true, but SQLite does not support 
   materialized views. Created as regular view. Queries will compute on each access.
```

### A1.4 Enums

Enums as portable type using Python's `Literal` type. The library auto-generates lookup tables with FK constraints for cross-backend compatibility.

#### Schema Definition

```python
# models/orders.py
from typing import Literal
from uuid import UUID
from pydantic import BaseModel
from declaro_persistum import table, field

# Define enum values using Literal types
OrderStatus = Literal["pending", "confirmed", "shipped", "delivered", "cancelled"]
Priority = Literal["low", "medium", "high", "urgent"]

@table("orders")
class Order(BaseModel):
    id: UUID = field(primary=True)
    status: OrderStatus = field(default="'pending'")  # Uses Literal type
    priority: Priority = "medium"
```

#### Generated SQL

The library generates lookup tables with FK constraints for cross-backend compatibility:

```sql
-- All backends (lookup table + FK constraint)
CREATE TABLE _dp_enum_orders_status (
    value TEXT PRIMARY KEY
);
INSERT INTO _dp_enum_orders_status (value) VALUES
    ('pending'), ('confirmed'), ('shipped'), ('delivered'), ('cancelled');

CREATE TABLE orders (
    id UUID PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending' REFERENCES _dp_enum_orders_status(value),
    priority TEXT NOT NULL DEFAULT 'medium'
);
```

This approach works identically across PostgreSQL, SQLite, Turso, and LibSQL. The FK constraint ensures data integrity while the lookup table provides a clear source of valid values.

---

## A2. Portable Abstractions

These patterns provide PostgreSQL-like features using pure relational structures that work identically across all dialects.

### A2.1 Arrays → Junction Tables

PostgreSQL has native arrays. We emulate with junction tables for portability and indexability.

#### Schema Definition

```python
# models/users.py
from declaro_persistum import table, field

@table("users")
class User(BaseModel):
    id: UUID = field(primary=True)
    name: str
    roles: list[str] = field(db_type="array<text>")
```

#### Generated Schema

```sql
-- All dialects: junction table
CREATE TABLE users_roles (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    value TEXT NOT NULL,
    position INTEGER NOT NULL,    -- preserves array order
    PRIMARY KEY (user_id, position)
);
CREATE INDEX users_roles_value_idx ON users_roles(value);
CREATE INDEX users_roles_user_id_idx ON users_roles(user_id);
```

#### Query Abstraction

```python
# Insert with array
insert(conn, "users", {
    "name": "alice",
    "roles": ["admin", "editor"]
})
# Executes:
#   INSERT INTO users (id, name) VALUES (:id, :name);
#   INSERT INTO users_roles (user_id, value, position) VALUES
#       (:id, 'admin', 0), (:id, 'editor', 1);

# Select with array hydration
select("*", from_table="users", where="id = :id")
# Returns: {"id": "...", "name": "alice", "roles": ["admin", "editor"]}

# Array contains
select("*", from_table="users", where="roles CONTAINS :role", params={"role": "admin"})
# Executes:
#   SELECT u.* FROM users u
#   WHERE EXISTS (SELECT 1 FROM users_roles r WHERE r.user_id = u.id AND r.value = :role)

# Array append
update(conn, "users", array_append("roles", "viewer"), where="id = :id")
# Executes:
#   INSERT INTO users_roles (user_id, value, position)
#   SELECT :id, 'viewer', COALESCE(MAX(position) + 1, 0) FROM users_roles WHERE user_id = :id
```

#### Advantages Over Native Arrays

| Aspect | PostgreSQL Array | Junction Table |
|--------|------------------|----------------|
| Indexable values | GIN index required | B-tree index (standard) |
| Query by element | `ANY()` operator | Simple JOIN |
| Portable | PostgreSQL only | All dialects |
| Normalized | No | Yes |
| Can query "users with role X" | Full table scan or GIN | Index scan |

### A2.2 Maps (hstore) → Junction Tables

Key-value storage as junction table.

#### Schema Definition

```python
# models/products.py
from declaro_persistum import table, field

@table("products")
class Product(BaseModel):
    id: UUID = field(primary=True)
    name: str
    attributes: dict[str, str] = field(db_type="map<text, text>")
```

#### Generated Schema

```sql
CREATE TABLE products_attributes (
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (product_id, key)
);
CREATE INDEX products_attributes_key_idx ON products_attributes(key);
CREATE INDEX products_attributes_value_idx ON products_attributes(value);
```

#### Query Abstraction

```python
# Insert with map
insert(conn, "products", {
    "name": "Widget",
    "attributes": {"color": "red", "size": "large", "material": "steel"}
})

# Get by key
select("*", from_table="products", where="attributes['color'] = :color", params={"color": "red"})
# Executes:
#   SELECT p.* FROM products p
#   JOIN products_attributes a ON p.id = a.product_id
#   WHERE a.key = 'color' AND a.value = :color

# Has key
select("*", from_table="products", where="attributes HAS KEY :key", params={"key": "material"})
# Executes:
#   SELECT p.* FROM products p
#   WHERE EXISTS (SELECT 1 FROM products_attributes a WHERE a.product_id = p.id AND a.key = :key)
```

### A2.3 Ranges → Nullable Start/End Columns

Temporal and numeric ranges with open-ended support.

#### Schema Definition

```python
# models/reservations.py
from datetime import datetime
from decimal import Decimal
from declaro_persistum import table, field, RangeType

@table("reservations")
class Reservation(BaseModel):
    id: UUID = field(primary=True)
    during: RangeType[datetime] = field(
        db_type="range<timestamptz>",
        start_required=False,  # NULL = beginning of time
        end_required=False,    # NULL = end of time
    )

@table("subscriptions")
class Subscription(BaseModel):
    id: UUID = field(primary=True)
    price_range: RangeType[Decimal] = field(
        db_type="range<numeric>",
        start_required=True,   # must have minimum
        end_required=False,    # no maximum = unlimited
    )
```

#### Generated Schema

```sql
CREATE TABLE reservations (
    id UUID PRIMARY KEY,
    during_start TIMESTAMPTZ,
    during_end TIMESTAMPTZ,
    CHECK (during_start IS NULL OR during_end IS NULL OR during_start < during_end)
);
CREATE INDEX reservations_during_idx ON reservations(during_start, during_end);

CREATE TABLE subscriptions (
    id UUID PRIMARY KEY,
    price_range_start NUMERIC NOT NULL,
    price_range_end NUMERIC,
    CHECK (price_range_end IS NULL OR price_range_start < price_range_end)
);
```

#### Query Abstraction

```python
# Overlap query (do two ranges intersect?)
select("*", from_table="reservations", 
    where="during OVERLAPS :range",
    params={"range": {"start": "2025-01-01", "end": "2025-01-31"}})
# Executes:
#   SELECT * FROM reservations
#   WHERE (during_start IS NULL OR during_start < :range_end)
#     AND (during_end IS NULL OR during_end > :range_start)

# Contains point (is a timestamp within range?)
select("*", from_table="subscriptions",
    where="price_range CONTAINS :point",
    params={"point": 50.00})
# Executes:
#   SELECT * FROM subscriptions
#   WHERE (price_range_start IS NULL OR price_range_start <= :point)
#     AND (price_range_end IS NULL OR price_range_end > :point)

# Contains range (is one range fully within another?)
select("*", from_table="reservations",
    where="during CONTAINS :range",
    params={"range": {"start": "2025-01-10", "end": "2025-01-15"}})
# Executes:
#   SELECT * FROM reservations
#   WHERE (during_start IS NULL OR during_start <= :range_start)
#     AND (during_end IS NULL OR during_end >= :range_end)
```

NULL semantics: NULL means "unbounded" (infinity in that direction).

### A2.4 Full-Text Search → Inverted Index Tables

Basic full-text search via application-maintained inverted index.

#### Schema Definition

```python
# models/articles.py
from declaro_persistum import table, field

@table("articles")
class Article(BaseModel):
    id: UUID = field(primary=True)
    title: str
    body: str = field(
        search=True,
        search_config={"min_word_length": 3, "stop_words": "english"},
    )
```

#### Generated Schema

```sql
-- Main table unchanged
CREATE TABLE articles (
    id UUID PRIMARY KEY,
    title TEXT,
    body TEXT
);

-- Inverted index table (system-managed)
CREATE TABLE articles_body_search (
    article_id UUID NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    term TEXT NOT NULL,
    position INTEGER NOT NULL,   -- word position for phrase queries
    PRIMARY KEY (article_id, term, position)
);
CREATE INDEX articles_body_search_term_idx ON articles_body_search(term);
```

#### Tokenization (Application Layer)

```python
def tokenize(text: str, config: SearchConfig) -> list[tuple[str, int]]:
    """
    Tokenize text into (term, position) pairs.
    
    - Lowercase
    - Remove punctuation
    - Filter by min_word_length
    - Remove stop words
    - Optionally stem (snowball)
    """
    ...
```

On INSERT/UPDATE to `articles.body`:
1. Delete existing terms: `DELETE FROM articles_body_search WHERE article_id = :id`
2. Tokenize new body
3. Insert terms: `INSERT INTO articles_body_search VALUES ...`

#### Query Abstraction

```python
# Single term search
select("*", from_table="articles", where="body SEARCH :query", params={"query": "database"})
# Executes:
#   SELECT DISTINCT a.* FROM articles a
#   JOIN articles_body_search s ON a.id = s.article_id
#   WHERE s.term = 'database'

# Multi-term AND search
select("*", from_table="articles", where="body SEARCH :query", params={"query": "database performance"})
# Executes:
#   SELECT a.* FROM articles a
#   JOIN articles_body_search s1 ON a.id = s1.article_id AND s1.term = 'database'
#   JOIN articles_body_search s2 ON a.id = s2.article_id AND s2.term = 'performance'

# Prefix search
select("*", from_table="articles", where="body SEARCH :query", params={"query": "data*"})
# Executes:
#   SELECT DISTINCT a.* FROM articles a
#   JOIN articles_body_search s ON a.id = s.article_id
#   WHERE s.term LIKE 'data%'
```

#### Limitations vs PostgreSQL tsvector

| Feature | PostgreSQL FTS | Inverted Index |
|---------|----------------|----------------|
| Ranking | Yes (ts_rank) | No |
| Phrase search | Yes | Possible via position |
| Stemming | Built-in | Application layer |
| Performance | Highly optimized | Good, not as fast |
| Portability | PostgreSQL only | All dialects |

For advanced FTS needs, recommend PostgreSQL directly or external search (Meilisearch, Typesense).

### A2.5 Hierarchies → Closure Tables

Trees and DAGs via closure table pattern.

#### Schema Definition

```python
# models/categories.py
from declaro_persistum import table, field

@table("categories")
class Category(BaseModel):
    id: UUID = field(primary=True)
    name: str
    parent_id: UUID | None = field(
        references="categories.id",
        closure=True,  # maintain transitive closure
    )
```

#### Generated Schema

```sql
CREATE TABLE categories (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    parent_id UUID REFERENCES categories(id) ON DELETE CASCADE
);
CREATE INDEX categories_parent_idx ON categories(parent_id);

-- Closure table (system-managed)
CREATE TABLE categories_closure (
    ancestor_id UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    descendant_id UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    depth INTEGER NOT NULL,
    PRIMARY KEY (ancestor_id, descendant_id)
);
CREATE INDEX categories_closure_descendant_idx ON categories_closure(descendant_id);
CREATE INDEX categories_closure_depth_idx ON categories_closure(ancestor_id, depth);
```

#### Closure Maintenance

On INSERT:
```sql
-- Self-reference (depth 0)
INSERT INTO categories_closure (ancestor_id, descendant_id, depth)
VALUES (:new_id, :new_id, 0);

-- Copy ancestors from parent, increment depth
INSERT INTO categories_closure (ancestor_id, descendant_id, depth)
SELECT ancestor_id, :new_id, depth + 1
FROM categories_closure
WHERE descendant_id = :parent_id;
```

On UPDATE (parent change): delete old ancestry, recompute.

On DELETE: CASCADE handles it.

#### Query Abstraction

```python
# All descendants of X (any depth)
select("*", from_table="categories", where="DESCENDANT OF :id", params={"id": parent_id})
# Executes:
#   SELECT c.* FROM categories c
#   JOIN categories_closure cl ON c.id = cl.descendant_id
#   WHERE cl.ancestor_id = :id AND cl.depth > 0

# Direct children only
select("*", from_table="categories", where="CHILD OF :id", params={"id": parent_id})
# Executes:
#   SELECT c.* FROM categories c
#   JOIN categories_closure cl ON c.id = cl.descendant_id
#   WHERE cl.ancestor_id = :id AND cl.depth = 1

# All ancestors of X
select("*", from_table="categories", where="ANCESTOR OF :id", params={"id": child_id})
# Executes:
#   SELECT c.* FROM categories c
#   JOIN categories_closure cl ON c.id = cl.ancestor_id
#   WHERE cl.descendant_id = :id AND cl.depth > 0

# Path from root to X (ordered)
select("*", from_table="categories", where="PATH TO :id", params={"id": node_id})
# Executes:
#   SELECT c.* FROM categories c
#   JOIN categories_closure cl ON c.id = cl.ancestor_id
#   WHERE cl.descendant_id = :id
#   ORDER BY cl.depth DESC
```

#### Advantages Over Recursive CTEs

| Aspect | Recursive CTE | Closure Table |
|--------|---------------|---------------|
| Read performance | O(depth) queries | O(1) join |
| Write performance | O(1) | O(depth) inserts |
| SQLite support | Limited | Full |
| Subtree queries | Complex | Simple join |
| Depth queries | Requires counting | Stored in table |

Best for read-heavy hierarchies. Write-heavy trees may prefer adjacency list with CTEs.

### A2.6 Events → Polling Table

Cross-dialect event/notification system.

#### Configuration

Events are configured in the `Config` class or environment:

```python
# config.py
from declaro_persistum import EventsConfig

events_config = EventsConfig(
    enabled=True,
    retention_hours=24,      # auto-cleanup old events
    channels=["orders", "users", "system"],
)
```

#### Generated Schema

```sql
CREATE TABLE declaro_events (
    id BIGSERIAL PRIMARY KEY,          -- PostgreSQL
    -- id INTEGER PRIMARY KEY AUTOINCREMENT,  -- SQLite
    channel TEXT NOT NULL,
    payload TEXT,                       -- JSON as text for portability
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX declaro_events_channel_id_idx ON declaro_events(channel, id);
CREATE INDEX declaro_events_created_idx ON declaro_events(created_at);
```

#### Publish/Subscribe API

```python
# Publish event
async def publish(
    connection: AsyncConnection,
    channel: str,
    payload: dict | None = None
) -> int:
    """
    Publish event to channel, return event ID.
    """
    result = await connection.fetchrow(
        "INSERT INTO declaro_events (channel, payload, created_at) "
        "VALUES (:channel, :payload, now()) RETURNING id",
        {"channel": channel, "payload": json.dumps(payload) if payload else None}
    )
    return result["id"]

# Subscribe to channel
async def subscribe(
    connection: AsyncConnection,
    channel: str,
    last_seen_id: int = 0,
    poll_interval_ms: int = 100,
    batch_size: int = 100
) -> AsyncIterator[Event]:
    """
    Subscribe to channel, yield events as they arrive.
    
    Poll interval determines latency vs load tradeoff:
      - 100ms: ~50ms avg latency, 10 QPS per subscriber
      - 20ms:  ~10ms avg latency, 50 QPS per subscriber
    """
    current_id = last_seen_id
    while True:
        events = await connection.fetch(
            "SELECT id, channel, payload, created_at FROM declaro_events "
            "WHERE channel = :channel AND id > :last_id "
            "ORDER BY id LIMIT :limit",
            {"channel": channel, "last_id": current_id, "limit": batch_size}
        )
        for event in events:
            current_id = event["id"]
            yield Event(
                id=event["id"],
                channel=event["channel"],
                payload=json.loads(event["payload"]) if event["payload"] else None,
                created_at=event["created_at"]
            )
        await asyncio.sleep(poll_interval_ms / 1000)

# Cleanup old events (run periodically)
async def cleanup_events(connection: AsyncConnection, retention_hours: int = 24):
    """Delete events older than retention period."""
    await connection.execute(
        "DELETE FROM declaro_events WHERE created_at < now() - interval ':hours hours'",
        {"hours": retention_hours}
    )
```

#### Configuration

```python
events_config = EventsConfig(
    enabled=True,
    poll_interval_ms=100,      # 10ms avg latency
    retention_hours=24,
    cleanup_interval_minutes=60,
)
```

#### Latency Characteristics

| Poll Interval | Avg Latency | QPS per Subscriber |
|---------------|-------------|-------------------|
| 10ms | 5ms | 100 |
| 20ms | 10ms | 50 |
| 50ms | 25ms | 20 |
| 100ms | 50ms | 10 |

The query is an index scan; database handles high QPS easily.

For true real-time (sub-5ms), use PostgreSQL LISTEN/NOTIFY directly or external pub/sub.

---

## A3. Query Layer Extensions

### A3.1 Native Function Mapping

Push computation to database instead of fetching rows to Python.

#### Aggregate Functions

```python
from declaro_persistum.query import select, sum_, count_, avg_, min_, max_

# Instead of: fetch all rows, sum in Python
# Do: let database compute
result = await select(
    "user_id",
    sum_("amount"),           # → SUM(amount)
    count_("*"),              # → COUNT(*)
    avg_("amount"),           # → AVG(amount)
    min_("created_at"),       # → MIN(created_at)
    max_("created_at"),       # → MAX(created_at)
    from_table="orders",
    group_by=["user_id"],
    having="SUM(amount) > 1000"
).execute(conn)
```

#### Scalar Functions

```python
from declaro_persistum.query import select, lower_, upper_, coalesce_, length_

result = await select(
    "id",
    lower_("email"),          # → LOWER(email)
    coalesce_("nickname", "name"),  # → COALESCE(nickname, name)
    from_table="users"
).execute(conn)
```

#### Type Definition

```python
class SQLFunction(Protocol):
    """Protocol for SQL function wrappers."""
    
    def to_sql(self, dialect: str) -> str:
        """Generate SQL for this function call."""
        ...
    
    @property
    def alias(self) -> str | None:
        """Optional column alias for result."""
        ...
```

### A3.2 Dialect-Aware Function Translation

Some functions have different syntax per dialect.

#### Translation Table

```python
FUNCTION_TRANSLATIONS: dict[str, dict[str, str]] = {
    "now": {
        "postgresql": "now()",
        "sqlite": "datetime('now')",
        "turso": "datetime('now')",
        "libsql": "datetime('now')",
    },
    "current_timestamp": {
        "postgresql": "CURRENT_TIMESTAMP",
        "sqlite": "CURRENT_TIMESTAMP",
        "turso": "CURRENT_TIMESTAMP",
        "libsql": "CURRENT_TIMESTAMP",
    },
    "gen_random_uuid": {
        "postgresql": "gen_random_uuid()",
        "sqlite": "lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))),2) || '-' || substr('89ab',abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))),2) || '-' || lower(hex(randomblob(6)))",
        "turso": "lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))),2) || '-' || substr('89ab',abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))),2) || '-' || lower(hex(randomblob(6)))",
        "libsql": "lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' || substr(lower(hex(randomblob(2))),2) || '-' || substr('89ab',abs(random()) % 4 + 1, 1) || substr(lower(hex(randomblob(2))),2) || '-' || lower(hex(randomblob(6)))",
    },
    "concat": {
        "postgresql": "CONCAT({args})",
        "sqlite": "{args:' || '}",       # a || b || c
        "turso": "{args:' || '}",
        "libsql": "{args:' || '}",
    },
    "string_agg": {
        "postgresql": "STRING_AGG({arg}, {separator})",
        "sqlite": "GROUP_CONCAT({arg}, {separator})",
        "turso": "GROUP_CONCAT({arg}, {separator})",
        "libsql": "GROUP_CONCAT({arg}, {separator})",
    },
    "extract_year": {
        "postgresql": "EXTRACT(YEAR FROM {arg})",
        "sqlite": "CAST(strftime('%Y', {arg}) AS INTEGER)",
        "turso": "CAST(strftime('%Y', {arg}) AS INTEGER)",
        "libsql": "CAST(strftime('%Y', {arg}) AS INTEGER)",
    },
    "date_add_days": {
        "postgresql": "{date} + interval '{days} days'",
        "sqlite": "datetime({date}, '+{days} days')",
        "turso": "datetime({date}, '+{days} days')",
        "libsql": "datetime({date}, '+{days} days')",
    },
    "json_extract": {
        "postgresql": "{column}->>{path}",
        "sqlite": "json_extract({column}, '$.{path}')",
        "turso": "json_extract({column}, '$.{path}')",
        "libsql": "json_extract({column}, '$.{path}')",
    },
}
```

#### Usage

```python
from declaro_persistum.query import now_, gen_random_uuid_, extract_year_

# Portable timestamp
insert(conn, "events", {
    "id": gen_random_uuid_(),
    "created_at": now_()
})
# PostgreSQL: gen_random_uuid(), now()
# SQLite: (complex uuid expression), datetime('now')

# Portable year extraction
select(
    extract_year_("created_at"),
    count_("*"),
    from_table="orders",
    group_by=[extract_year_("created_at")]
)
# PostgreSQL: EXTRACT(YEAR FROM created_at)
# SQLite: CAST(strftime('%Y', created_at) AS INTEGER)
```

---

## A4. Observability

### A4.1 Query Timing

Lightweight instrumentation on every query.

#### Implementation

```python
async def execute_with_timing(
    connection: AsyncConnection,
    sql: str,
    params: dict[str, Any],
    *,
    observer: QueryObserver | None = None
) -> Any:
    """
    Execute query with timing instrumentation.
    
    Overhead: ~100 nanoseconds (two clock reads + comparison)
    """
    start = time.monotonic_ns()
    try:
        result = await connection.execute(sql, params)
        return result
    finally:
        elapsed_ns = time.monotonic_ns() - start
        elapsed_ms = elapsed_ns / 1_000_000
        
        if observer:
            observer.record(
                fingerprint=fingerprint_query(sql),
                elapsed_ms=elapsed_ms,
                params_hash=hash_params(params)
            )
```

#### Query Fingerprinting

```python
def fingerprint_query(sql: str) -> str:
    """
    Normalize query to fingerprint for aggregation.
    
    'SELECT * FROM users WHERE id = :id' → 'SELECT * FROM users WHERE id = ?'
    
    Groups all parameterized variants of same query.
    """
    # Replace :param_name and $1 style params with ?
    normalized = re.sub(r':\w+|\$\d+', '?', sql)
    # Remove extra whitespace
    normalized = ' '.join(normalized.split())
    return normalized
```

### A4.2 Slow Query Recording

Record queries exceeding threshold for analysis.

#### Configuration

```python
from declaro_persistum import ObservabilityConfig

observability_config = ObservabilityConfig(
    enabled=True,
    slow_threshold_ms=500,
    sample_rate=1.0,            # record all slow queries
    storage="table",            # or "file", "callback"
    retention_hours=168,        # 7 days
)
```

#### Storage Schema

```sql
CREATE TABLE declaro_slow_queries (
    id BIGSERIAL PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    sql_text TEXT NOT NULL,
    elapsed_ms REAL NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX declaro_slow_queries_fingerprint_idx ON declaro_slow_queries(fingerprint);
CREATE INDEX declaro_slow_queries_elapsed_idx ON declaro_slow_queries(elapsed_ms DESC);
CREATE INDEX declaro_slow_queries_recorded_idx ON declaro_slow_queries(recorded_at);
```

#### Observer Implementation

```python
class SlowQueryObserver:
    """Record slow queries for later analysis."""
    
    def __init__(
        self,
        threshold_ms: float = 500,
        connection: AsyncConnection | None = None
    ):
        self.threshold_ms = threshold_ms
        self.connection = connection
    
    async def record(
        self,
        fingerprint: str,
        sql: str,
        elapsed_ms: float
    ):
        if elapsed_ms < self.threshold_ms:
            return
        
        if self.connection:
            await self.connection.execute(
                "INSERT INTO declaro_slow_queries (fingerprint, sql_text, elapsed_ms) "
                "VALUES (:fingerprint, :sql, :elapsed)",
                {"fingerprint": fingerprint, "sql": sql, "elapsed": elapsed_ms}
            )
```

### A4.3 Automatic Index Recommendations

Analyze slow queries and recommend indexes.

#### Analysis Command

```
declaro analyze --connection $DATABASE_URL

Slow Query Analysis (last 7 days):

🔴 High Impact (estimated improvement >50%):

  Fingerprint: SELECT * FROM orders WHERE user_id = ? AND status = ?
  Occurrences: 47,312
  Avg latency: 230ms
  P95 latency: 890ms
  
  Current execution: Sequential scan on orders (2.1M rows)
  
  Recommendation:
    CREATE INDEX orders_user_id_status_idx ON orders(user_id, status);
  
  Estimated new latency: <10ms
  
  [Apply automatically? y/N]

🟡 Medium Impact (estimated improvement 20-50%):

  Fingerprint: SELECT * FROM users WHERE email = ?
  Occurrences: 12,847
  Avg latency: 45ms
  
  Index exists: users_email_idx
  Issue: Type mismatch causing cast (varchar vs text)
  
  Recommendation: Align column type with query parameter type

🟢 Performing Well:
  23 other query patterns within acceptable latency
```

### A4.4 Auto-Index Creation

Optionally create indexes automatically.

#### Configuration

```python
from declaro_persistum import AutoIndexConfig

auto_index_config = AutoIndexConfig(
    enabled=True,
    mode="auto",                # "auto" | "recommend" | "off"
    min_occurrences=1000,       # queries before considering
    min_latency_ms=200,         # only optimize slow queries
    max_indexes_per_table=10,   # prevent index bloat
    excluded_tables=["audit_log", "declaro_events"],
)
```

#### Safety Model

Automatic index creation is safe because:

1. **No locks** (PostgreSQL): `CREATE INDEX CONCURRENTLY` doesn't block writes
2. **No query breakage**: Indexes only help or are ignored; never break queries
3. **Reversible**: `DROP INDEX` if wrong
4. **Bounded**: `max_indexes_per_table` prevents bloat

Only risk: disk space. Check before creating:

```python
async def estimate_index_size(
    connection: AsyncConnection,
    table: str,
    columns: list[str]
) -> int:
    """Estimate index size in bytes based on column stats."""
    ...

async def check_disk_space(connection: AsyncConnection) -> bool:
    """Ensure sufficient disk space for new index."""
    ...
```

#### Implementation

```python
async def auto_create_index(
    connection: AsyncConnection,
    table: str,
    columns: list[str],
    *,
    unique: bool = False,
    concurrently: bool = True
) -> str:
    """
    Create index automatically based on query patterns.
    
    Returns index name.
    """
    index_name = f"{table}_{'_'.join(columns)}_auto_idx"
    
    # Check we haven't exceeded max indexes
    existing = await count_indexes(connection, table)
    if existing >= config.max_indexes_per_table:
        log.warning(f"Table {table} has {existing} indexes; skipping auto-index")
        return None
    
    # Check disk space
    if not await check_disk_space(connection):
        log.warning("Insufficient disk space for new index")
        return None
    
    # Create index
    concurrent = "CONCURRENTLY" if concurrently and dialect == "postgresql" else ""
    unique_kw = "UNIQUE" if unique else ""
    
    await connection.execute(
        f"CREATE {unique_kw} INDEX {concurrent} {index_name} "
        f"ON {table}({', '.join(columns)})"
    )
    
    log.info(f"Created index {index_name}")
    return index_name
```

---

## A5. Type Definitions Summary

All new types from this addendum:

```python
# types.py additions

class Trigger(TypedDict, total=False):
    timing: Literal["before", "after", "instead_of"]
    event: str | list[str]
    for_each: Literal["row", "statement"]
    when: str
    body: str
    execute: str


class Procedure(TypedDict, total=False):
    language: Literal["sql", "plpgsql"]
    returns: str
    parameters: list[Parameter]
    body: str


class Parameter(TypedDict):
    name: str
    type: str
    default: str | None


class View(TypedDict, total=False):
    query: str
    materialized: bool
    refresh: Literal["on_demand", "on_commit"]


class Enum(TypedDict):
    type: Literal["enum"]
    values: list[str]


class ArrayColumn(TypedDict):
    type: str  # "array<text>", "array<integer>", etc.


class MapColumn(TypedDict):
    type: str  # "map<text, text>", etc.


class RangeColumn(TypedDict, total=False):
    type: str  # "range<timestamptz>", "range<numeric>", etc.
    start_required: bool
    end_required: bool


class SearchConfig(TypedDict, total=False):
    min_word_length: int
    stop_words: str  # language name or "none"
    stemmer: str     # "snowball" or "none"


class EventsConfig(TypedDict, total=False):
    enabled: bool
    poll_interval_ms: int
    retention_hours: int
    channels: list[str]


class ObservabilityConfig(TypedDict, total=False):
    enabled: bool
    slow_threshold_ms: float
    sample_rate: float
    storage: Literal["table", "file", "callback"]
    retention_hours: int


class AutoIndexConfig(TypedDict, total=False):
    enabled: bool
    mode: Literal["auto", "recommend", "off"]
    min_occurrences: int
    min_latency_ms: float
    max_indexes_per_table: int
    excluded_tables: list[str]
```

---

## A6. File Structure Additions

```
models/
├── users.py                # Pydantic models with @table decorator
├── orders.py               # Table definitions
├── views.py                # View definitions with @view decorator
├── procedures.py           # Stored procedure definitions
└── snapshot.toml           # Auto-generated: last applied state

migrations/
└── pending.toml            # Ephemeral: ambiguity decisions

declaro_persistum/
├── ...                     # existing
├── abstractions/
│   ├── __init__.py
│   ├── arrays.py           # NEW: array → junction table
│   ├── maps.py             # NEW: map → junction table
│   ├── ranges.py           # NEW: range → start/end columns
│   ├── search.py           # NEW: FTS → inverted index
│   ├── hierarchy.py        # NEW: tree → closure table
│   ├── enums.py            # NEW: Literal → lookup table + FK
│   └── events.py           # NEW: pub/sub → polling table
├── functions/
│   ├── __init__.py
│   ├── aggregates.py       # NEW: sum_, count_, etc.
│   ├── scalars.py          # NEW: lower_, coalesce_, etc.
│   └── translations.py     # NEW: dialect-specific translations
└── observability/
    ├── __init__.py
    ├── timing.py           # NEW: query instrumentation
    ├── slow_queries.py     # NEW: slow query recording
    └── auto_index.py       # NEW: automatic index creation
```

---

## A7. Implementation Order

Recommended order for Claude Code implementation:

**Phase 1: Core Extensions**
1. Enums (simplest, immediate value)
2. Triggers and procedures (PostgreSQL first, then stubs for SQLite)
3. Views

**Phase 2: Portable Abstractions**
4. Arrays → junction tables
5. Maps → junction tables
6. Ranges → start/end columns
7. Hierarchies → closure tables

**Phase 3: Query Layer**
8. Native function mapping (aggregates)
9. Dialect-aware function translation

**Phase 4: Observability**
10. Query timing instrumentation
11. Slow query recording
12. Auto-index recommendations
13. Auto-index creation

**Phase 5: Advanced**
14. Full-text search (optional, complex)
15. Events/polling (optional, simple but niche)

---

## A8. Quality Checklist

- [ ] All abstractions generate valid SQL for PostgreSQL, SQLite, Turso (embedded), and LibSQL (cloud)
- [ ] Junction table patterns include proper CASCADE deletes
- [ ] Closure table maintains referential integrity
- [ ] Function translations tested per dialect (postgresql, sqlite, turso, libsql)
- [ ] Observability has negligible overhead (<1ms per query)
- [ ] Auto-index respects safety constraints
- [ ] Error messages explain dialect limitations clearly
- [ ] All new types have complete TypedDict definitions
- [ ] Pydantic model loader correctly detects Literal types for enum abstraction
- [ ] TursoCloudManager tested against real Turso Platform API
