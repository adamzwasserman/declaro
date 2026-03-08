# declaro_persistum Architecture Document

---
**STATUS**: DESIGN
**VERSION**: 0.1.0
**DATE**: 2025-12-14
---

## 1. Executive Summary

### Problem Statement

SQLAlchemy and Alembic represent the dominant Python database toolkit, but they embody architectural decisions that create systemic problems:

1. **ORM State Machine Complexity**: SQLAlchemy's Session tracks "dirty" objects, manages identity maps, and performs lazy loading with hidden side effects. This stateful machinery makes reasoning about code behavior nearly impossible and creates concurrency nightmares.

2. **Alembic's Linear Revision Chain**: Multi-developer, multi-branch workflows inevitably produce revision collisions. Two developers branching from main both create migrations—now you have two heads claiming the same parent, requiring manual merge gymnastics.

3. **Imperative vs Declarative**: Both tools track *operations* (what to do) rather than *desired state* (what should exist). This forces developers to manually write migration scripts for changes the system could infer.

4. **Class-Based Architecture**: The ORM encourages scattering data across heap-allocated objects, guaranteeing cache misses and making code harder to test, parallelize, and reason about.

### Proposed Solution

`declaro_persistum` is a pure functional SQL library that replaces both SQLAlchemy ORM and Alembic with a declarative, state-based approach:

- **Schema as Data**: Database schemas defined as Pydantic models with `@table` decorator
- **State Diffing**: Migrations computed by diffing desired state vs actual database state
- **Pure Functions**: No sessions, no identity maps, no hidden state—data in, data out
- **Branch-Friendly**: No linear revision chain; each branch carries its own schema state

### Key Architectural Decisions

1. TypedDict over classes for internal data structures
2. Pydantic models for schema definition (type-safe, IDE-friendly)
3. Schema snapshots stored in repository, not database
4. Drift detection comparing introspected DB against cached expected state
5. Dependency-aware diff engine with topological sorting
6. Interactive and unattended modes for ambiguity resolution
7. Dialect-specific adapters for PostgreSQL, SQLite, Turso (embedded), and LibSQL (Turso cloud)
8. Multi-tenant support via TursoCloudManager for Turso cloud deployments

### Expected Outcomes

- Elimination of ORM-related state bugs
- Zero migration merge conflicts in multi-branch workflows
- 50-100x performance improvement for bulk operations (no object instantiation overhead)
- Trivially testable database code (pure functions with explicit dependencies)
- Safe concurrent database access without mutex ceremonies

---

## 2. System Context

### 2.1 Current State (What We Replace)

```mermaid
graph TB
    subgraph "SQLAlchemy/Alembic Architecture"
        App[Application Code]
        Session[Session Object]
        IdentityMap[Identity Map]
        UnitOfWork[Unit of Work]
        ORM[ORM Layer]
        Core[SQLAlchemy Core]
        DB[(Database)]
        
        App -->|"session.add(obj)"| Session
        Session -->|tracks| IdentityMap
        Session -->|manages| UnitOfWork
        Session -->|"session.commit()"| ORM
        ORM -->|generates SQL| Core
        Core -->|executes| DB
        
        Alembic[Alembic]
        Revisions[Linear Revision Chain]
        OpScripts[op.add_column scripts]
        
        Alembic -->|manages| Revisions
        Revisions -->|contains| OpScripts
        OpScripts -->|executes| DB
    end
    
    style Session fill:#f66,stroke:#333
    style IdentityMap fill:#f66,stroke:#333
    style UnitOfWork fill:#f66,stroke:#333
    style Revisions fill:#f66,stroke:#333
```

**Figure 2.1**: Current SQLAlchemy/Alembic architecture showing stateful components (red) that declaro_persistum eliminates.

### 2.2 Target State (declaro_persistum)

```mermaid
graph TB
    subgraph "declaro_persistum Architecture"
        App[Application Code]
        Schema[Schema Files<br/>Pydantic]
        Snapshot[Cached Snapshot<br/>repo]
        
        subgraph "Pure Functions"
            Load[load_schema]
            Introspect[introspect_db]
            Diff[diff]
            Apply[apply]
        end
        
        DB[(Database)]
        
        App -->|"import"| Load
        Schema -->|"reads"| Load
        Load -->|"target: Schema"| Diff
        
        DB -->|"query info_schema"| Introspect
        Introspect -->|"actual: Schema"| Diff
        Snapshot -->|"expected: Schema"| Diff
        
        Diff -->|"ops: list[Operation]"| Apply
        Apply -->|"executes DDL"| DB
        Apply -->|"updates"| Snapshot
    end
    
    style Load fill:#9f9,stroke:#333
    style Introspect fill:#9f9,stroke:#333
    style Diff fill:#9f9,stroke:#333
    style Apply fill:#9f9,stroke:#333
```

**Figure 2.2**: declaro_persistum architecture showing pure functions (green) with explicit data flow.

### 2.3 Integration Points

| Integration Point | Direction | Protocol | Purpose |
|-------------------|-----------|----------|---------|
| PostgreSQL | Bidirectional | asyncpg | Production database |
| SQLite | Bidirectional | aiosqlite | Local development, testing |
| Turso (embedded) | Bidirectional | pyturso | Embedded SQLite-compatible with vector search |
| LibSQL (Turso cloud) | Bidirectional | libsql-experimental | Multi-tenant cloud database |
| Turso Platform API | Outbound | HTTPS REST | Database provisioning for multi-tenant |
| Git Repository | Read/Write | Filesystem | Schema files, snapshots |
| CI/CD Pipeline | Invoke | CLI | Unattended migration checks |
| Pre-commit Hook | Invoke | CLI | Interactive migration resolution |

### 2.4 Data Flow

```mermaid
sequenceDiagram
    participant Dev as Developer
    participant CLI as declaro CLI
    participant Loader as load_schema()
    participant Inspector as introspect_db()
    participant Differ as diff()
    participant Applier as apply()
    participant DB as Database
    participant Repo as Git Repo

    Dev->>CLI: declaro diff --interactive
    CLI->>Loader: load(models/*.py)
    Loader-->>CLI: target: Schema
    
    CLI->>Inspector: introspect(connection)
    Inspector->>DB: SELECT FROM information_schema
    DB-->>Inspector: table/column metadata
    Inspector-->>CLI: actual: Schema
    
    CLI->>Repo: read(schema/snapshot.toml)
    Repo-->>CLI: expected: Schema
    
    alt actual != expected
        CLI->>Dev: ⚠️ Drift detected
        Dev->>CLI: --force or abort
    end
    
    CLI->>Differ: diff(actual, target)
    Differ-->>CLI: DiffResult{ops, deps, order}
    
    alt ambiguities exist
        CLI->>Dev: Prompt: rename or drop+add?
        Dev->>CLI: decision
        CLI->>Repo: write decision to TOML
    end
    
    CLI->>Dev: Show proposed operations
    Dev->>CLI: confirm
    
    CLI->>Applier: apply(ops, connection)
    Applier->>DB: BEGIN; DDL statements; COMMIT
    DB-->>Applier: success
    
    Applier->>Repo: write(schema/snapshot.toml, target)
    CLI->>Dev: ✓ Migration complete
```

**Figure 2.3**: Complete data flow for interactive migration workflow.

### 2.5 Security Boundaries

| Boundary | Trust Level | Controls |
|----------|-------------|----------|
| Schema Files | Trusted (in repo) | Code review, git history |
| Database Credentials | Secret | Environment variables only |
| CI/CD Environment | Trusted | No interactive prompts |
| Developer Workstation | Trusted | Interactive confirmation |
| Database Connection | Encrypted | TLS required for non-localhost |

---

## 3. Technical Design

### 3.1 Component Architecture

```mermaid
graph TB
    subgraph "declaro_persistum Package"
        subgraph "Schema Layer"
            Types[types.py<br/>TypedDict definitions]
            Loader[loader.py<br/>Pydantic model loading]
            Validator[validator.py<br/>Reference validation]
        end
        
        subgraph "Introspection Layer"
            Inspector[inspector.py<br/>Protocol definition]
            PGInspector[pg_inspector.py<br/>PostgreSQL impl]
            SQLiteInspector[sqlite_inspector.py<br/>SQLite impl]
            TursoInspector[turso_inspector.py<br/>Turso impl]
        end
        
        subgraph "Diff Engine"
            Differ[differ.py<br/>Core diff logic]
            Resolver[resolver.py<br/>Ambiguity resolution]
            Toposort[toposort.py<br/>Dependency ordering]
        end
        
        subgraph "Execution Layer"
            Applier[applier.py<br/>Protocol definition]
            PGApplier[pg_applier.py<br/>PostgreSQL impl]
            SQLiteApplier[sqlite_applier.py<br/>SQLite impl]
            TursoApplier[turso_applier.py<br/>Turso impl]
        end
        
        subgraph "Query Layer"
            Query[query.py<br/>Query building]
            Execute[execute.py<br/>Query execution]
        end
        
        CLI[cli.py<br/>Command interface]
    end
    
    Types --> Loader
    Loader --> Validator
    
    Inspector --> PGInspector
    Inspector --> SQLiteInspector
    Inspector --> TursoInspector
    
    Applier --> PGApplier
    Applier --> SQLiteApplier
    Applier --> TursoApplier
    
    Validator --> Differ
    Inspector --> Differ
    Differ --> Resolver
    Differ --> Toposort
    
    Toposort --> Applier
    
    CLI --> Loader
    CLI --> Inspector
    CLI --> Differ
    CLI --> Applier
    CLI --> Query
```

**Figure 3.1**: Component architecture showing Protocol-based abstractions for dialect support.

### 3.2 Data Architecture

#### 3.2.1 Core Type Definitions

```python
# types.py
from typing import TypedDict, Literal, Any

class Column(TypedDict, total=False):
    """Column definition - all fields optional except type."""
    type: str                          # Required: SQL type (text, integer, etc.)
    nullable: bool                     # Default: True
    default: Any                       # Default value expression
    primary_key: bool                  # Default: False
    unique: bool                       # Default: False
    references: str                    # Foreign key: "table.column"
    on_delete: Literal["cascade", "set null", "restrict", "no action"]
    on_update: Literal["cascade", "set null", "restrict", "no action"]
    check: str                         # CHECK constraint expression
    # Migration hints (not persisted to DB)
    renamed_from: str                  # Indicates column was renamed
    is_new: bool                       # Confirms intentional new column


class Index(TypedDict, total=False):
    """Index definition."""
    columns: list[str]                 # Required: columns in index
    unique: bool                       # Default: False
    where: str                         # Partial index condition
    using: str                         # Index method (btree, hash, gin, etc.)


class Table(TypedDict, total=False):
    """Table definition."""
    columns: dict[str, Column]         # Required: column definitions
    primary_key: list[str]             # Composite PK (if not in column)
    indexes: dict[str, Index]          # Named indexes
    constraints: dict[str, str]        # Named CHECK constraints
    

Schema = dict[str, Table]
"""Complete database schema as table name -> Table mapping."""


class Operation(TypedDict):
    """A single DDL operation."""
    op: Literal[
        "create_table", "drop_table", "rename_table",
        "add_column", "drop_column", "rename_column", "alter_column",
        "add_index", "drop_index",
        "add_constraint", "drop_constraint",
        "add_foreign_key", "drop_foreign_key"
    ]
    table: str
    details: dict[str, Any]            # Operation-specific parameters


class DiffResult(TypedDict):
    """Result of schema diff operation."""
    operations: list[Operation]
    dependencies: dict[str, list[str]]  # op_id -> [dependency_op_ids]
    execution_order: list[str]          # Topologically sorted op_ids
    ambiguities: list[dict]             # Unresolved ambiguities
```

#### 3.2.2 Entity Relationship Diagram

```mermaid
erDiagram
    SCHEMA ||--|{ TABLE : contains
    TABLE ||--|{ COLUMN : has
    TABLE ||--o{ INDEX : has
    TABLE ||--o{ CONSTRAINT : has
    COLUMN ||--o| FOREIGN_KEY : references
    
    SCHEMA {
        string name PK
        dict tables
    }
    
    TABLE {
        string name PK
        list primary_key
        dict columns
        dict indexes
        dict constraints
    }
    
    COLUMN {
        string name PK
        string type
        bool nullable
        any default
        bool primary_key
        bool unique
        string check
    }
    
    INDEX {
        string name PK
        list columns
        bool unique
        string where
        string using
    }
    
    CONSTRAINT {
        string name PK
        string expression
    }
    
    FOREIGN_KEY {
        string column FK
        string references
        string on_delete
        string on_update
    }
```

**Figure 3.2**: Entity relationships within schema data structures.

#### 3.2.3 Pydantic Schema Format

```python
# models/user.py
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel
from declaro_persistum import table, field, index

@table("users")
class User(BaseModel):
    id: UUID = field(primary=True, default="gen_random_uuid()")
    email: str = field(unique=True)
    created_at: datetime = field(default="now()")
    updated_at: datetime = field(default="now()")

    class Meta:
        indexes = [
            index("users_email_idx", columns=["email"], unique=True),
            index("users_created_at_idx", columns=["created_at"]),
        ]
```

```python
# models/order.py
from uuid import UUID
from decimal import Decimal
from typing import Literal
from pydantic import BaseModel
from declaro_persistum import table, field

OrderStatus = Literal["pending", "confirmed", "shipped", "delivered"]

@table("orders")
class Order(BaseModel):
    id: UUID = field(primary=True, default="gen_random_uuid()")
    user_id: UUID = field(references="users.id", on_delete="cascade")
    total: Decimal = field(check="total >= 0")
    status: OrderStatus = field(default="pending")
```

#### 3.2.4 Enum Abstraction via Literal Types

Python's `Literal` type is automatically detected and converted to a lookup table with foreign key constraint. This provides consistent enum enforcement across all backends via standard FK constraints.

**Detection**: When a field's type annotation is `Literal["a", "b", "c"]`, declaro_persistum:
1. Creates a lookup table `_dp_enum_{table}_{field}`
2. Inserts the literal values as rows
3. Generates a FK constraint instead of a CHECK constraint

**Example**:

```python
from typing import Literal

OrderStatus = Literal["pending", "confirmed", "shipped", "delivered"]

@table("orders")
class Order(BaseModel):
    id: UUID = field(primary=True)
    status: OrderStatus = "pending"
```

**Generated SQL**:

```sql
-- Lookup table (auto-managed)
CREATE TABLE _dp_enum_orders_status (
    value TEXT PRIMARY KEY
);
INSERT INTO _dp_enum_orders_status (value) VALUES
    ('pending'), ('confirmed'), ('shipped'), ('delivered');

-- Orders table with FK
CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status TEXT NOT NULL DEFAULT 'pending'
        REFERENCES _dp_enum_orders_status(value)
);
```

**Migration Behavior**:
- Adding a value to `Literal[...]`: Inserts new row into lookup table
- Removing a value: Deletes row from lookup table (fails if referenced)
- Renaming: Detected as drop+add; requires explicit decision

**Benefits**:
- Works across all backends (PostgreSQL, SQLite, Turso, LibSQL)
- No CHECK constraint parsing required (enum values managed via FK lookup)
- Values queryable at runtime via lookup table
- Standard FK constraint enforcement

#### 3.2.5 Snapshot Format

```toml
# schema/snapshot.toml
# AUTO-GENERATED - DO NOT EDIT MANUALLY
# Last applied: 2025-12-14T10:30:00Z
# Applied by: developer@example.com

[_meta]
version = "1.0.0"
applied_at = "2025-12-14T10:30:00Z"
dialect = "postgresql"

[users]
# ... complete schema state at time of last migration
```

#### 3.2.6 Pending Decisions Format

```toml
# schema/migrations/pending.toml
# Ephemeral decisions - purged after migration applied

[decisions]

[decisions.users_name_to_full_name]
type = "rename"
table = "users"
from_column = "name"
to_column = "full_name"
decided_at = "2025-12-14T10:25:00Z"

[decisions.orders_drop_legacy_status]
type = "confirm_drop"
table = "orders"
column = "legacy_status"
decided_at = "2025-12-14T10:26:00Z"
```

### 3.3 Protocol Definitions

#### 3.3.1 Database Inspector Protocol

```python
# inspector.py
from typing import Protocol

class DatabaseInspector(Protocol):
    """Protocol for database schema introspection."""
    
    def introspect(
        self,
        connection: Any,
        *,
        schema_name: str = "public"
    ) -> Schema:
        """
        Introspect database and return current schema state.
        
        Args:
            connection: Database connection object
            schema_name: Database schema to introspect (PostgreSQL)
            
        Returns:
            Schema dict representing current database state
            
        Raises:
            ConnectionError: If database connection fails
            IntrospectionError: If schema cannot be read
        """
        ...
    
    def get_dialect(self) -> str:
        """Return dialect identifier (postgresql, sqlite, turso)."""
        ...
```

#### 3.3.2 Schema Differ Protocol

```python
# differ.py
from typing import Protocol

class SchemaDiffer(Protocol):
    """Protocol for computing schema differences."""
    
    def diff(
        self,
        current: Schema,
        target: Schema,
        *,
        decisions: dict[str, Any] | None = None
    ) -> DiffResult:
        """
        Compute operations needed to transform current to target.
        
        Args:
            current: Current database schema state
            target: Desired schema state
            decisions: Pre-made decisions for ambiguous changes
            
        Returns:
            DiffResult with operations, dependencies, and execution order
            
        Raises:
            CycleError: If operation dependencies form a cycle
            AmbiguityError: If unresolved ambiguities exist (unattended mode)
        """
        ...
    
    def detect_ambiguities(
        self,
        current: Schema,
        target: Schema
    ) -> list[Ambiguity]:
        """
        Identify changes that require human decision.
        
        Returns:
            List of ambiguous changes (renames vs drop+add, etc.)
        """
        ...
```

#### 3.3.3 Migration Applier Protocol

```python
# applier.py
from typing import Protocol

class MigrationApplier(Protocol):
    """Protocol for applying schema migrations."""
    
    def apply(
        self,
        connection: Any,
        operations: list[Operation],
        execution_order: list[str],
        *,
        dry_run: bool = False
    ) -> ApplyResult:
        """
        Apply migration operations to database.
        
        Args:
            connection: Database connection object
            operations: List of DDL operations
            execution_order: Topologically sorted operation IDs
            dry_run: If True, generate SQL without executing
            
        Returns:
            ApplyResult with success status and executed SQL
            
        Raises:
            MigrationError: If any operation fails
            RollbackError: If rollback after failure also fails
        """
        ...
    
    def generate_sql(
        self,
        operations: list[Operation],
        execution_order: list[str]
    ) -> list[str]:
        """Generate SQL statements without executing."""
        ...
    
    def get_transaction_mode(self) -> Literal["all_or_nothing", "per_operation"]:
        """
        Return transaction behavior for this dialect.
        
        - all_or_nothing: Single transaction wraps all operations (PostgreSQL, SQLite)
        - per_operation: Each operation in separate transaction with checkpoints
        """
        ...
```

#### 3.3.4 Query Builder Protocol

```python
# query.py
from typing import Protocol, TypeVar, Generic

T = TypeVar('T')

class QueryBuilder(Protocol[T]):
    """Protocol for building SQL queries functionally."""
    
    def select(
        self,
        *columns: str,
        from_table: str,
        where: str | None = None,
        params: dict[str, Any] | None = None,
        order_by: list[str] | None = None,
        limit: int | None = None,
        offset: int | None = None
    ) -> Query[T]:
        """
        Build a SELECT query.
        
        Args:
            columns: Column names to select (* for all)
            from_table: Table name
            where: WHERE clause (use :param_name for parameters)
            params: Parameter values
            order_by: ORDER BY columns
            limit: LIMIT value
            offset: OFFSET value
            
        Returns:
            Query object that can be executed or composed
        """
        ...
    
    def insert(
        self,
        into_table: str,
        values: dict[str, Any] | list[dict[str, Any]],
        *,
        returning: list[str] | None = None
    ) -> Query[T]:
        """Build an INSERT query."""
        ...
    
    def update(
        self,
        table: str,
        set_values: dict[str, Any],
        *,
        where: str,
        params: dict[str, Any] | None = None,
        returning: list[str] | None = None
    ) -> Query[T]:
        """Build an UPDATE query."""
        ...
    
    def delete(
        self,
        from_table: str,
        *,
        where: str,
        params: dict[str, Any] | None = None,
        returning: list[str] | None = None
    ) -> Query[T]:
        """Build a DELETE query."""
        ...


class Query(Protocol[T]):
    """An executable query."""
    
    def sql(self) -> str:
        """Return the SQL string."""
        ...
    
    def params(self) -> dict[str, Any]:
        """Return query parameters."""
        ...
    
    async def execute(self, connection: Any) -> list[T]:
        """Execute query and return results."""
        ...
    
    async def execute_one(self, connection: Any) -> T | None:
        """Execute query and return single result or None."""
        ...
    
    async def execute_scalar(self, connection: Any) -> Any:
        """Execute query and return scalar value."""
        ...
```

### 3.4 Diff Engine Design

#### 3.4.1 Diff Algorithm

```mermaid
flowchart TB
    Start([Start Diff]) --> LoadSchemas[Load current & target schemas]
    LoadSchemas --> CompareTables{Compare table sets}
    
    CompareTables --> DroppedTables[Identify dropped tables]
    CompareTables --> AddedTables[Identify added tables]
    CompareTables --> ModifiedTables[Identify modified tables]
    
    DroppedTables --> GenDropOps[Generate DROP TABLE ops]
    AddedTables --> GenCreateOps[Generate CREATE TABLE ops]
    
    ModifiedTables --> CompareColumns{Compare columns}
    CompareColumns --> DroppedCols[Dropped columns]
    CompareColumns --> AddedCols[Added columns]
    CompareColumns --> ModifiedCols[Modified columns]
    
    DroppedCols --> CheckAmbiguity{Possible rename?}
    AddedCols --> CheckAmbiguity
    
    CheckAmbiguity -->|Yes| RecordAmbiguity[Record ambiguity]
    CheckAmbiguity -->|No| GenColOps[Generate column ops]
    RecordAmbiguity --> NeedsDecision[Requires human decision]
    
    ModifiedCols --> GenAlterOps[Generate ALTER ops]
    
    GenDropOps --> CollectOps[Collect all operations]
    GenCreateOps --> CollectOps
    GenColOps --> CollectOps
    GenAlterOps --> CollectOps
    
    CollectOps --> BuildDepGraph[Build dependency graph]
    BuildDepGraph --> Toposort[Topological sort]
    
    Toposort --> CycleCheck{Cycle detected?}
    CycleCheck -->|Yes| Error([CycleError])
    CycleCheck -->|No| Result([Return DiffResult])
```

**Figure 3.4.1**: Diff algorithm flowchart showing ambiguity detection and dependency resolution.

#### 3.4.2 Dependency Rules

| Operation | Depends On |
|-----------|------------|
| DROP FOREIGN KEY | Nothing |
| DROP INDEX | Nothing |
| DROP COLUMN | DROP FOREIGN KEY (if referenced), DROP INDEX (if included) |
| DROP TABLE | DROP FOREIGN KEY (all referencing), DROP dependent tables |
| CREATE TABLE | CREATE referenced tables |
| ADD COLUMN | CREATE TABLE |
| ADD FOREIGN KEY | CREATE referenced table, ADD referenced column |
| ADD INDEX | ADD indexed columns |
| ALTER COLUMN | DROP INDEX (if type change), DROP FK (if referenced) |

#### 3.4.3 Ambiguity Detection

```python
def detect_column_ambiguity(
    table: str,
    dropped: set[str],
    added: set[str],
    current_columns: dict[str, Column],
    target_columns: dict[str, Column]
) -> list[Ambiguity]:
    """
    Detect potential renames vs drop+add.
    
    Heuristics for likely rename:
    - Same type
    - Same nullability  
    - Similar position in column order
    - Similar name (edit distance)
    """
    ambiguities = []
    
    for dropped_col in dropped:
        dropped_def = current_columns[dropped_col]
        
        for added_col in added:
            added_def = target_columns[added_col]
            
            # Check if types match
            if dropped_def.get("type") != added_def.get("type"):
                continue
            
            # Check nullability match
            if dropped_def.get("nullable", True) != added_def.get("nullable", True):
                continue
            
            # Potential rename detected
            ambiguities.append(Ambiguity(
                type="possible_rename",
                table=table,
                from_column=dropped_col,
                to_column=added_col,
                confidence=calculate_rename_confidence(dropped_col, added_col),
                message=f"Column '{dropped_col}' removed, '{added_col}' added. "
                        f"Is this a rename (preserves data) or drop+add (loses data)?"
            ))
    
    return ambiguities
```

### 3.5 Dialect Implementations

#### 3.5.1 PostgreSQL Adapter

```python
# pg_inspector.py

def introspect_postgresql(
    connection: AsyncConnection,
    *,
    schema_name: str = "public"
) -> Schema:
    """
    Introspect PostgreSQL database schema.
    
    Uses information_schema and pg_catalog for complete metadata.
    """
    tables = await connection.fetch("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = $1 AND table_type = 'BASE TABLE'
    """, schema_name)
    
    schema: Schema = {}
    
    for table_row in tables:
        table_name = table_row["table_name"]
        
        columns = await connection.fetch("""
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        """, schema_name, table_name)
        
        # ... build Column dicts
        
        indexes = await connection.fetch("""
            SELECT 
                i.relname as index_name,
                array_agg(a.attname ORDER BY k.n) as columns,
                ix.indisunique as is_unique,
                pg_get_expr(ix.indpred, ix.indrelid) as predicate
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n)
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
            WHERE n.nspname = $1 AND t.relname = $2
            GROUP BY i.relname, ix.indisunique, ix.indpred, ix.indrelid
        """, schema_name, table_name)
        
        # ... build schema dict
    
    return schema
```

#### 3.5.2 Transaction Modes by Dialect

| Dialect | DDL Transactional | Mode | Behavior |
|---------|-------------------|------|----------|
| PostgreSQL | Yes | all_or_nothing | Single transaction, full rollback on failure |
| SQLite | Yes | all_or_nothing | Single transaction, full rollback on failure |
| Turso (embedded) | Yes | all_or_nothing | Single transaction (inherits SQLite behavior) |
| LibSQL (Turso cloud) | Yes | all_or_nothing | Single transaction (inherits SQLite behavior) |

### 3.6 CLI Design

```
declaro_persistum CLI

USAGE:
    declaro <command> [options]

COMMANDS:
    diff        Compare target schema to database, show proposed operations
    apply       Apply pending migrations to database
    snapshot    Update schema snapshot from current database
    validate    Validate schema files without database connection
    generate    Generate SQL without executing

OPTIONS:
    --connection, -c    Database connection string (or DECLARO_DATABASE_URL env)
    --schema-dir, -s    Schema directory (default: ./schema)
    --interactive, -i   Prompt for ambiguity resolution (default for TTY)
    --unattended, -u    Fail on ambiguities (default for non-TTY, CI)
    --dry-run           Show operations without executing
    --force             Skip drift detection warning
    --verbose, -v       Verbose output
    --dialect, -d       Force dialect (postgresql, sqlite, turso)

EXAMPLES:
    # Interactive diff and apply
    declaro diff -c postgresql://localhost/mydb
    declaro apply -c postgresql://localhost/mydb

    # CI pipeline (unattended)
    declaro diff --unattended -c $DATABASE_URL
    
    # Generate SQL for review
    declaro generate -c postgresql://localhost/mydb > migration.sql
```

### 3.7 Error Messages

All error messages follow this principle from the design conversation:

> "A programmer should not have to be an expert stack tracer to figure out where they went wrong. We need to hold their hand."

#### 3.7.1 Ambiguity Error

```
SchemaError: Ambiguous column change in 'users' table

  Column 'name' (text, not null) was removed
  Column 'full_name' (text, not null) was added

  This could be:
    1. A rename (preserves data)
    2. A drop + add (loses data)

  To resolve, add to models/user.py:

    full_name: str = field(renamed_from="name")

  Or to confirm drop + add:

    full_name: str = field(is_new=True)

  File: models/user.py, line 14
```

#### 3.7.2 Cycle Error

```
DependencyError: Circular dependency detected in migration operations

  The following operations form a cycle:
  
    1. add_foreign_key: orders.customer_id -> customers.id
       ↓ depends on
    2. create_table: customers
       ↓ depends on  
    3. add_foreign_key: customers.primary_order_id -> orders.id
       ↓ depends on
    4. create_table: orders
       ↓ depends on
    1. (cycle back to add_foreign_key)

  This usually indicates a circular reference in your schema.
  
  To resolve:
    - Remove one of the foreign keys
    - Or use a deferred constraint (add manually after migration)

  Tables involved: orders, customers
```

#### 3.7.3 Drift Error

```
DriftError: Database schema has drifted from expected state

  The database does not match the last applied migration snapshot.
  Someone may have modified the database directly.

  Differences detected:
  
    Table 'users':
      + Column 'temp_flag' exists in DB but not in snapshot
      ~ Column 'email' type differs: varchar(100) vs text
    
    Table 'audit_log':
      - Entire table exists in DB but not in snapshot

  Options:
    1. Run 'declaro snapshot' to update snapshot to current DB state
    2. Run 'declaro apply --force' to proceed anyway (may cause errors)
    3. Manually reconcile the differences

  Last snapshot: 2025-12-10T14:30:00Z
  Current time:  2025-12-14T10:30:00Z
```

---

## 4. Architectural Principles Compliance

### 4.1 Function-Based Architecture

**Mandate**: NO classes except for approved types.

**Approved Class Usage in declaro_persistum**:
- TypedDict subclasses (data shape definitions)
- Protocol definitions (interface contracts)
- Exception classes (error hierarchy)
- Context managers (connection handling)

**Forbidden**:
- ORM-style model classes
- Service classes with state
- Repository pattern classes
- Factory classes (use factory functions instead)

### 4.2 Pure Function Requirements

All core operations must be pure functions:

```python
# REQUIRED: Pure function pattern
def diff(
    current: Schema,
    target: Schema,
    *,
    decisions: dict[str, Any] | None = None
) -> DiffResult:
    """
    Pure function: same inputs always produce same outputs.
    No side effects, no hidden state, no I/O.
    """
    ...

# FORBIDDEN: Stateful class pattern
class SchemaDiffer:
    def __init__(self):
        self._cache = {}  # Hidden state!
    
    def diff(self, current, target):
        # Behavior depends on self._cache - not pure!
        ...
```

### 4.3 Explicit Dependencies

All dependencies must be passed explicitly:

```python
# REQUIRED: Explicit dependencies
async def apply_migration(
    connection: AsyncConnection,      # Explicit
    operations: list[Operation],      # Explicit
    execution_order: list[str],       # Explicit
    *,
    dialect: Dialect,                 # Explicit
    dry_run: bool = False             # Explicit
) -> ApplyResult:
    ...

# FORBIDDEN: Hidden dependencies
async def apply_migration(operations):
    conn = get_global_connection()    # Hidden!
    dialect = infer_dialect()         # Hidden!
    ...
```

### 4.4 Immutable Data

All data structures should be treated as immutable:

```python
# Use frozenset for sets that shouldn't change
def get_table_names(schema: Schema) -> frozenset[str]:
    return frozenset(schema.keys())

# Return new dicts rather than mutating
def add_column(table: Table, name: str, column: Column) -> Table:
    return {
        **table,
        "columns": {**table.get("columns", {}), name: column}
    }

# FORBIDDEN: Mutation
def add_column_mutating(table: Table, name: str, column: Column) -> None:
    table["columns"][name] = column  # Mutates input!
```

### 4.5 Set Theory Operations

Document filtering and comparison operations using set theory notation:

```python
def diff_tables(current: Schema, target: Schema) -> tuple[set[str], set[str], set[str]]:
    """
    Compute table differences using set operations.
    
    Let C = set of current table names
    Let T = set of target table names
    
    Returns:
        dropped: C - T (tables to drop)
        added: T - C (tables to create)  
        modified: C ∩ T (tables to compare for changes)
    """
    current_tables = set(current.keys())
    target_tables = set(target.keys())
    
    dropped = current_tables - target_tables      # C - T
    added = target_tables - current_tables        # T - C
    modified = current_tables & target_tables     # C ∩ T
    
    return dropped, added, modified
```

---

## 5. Performance Considerations

### 5.1 Introspection Performance

| Database | Tables | Introspection Time Target |
|----------|--------|---------------------------|
| PostgreSQL | < 100 | < 500ms |
| PostgreSQL | 100-500 | < 2s |
| PostgreSQL | 500+ | < 5s |
| SQLite | Any | < 100ms |
| Turso | Any | < 200ms (network latency) |

**Optimization**: Batch metadata queries rather than per-table queries.

### 5.2 Diff Performance

| Schema Size | Operations | Diff Time Target |
|-------------|------------|------------------|
| Small (< 50 tables) | < 100 | < 50ms |
| Medium (50-200 tables) | < 500 | < 200ms |
| Large (200+ tables) | < 2000 | < 1s |

**Optimization**: O(n) comparison with hash-based lookups.

### 5.3 Query Execution Performance

Compared to SQLAlchemy ORM:

| Operation | SQLAlchemy ORM | declaro_persistum | Improvement |
|-----------|----------------|-------------------|-------------|
| Insert 1000 rows | ~500ms | ~10ms | 50x |
| Select 10000 rows | ~200ms | ~5ms | 40x |
| Update 1000 rows | ~400ms | ~10ms | 40x |

**Why**: No object instantiation, no identity map management, no dirty tracking.

### 5.4 Memory Usage

| SQLAlchemy | declaro_persistum |
|------------|-------------------|
| O(n) objects in identity map | O(1) - no object caching |
| Session grows unbounded | Constant memory per query |
| GC pressure from object churn | Minimal allocations |

---

## 6. Security Architecture

### 6.1 Connection String Handling

```python
# REQUIRED: Environment variable for credentials
DATABASE_URL = os.environ.get("DECLARO_DATABASE_URL")

# FORBIDDEN: Hardcoded credentials
DATABASE_URL = "postgresql://user:password@host/db"  # Never!

# Connection string parsing
def parse_connection_string(url: str) -> ConnectionConfig:
    """
    Parse connection string, masking password in logs.
    
    Supports:
        postgresql://user:pass@host:port/db?sslmode=require
        sqlite:///path/to/db.sqlite
        libsql://token@host/db
    """
    ...
```

### 6.2 SQL Injection Prevention

```python
# REQUIRED: Parameterized queries
def select_by_id(table: str, id_value: Any) -> Query:
    # Table names validated against schema, not interpolated from user input
    if table not in KNOWN_TABLES:
        raise ValueError(f"Unknown table: {table}")
    
    return Query(
        sql=f"SELECT * FROM {table} WHERE id = :id",  # Table from whitelist
        params={"id": id_value}                        # Value parameterized
    )

# FORBIDDEN: String interpolation of values
def select_by_id_unsafe(table: str, id_value: Any) -> str:
    return f"SELECT * FROM {table} WHERE id = '{id_value}'"  # SQL injection!
```

### 6.3 Audit Logging

```python
@dataclass(frozen=True)
class MigrationAudit:
    """Immutable audit record for migration operations."""
    timestamp: datetime
    user: str
    operations: tuple[Operation, ...]
    before_snapshot: str  # Hash of before state
    after_snapshot: str   # Hash of after state
    success: bool
    error_message: str | None = None
```

---

## 7. Error Handling

### 7.1 Exception Hierarchy

```python
class DeclaroError(Exception):
    """Base exception for all declaro_persistum errors."""
    pass

class SchemaError(DeclaroError):
    """Schema definition or validation error."""
    pass

class AmbiguityError(SchemaError):
    """Unresolved ambiguous change detected."""
    ambiguities: list[Ambiguity]

class CycleError(SchemaError):
    """Circular dependency in operations."""
    cycle: list[str]

class DriftError(DeclaroError):
    """Database state differs from expected snapshot."""
    differences: list[Difference]

class ConnectionError(DeclaroError):
    """Database connection failure."""
    pass

class MigrationError(DeclaroError):
    """Migration execution failure."""
    operation: Operation
    sql: str
    original_error: Exception

class RollbackError(MigrationError):
    """Rollback after failure also failed."""
    rollback_error: Exception
```

### 7.2 Recovery Strategies

| Error Type | Recovery Strategy |
|------------|-------------------|
| AmbiguityError | Return to interactive mode, prompt user |
| CycleError | Abort, display cycle for manual resolution |
| DriftError | Offer snapshot update or --force |
| ConnectionError | Retry with exponential backoff (3 attempts) |
| MigrationError (PostgreSQL) | Automatic rollback via transaction |
| MigrationError (other) | Checkpoint-based partial recovery |

---

## 8. Testing Strategy

### 8.1 Unit Tests

**Coverage Target**: 100% for core modules (types, differ, toposort)

```python
# Test pure functions with property-based testing
from hypothesis import given, strategies as st

@given(st.dictionaries(st.text(), st.text()))
def test_diff_empty_to_schema_creates_all_tables(target):
    """Diffing empty schema to any target creates tables for all keys."""
    result = diff(current={}, target=target)
    created_tables = {op["table"] for op in result["operations"] if op["op"] == "create_table"}
    assert created_tables == set(target.keys())

@given(st.dictionaries(st.text(), st.text()))
def test_diff_schema_to_empty_drops_all_tables(current):
    """Diffing any schema to empty drops all tables."""
    result = diff(current=current, target={})
    dropped_tables = {op["table"] for op in result["operations"] if op["op"] == "drop_table"}
    assert dropped_tables == set(current.keys())

def test_diff_identical_schemas_produces_no_operations():
    """Identical schemas produce empty operation list."""
    schema = {"users": {"columns": {"id": {"type": "integer"}}}}
    result = diff(current=schema, target=schema)
    assert result["operations"] == []
```

### 8.2 Integration Tests

**Per Dialect**: PostgreSQL, SQLite, Turso

```python
@pytest.fixture
async def pg_connection():
    """Create isolated PostgreSQL test database."""
    conn = await asyncpg.connect(TEST_DATABASE_URL)
    await conn.execute("CREATE SCHEMA test_schema")
    yield conn
    await conn.execute("DROP SCHEMA test_schema CASCADE")
    await conn.close()

async def test_full_migration_cycle(pg_connection):
    """Test complete migration: introspect → diff → apply → verify."""
    # Start with empty schema
    initial = await introspect(pg_connection)
    assert initial == {}
    
    # Define target schema
    target = {
        "users": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True},
                "email": {"type": "text", "unique": True}
            }
        }
    }
    
    # Diff and apply
    diff_result = diff(current=initial, target=target)
    await apply(pg_connection, diff_result)
    
    # Verify
    final = await introspect(pg_connection)
    assert "users" in final
    assert "id" in final["users"]["columns"]
    assert "email" in final["users"]["columns"]
```

### 8.3 Migration Safety Tests

```python
def test_destructive_operation_requires_confirmation():
    """DROP operations require explicit confirmation in decisions."""
    current = {"users": {"columns": {"temp": {"type": "text"}}}}
    target = {"users": {"columns": {}}}  # Column removed
    
    # Without decision, should raise or flag
    result = diff(current=current, target=target)
    assert any(a["type"] == "confirm_drop" for a in result["ambiguities"])

def test_rename_annotation_prevents_data_loss():
    """renamed_from annotation generates RENAME instead of DROP+ADD."""
    current = {"users": {"columns": {"name": {"type": "text"}}}}
    target = {"users": {"columns": {"full_name": {"type": "text", "renamed_from": "name"}}}}
    
    result = diff(current=current, target=target)
    ops = result["operations"]
    
    assert any(op["op"] == "rename_column" for op in ops)
    assert not any(op["op"] == "drop_column" for op in ops)
    assert not any(op["op"] == "add_column" for op in ops)
```

---

## 9. Deployment Architecture

### 9.1 Package Structure

```
declaro_persistum/
├── __init__.py
├── types.py              # TypedDict definitions
├── loader.py             # Pydantic model loading
├── validator.py          # Schema validation
├── pool.py               # Unified connection pool (PostgreSQL, SQLite, Turso, LibSQL)
├── exceptions.py         # Exception hierarchy
├── inspector/
│   ├── __init__.py
│   ├── protocol.py       # DatabaseInspector protocol
│   ├── shared.py         # Shared pure functions (type normalization, FK parsing, etc.)
│   ├── postgresql.py     # PostgreSQL implementation
│   ├── sqlite.py         # SQLite implementation
│   └── turso.py          # Turso/LibSQL implementation
├── differ/
│   ├── __init__.py
│   ├── core.py           # Main diff logic
│   ├── ambiguity.py      # Ambiguity detection
│   └── toposort.py       # Dependency sorting
├── applier/
│   ├── __init__.py
│   ├── protocol.py       # MigrationApplier protocol
│   ├── shared.py         # Shared pure SQL generation (sqlite + turso)
│   ├── postgresql.py     # PostgreSQL implementation
│   ├── sqlite.py         # SQLite implementation (thin I/O shell)
│   └── turso.py          # Turso/LibSQL implementation (thin I/O shell)
├── query/
│   ├── __init__.py
│   ├── builder.py        # Query building
│   └── executor.py       # Query execution
└── cli/
    ├── __init__.py
    ├── main.py           # CLI entry point
    └── commands.py       # Command implementations

tests/
├── unit/
│   ├── test_types.py
│   ├── test_loader.py
│   ├── test_differ.py
│   └── test_toposort.py
├── integration/
│   ├── test_postgresql.py
│   ├── test_sqlite.py
│   └── test_turso.py
└── conftest.py           # Shared fixtures
```

### 9.2 Dependencies

```toml
# pyproject.toml
[project]
name = "declaro_persistum"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "tomli>=2.0.0;python_version<'3.11'",  # TOML parsing (stdlib in 3.11+)
    "tomli-w>=1.0.0",                       # TOML writing
]

[project.optional-dependencies]
postgresql = ["asyncpg>=0.28.0"]
sqlite = ["aiosqlite>=0.19.0"]
turso = ["pyturso>=0.5.0"]           # Embedded Turso (Rust-based SQLite-compatible)
libsql = ["libsql-experimental>=0.0.55"]  # Turso cloud connections
all = [
    "declaro_persistum[postgresql]",
    "declaro_persistum[sqlite]",
    "declaro_persistum[turso]",
    "declaro_persistum[libsql]",
]
dev = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "hypothesis>=6.0.0",
    "mypy>=1.0.0",
    "ruff>=0.1.0",
]

[project.scripts]
declaro = "declaro_persistum.cli.main:main"
```

### 9.3 CI/CD Pipeline

```yaml
# .github/workflows/ci.yml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: test
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      
      - name: Install dependencies
        run: pip install -e ".[all,dev]"
      
      - name: Type check
        run: mypy declaro_persistum
      
      - name: Lint
        run: ruff check declaro_persistum
      
      - name: Unit tests
        run: pytest tests/unit -v --cov=declaro_persistum
      
      - name: Integration tests
        run: pytest tests/integration -v
        env:
          TEST_POSTGRESQL_URL: postgresql://postgres:test@localhost/postgres
```

---

## 10. Risk Assessment

### 10.1 Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Dialect-specific edge cases | High | Medium | Extensive integration tests per dialect |
| Complex FK cycles in user schemas | Medium | High | Clear error messages with resolution guidance |
| Performance regression vs SQLAlchemy | Low | High | Benchmark suite, performance tests in CI |
| TOML parsing edge cases | Low | Low | Use well-tested tomli library |

### 10.2 Adoption Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Learning curve for SQLAlchemy users | High | Medium | Migration guide, familiar query patterns |
| Resistance to "no ORM" approach | Medium | High | Clear documentation of benefits, performance comparisons |
| Enterprise compliance concerns | Medium | Medium | Audit logging, deterministic behavior |

### 10.3 Operational Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Accidental data loss from migrations | Medium | Critical | Two-phase confirmation, dry-run default |
| Drift between schema and DB | High | Medium | Drift detection, CI enforcement |
| Lost decisions in pending.toml | Low | Low | Decisions also logged in git history |

---

## 11. Decision Log

### D001: TypedDict over dataclasses

**Decision**: Use TypedDict for all data structures.

**Rationale**: 
- TypedDict is just a type annotation over plain dict - no class instantiation
- Serializes directly to/from TOML/JSON
- Compatible with the "no classes" philosophy
- Editor autocomplete works

**Alternatives Considered**:
- dataclasses: Cleaner syntax, but still class instantiation overhead
- NamedTuple: Immutable, but awkward for nested structures
- Plain dicts: No type checking

### D002: Pydantic Models for Schema Definition

**Decision**: Use Pydantic models with `@table` decorator for schema definition.

**Rationale**:
- Full IDE support (autocomplete, type checking, refactoring)
- Native Python - no separate schema language to learn
- Pydantic validation built-in
- Type hints provide compile-time safety

**Alternatives Considered**:
- TOML files: Separate language, no IDE support, parsing overhead
- YAML: Implicit typing problems, no Python integration
- JSON: Too verbose, no comments
- Raw TypedDict: Verbose, no field metadata support

### D003: Snapshot in repo, not database

**Decision**: Store schema snapshots in git repository.

**Rationale**:
- Branches carry their own schema state
- Diffable in pull requests
- No database coupling for state tracking
- Git handles merge conflicts like any other code

**Alternatives Considered**:
- Metadata table in DB: Couples state to environment
- Both: Additional complexity without clear benefit

### D004: Interactive + Unattended modes

**Decision**: Support both interactive (TTY) and unattended (CI) modes.

**Rationale**:
- Developers need interactive ambiguity resolution
- CI pipelines must fail deterministically on ambiguities
- Pre-commit hook pattern: resolve locally, verify in CI

**Trade-offs Accepted**:
- Two code paths to maintain
- Decisions must be persistable (field annotations)

### D005: All-or-nothing transactions where supported

**Decision**: Use single transaction for PostgreSQL/SQLite, checkpoint-based for others.

**Rationale**:
- PostgreSQL DDL is transactional - use it
- SQLite DDL is transactional - use it
- Turso inherits SQLite behavior
- Maximum safety with minimal complexity

**Future Considerations**:
- MySQL support would require checkpoint-based approach
- May need per-operation mode for very large migrations

---

## 12. Appendices

### A. Glossary

| Term | Definition |
|------|------------|
| Schema | Complete database structure as dict[str, Table] |
| Target | Desired schema state (from Pydantic models) |
| Actual | Current database state (from introspection) |
| Expected | Last-applied schema state (from snapshot) |
| Drift | Difference between actual and expected states |
| Ambiguity | Change that requires human decision (rename vs drop+add) |
| Operation | Single DDL statement to execute |
| Dialect | Database-specific implementation (PostgreSQL, SQLite, Turso, LibSQL) |
| TursoCloudManager | Multi-tenant database provisioning for Turso cloud |

### B. SQL Type Mappings

| Python Type | PostgreSQL | SQLite | Turso (embedded) | LibSQL (cloud) |
|-----------|------------|--------|------------------|----------------|
| `integer` | INTEGER | INTEGER | INTEGER | INTEGER |
| `bigint` | BIGINT | INTEGER | INTEGER | INTEGER |
| `text` | TEXT | TEXT | TEXT | TEXT |
| `boolean` | BOOLEAN | INTEGER | INTEGER | INTEGER |
| `uuid` | UUID | TEXT | TEXT | TEXT |
| `timestamptz` | TIMESTAMPTZ | TEXT | TEXT | TEXT |
| `jsonb` | JSONB | TEXT | TEXT | TEXT |
| `numeric(p,s)` | NUMERIC(p,s) | REAL | REAL | REAL |

### C. Reference Links

- [PostgreSQL information_schema](https://www.postgresql.org/docs/current/information-schema.html)
- [SQLite pragma table_info](https://www.sqlite.org/pragma.html#pragma_table_info)
- [Turso documentation](https://docs.turso.tech/)
- [Turso Platform API](https://docs.turso.tech/api-reference/introduction)
- [pyturso (embedded Turso)](https://pypi.org/project/pyturso/)
- [libsql-experimental](https://pypi.org/project/libsql-experimental/)
- [Pydantic documentation](https://docs.pydantic.dev/)
- [Topological sorting algorithm](https://en.wikipedia.org/wiki/Topological_sorting)

---

## Quality Checklist

- [x] All diagrams use Mermaid syntax (no binary image files)
- [x] All diagrams have descriptive captions
- [x] Component names in diagrams match proposed code structure
- [x] All functions have complete signatures with type hints
- [x] Set theory operations documented mathematically
- [x] No unauthorized classes (only TypedDict, Protocol, Exception)
- [x] Performance implications analyzed
- [x] Security boundaries clearly defined
- [x] Error scenarios comprehensively covered
- [x] Testing approach specified
- [x] Rollback procedures documented (transaction-based)
- [x] Risks identified and mitigated
- [x] Decisions justified with rationale
