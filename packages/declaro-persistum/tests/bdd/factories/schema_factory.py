"""
Schema factories for generating test schemas.
"""

from typing import Any

from declaro_persistum.types import Schema, Table, Column


# =============================================================================
# Pre-built Schemas
# =============================================================================

def simple_todos_schema() -> Schema:
    """Simple todos schema for basic testing."""
    return {
        "todos": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "title": {"type": "text", "nullable": False},
                "completed": {"type": "boolean", "default": "false"},
                "created_at": {"type": "timestamptz", "default": "now()"},
            }
        }
    }


def simple_users_schema() -> Schema:
    """Simple users schema for basic testing."""
    return {
        "users": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "email": {"type": "text", "nullable": False, "unique": True},
                "name": {"type": "text"},
                "status": {"type": "text", "default": "'active'"},
                "age": {"type": "integer"},
                "created_at": {"type": "timestamptz", "default": "now()"},
            },
            "indexes": {
                "users_email_idx": {"columns": ["email"], "unique": True},
            },
        }
    }


def complex_ecommerce_schema() -> Schema:
    """Complex e-commerce schema with relationships."""
    return {
        "users": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "email": {"type": "text", "nullable": False, "unique": True},
                "name": {"type": "text"},
                "status": {"type": "text", "default": "'active'"},
                "created_at": {"type": "timestamptz", "default": "now()"},
            },
        },
        "products": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "name": {"type": "text", "nullable": False},
                "description": {"type": "text"},
                "price": {"type": "numeric(10,2)", "nullable": False},
                "stock": {"type": "integer", "default": "0"},
                "created_at": {"type": "timestamptz", "default": "now()"},
            },
        },
        "orders": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "user_id": {
                    "type": "uuid",
                    "nullable": False,
                    "references": "users.id",
                    "on_delete": "cascade",
                },
                "total": {"type": "numeric(10,2)", "nullable": False},
                "status": {"type": "text", "default": "'pending'"},
                "created_at": {"type": "timestamptz", "default": "now()"},
            },
            "indexes": {
                "orders_user_id_idx": {"columns": ["user_id"]},
                "orders_status_idx": {"columns": ["status"]},
            },
        },
        "order_items": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "order_id": {
                    "type": "uuid",
                    "nullable": False,
                    "references": "orders.id",
                    "on_delete": "cascade",
                },
                "product_id": {
                    "type": "uuid",
                    "nullable": False,
                    "references": "products.id",
                    "on_delete": "restrict",
                },
                "quantity": {"type": "integer", "nullable": False},
                "price": {"type": "numeric(10,2)", "nullable": False},
            },
            "indexes": {
                "order_items_order_id_idx": {"columns": ["order_id"]},
            },
        },
    }


# =============================================================================
# Schema Factory
# =============================================================================

class SchemaFactory:
    """Factory for building custom test schemas."""

    def __init__(self) -> None:
        self.tables: dict[str, Table] = {}

    def add_table(
        self,
        name: str,
        columns: dict[str, Column] | None = None,
        primary_key: list[str] | None = None,
        indexes: dict[str, Any] | None = None,
    ) -> "SchemaFactory":
        """Add a table to the schema."""
        table: Table = {"columns": columns or {}}
        if primary_key:
            table["primary_key"] = primary_key
        if indexes:
            table["indexes"] = indexes
        self.tables[name] = table
        return self

    def add_column(
        self,
        table_name: str,
        column_name: str,
        column_type: str,
        **kwargs: Any,
    ) -> "SchemaFactory":
        """Add a column to an existing table."""
        if table_name not in self.tables:
            self.add_table(table_name)

        column: Column = {"type": column_type, **kwargs}
        self.tables[table_name]["columns"][column_name] = column
        return self

    def add_uuid_pk(self, table_name: str, column_name: str = "id") -> "SchemaFactory":
        """Add a UUID primary key column."""
        return self.add_column(
            table_name,
            column_name,
            "uuid",
            primary_key=True,
            nullable=False,
        )

    def add_serial_pk(self, table_name: str, column_name: str = "id") -> "SchemaFactory":
        """Add a serial (auto-increment) primary key column."""
        return self.add_column(
            table_name,
            column_name,
            "serial",
            primary_key=True,
        )

    def add_foreign_key(
        self,
        table_name: str,
        column_name: str,
        references: str,
        on_delete: str = "cascade",
    ) -> "SchemaFactory":
        """Add a foreign key column."""
        return self.add_column(
            table_name,
            column_name,
            "uuid",
            nullable=False,
            references=references,
            on_delete=on_delete,
        )

    def add_timestamps(self, table_name: str) -> "SchemaFactory":
        """Add created_at and updated_at columns."""
        self.add_column(table_name, "created_at", "timestamptz", default="now()")
        self.add_column(table_name, "updated_at", "timestamptz", default="now()")
        return self

    def add_index(
        self,
        table_name: str,
        index_name: str,
        columns: list[str],
        unique: bool = False,
    ) -> "SchemaFactory":
        """Add an index to a table."""
        if table_name not in self.tables:
            self.add_table(table_name)

        if "indexes" not in self.tables[table_name]:
            self.tables[table_name]["indexes"] = {}

        self.tables[table_name]["indexes"][index_name] = {
            "columns": columns,
            "unique": unique,
        }
        return self

    def build(self) -> Schema:
        """Build and return the schema."""
        return dict(self.tables)

    @classmethod
    def todos(cls) -> Schema:
        """Quick factory method for todos schema."""
        return (
            cls()
            .add_uuid_pk("todos")
            .add_column("todos", "title", "text", nullable=False)
            .add_column("todos", "completed", "boolean", default="false")
            .add_timestamps("todos")
            .build()
        )

    @classmethod
    def users(cls) -> Schema:
        """Quick factory method for users schema."""
        return (
            cls()
            .add_uuid_pk("users")
            .add_column("users", "email", "text", nullable=False, unique=True)
            .add_column("users", "name", "text")
            .add_column("users", "status", "text", default="'active'")
            .add_column("users", "age", "integer")
            .add_timestamps("users")
            .add_index("users", "users_email_idx", ["email"], unique=True)
            .build()
        )

    @classmethod
    def users_and_orders(cls) -> Schema:
        """Quick factory method for users + orders schema."""
        return (
            cls()
            # Users table
            .add_uuid_pk("users")
            .add_column("users", "email", "text", nullable=False, unique=True)
            .add_column("users", "name", "text")
            .add_timestamps("users")
            # Orders table
            .add_uuid_pk("orders")
            .add_foreign_key("orders", "user_id", "users.id")
            .add_column("orders", "total", "numeric(10,2)", nullable=False)
            .add_column("orders", "status", "text", default="'pending'")
            .add_timestamps("orders")
            .add_index("orders", "orders_user_id_idx", ["user_id"])
            .build()
        )


# =============================================================================
# Schema Variations for Edge Case Testing
# =============================================================================

def schema_with_all_types() -> Schema:
    """Schema with all supported column types for comprehensive testing."""
    return {
        "all_types": {
            "columns": {
                # Primary key
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                # Numeric types
                "int_col": {"type": "integer"},
                "bigint_col": {"type": "bigint"},
                "smallint_col": {"type": "smallint"},
                "serial_col": {"type": "serial"},
                "numeric_col": {"type": "numeric(10,2)"},
                "real_col": {"type": "real"},
                "float_col": {"type": "float"},
                # Text types
                "text_col": {"type": "text"},
                "varchar_col": {"type": "varchar(255)"},
                "char_col": {"type": "char(10)"},
                # Boolean
                "bool_col": {"type": "boolean"},
                # Date/Time types
                "date_col": {"type": "date"},
                "time_col": {"type": "time"},
                "timestamp_col": {"type": "timestamp"},
                "timestamptz_col": {"type": "timestamptz"},
                # Binary
                "bytea_col": {"type": "bytea"},
                # JSON types
                "json_col": {"type": "json"},
                "jsonb_col": {"type": "jsonb"},
                # PostgreSQL-specific
                "uuid_col": {"type": "uuid"},
                "inet_col": {"type": "inet"},
                "cidr_col": {"type": "cidr"},
                # Arrays
                "int_array": {"type": "integer[]"},
                "text_array": {"type": "text[]"},
            }
        }
    }


def schema_with_constraints() -> Schema:
    """Schema with various constraints for testing validation."""
    return {
        "constrained_table": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "unique_col": {"type": "text", "unique": True},
                "nullable_col": {"type": "text", "nullable": True},
                "not_null_col": {"type": "text", "nullable": False},
                "default_col": {"type": "text", "default": "'default_value'"},
                "check_col": {"type": "integer", "check": "check_col >= 0 AND check_col <= 100"},
            },
            "constraints": {
                "positive_check": {
                    "type": "check",
                    "expression": "check_col >= 0",
                },
            },
        }
    }


def schema_with_composite_pk() -> Schema:
    """Schema with composite primary key."""
    return {
        "composite_pk_table": {
            "columns": {
                "tenant_id": {"type": "uuid", "nullable": False},
                "entity_id": {"type": "uuid", "nullable": False},
                "data": {"type": "jsonb"},
                "created_at": {"type": "timestamptz", "default": "now()"},
            },
            "primary_key": ["tenant_id", "entity_id"],
        }
    }
