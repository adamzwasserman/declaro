"""
Schema factories for generating test schemas.
"""


from declaro_persistum.types import Schema

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
