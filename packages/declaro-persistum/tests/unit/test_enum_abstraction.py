"""Literal types become lookup tables with FK constraints, not CHECK.

`abstractions/enums.py` sat at 18% with 12 branches — part of the Slop
Audit L1.19 gap (declaro-xu0). Every function is pure: names, SQL strings
and schema dicts out of plain input.

The design this pins down: a `Literal[...]` column does NOT become a CHECK
constraint, because CHECK is not portable across the four backends. It
becomes a `_dp_enum_{table}_{column}` lookup table plus a foreign key, so
the constraint is enforced by machinery every backend has.

Values reach SQL by string interpolation here, not as parameters — these
are DDL and seed statements built ahead of execution. `_escape_sql` is
therefore load-bearing and gets its own quote-injection tests.
"""

from declaro_persistum.abstractions.enums import (
    ENUM_TABLE_PREFIX,
    _escape_sql,
    add_enum_value_sql,
    create_enum_table_sql,
    diff_enum_values,
    drop_enum_table_sql,
    enum_table_name,
    expand_schema_enums,
    generate_enum_table_schema,
    get_enum_fk_reference,
    is_enum_table,
    remove_enum_value_sql,
    transform_column_for_enum,
)


class TestNaming:
    def test_the_lookup_name_joins_prefix_table_and_column(self):
        assert enum_table_name("orders", "status") == "_dp_enum_orders_status"

    def test_a_generated_name_is_recognised_as_an_enum_table(self):
        assert is_enum_table(enum_table_name("orders", "status"))

    def test_an_ordinary_table_is_not_an_enum_table(self):
        assert not is_enum_table("orders")

    def test_the_prefix_must_be_at_the_start(self):
        assert not is_enum_table(f"orders{ENUM_TABLE_PREFIX}status")

    def test_the_fk_reference_points_at_the_value_column(self):
        assert get_enum_fk_reference("orders", "status") == (
            "_dp_enum_orders_status.value"
        )


class TestEscaping:
    def test_a_plain_string_is_unchanged(self):
        assert _escape_sql("shipped") == "shipped"

    def test_a_single_quote_is_doubled(self):
        assert _escape_sql("O'Brien") == "O''Brien"

    def test_every_quote_is_doubled(self):
        assert _escape_sql("'a'b'") == "''a''b''"

    def test_a_quote_in_a_value_cannot_terminate_the_insert(self):
        sql = add_enum_value_sql("t", "c", "'); DROP TABLE t;--")[0]
        assert "''); DROP TABLE t;--" in sql


class TestGeneratedSchema:
    def test_the_lookup_table_is_keyed_on_a_not_null_value_column(self):
        s = generate_enum_table_schema("orders", "status", ["a"])
        col = s["_dp_enum_orders_status"]["columns"]["value"]
        assert col == {"type": "text", "primary_key": True, "nullable": False}

    def test_the_values_are_kept_as_metadata_for_migration_detection(self):
        s = generate_enum_table_schema("orders", "status", ["a", "b"])
        assert s["_dp_enum_orders_status"]["_enum_values"] == ["a", "b"]

    def test_the_owning_table_and_column_are_recorded(self):
        s = generate_enum_table_schema("orders", "status", [])
        assert s["_dp_enum_orders_status"]["_enum_for"] == {
            "table": "orders",
            "column": "status",
        }


class TestSqlGeneration:
    def test_creating_an_enum_emits_a_table_then_its_values(self):
        stmts = create_enum_table_sql("orders", "status", ["pending", "shipped"])
        assert len(stmts) == 2
        assert stmts[0].startswith('CREATE TABLE "_dp_enum_orders_status"')
        assert stmts[1] == (
            'INSERT INTO "_dp_enum_orders_status" (value) '
            "VALUES ('pending'), ('shipped')"
        )

    def test_an_enum_with_no_values_emits_only_the_table(self):
        """No INSERT with an empty VALUES list, which is a syntax error."""
        stmts = create_enum_table_sql("orders", "status", [])
        assert len(stmts) == 1
        assert "INSERT" not in stmts[0]

    def test_dropping_is_idempotent(self):
        assert drop_enum_table_sql("orders", "status") == [
            'DROP TABLE IF EXISTS "_dp_enum_orders_status"'
        ]

    def test_adding_a_value_inserts_one_row(self):
        assert add_enum_value_sql("orders", "status", "returned") == [
            'INSERT INTO "_dp_enum_orders_status" (value) VALUES (\'returned\')'
        ]

    def test_removing_a_value_deletes_by_value(self):
        assert remove_enum_value_sql("orders", "status", "returned") == [
            'DELETE FROM "_dp_enum_orders_status" WHERE value = \'returned\''
        ]


class TestTransformColumn:
    def test_the_literal_marker_is_replaced_by_a_foreign_key(self):
        out = transform_column_for_enum(
            {"type": "text", "literal_values": ["a", "b"]}, "orders", "status"
        )
        assert "literal_values" not in out
        assert out["references"] == "_dp_enum_orders_status.value"

    def test_a_check_constraint_is_removed_in_favour_of_the_fk(self):
        """CHECK is what this abstraction exists to avoid."""
        out = transform_column_for_enum(
            {"literal_values": ["a"], "check": "status IN ('a')"}, "o", "s"
        )
        assert "check" not in out

    def test_a_column_with_no_literal_values_gains_no_reference(self):
        out = transform_column_for_enum({"type": "text"}, "orders", "status")
        assert out == {"type": "text"}

    def test_an_empty_literal_list_is_not_treated_as_an_enum(self):
        out = transform_column_for_enum(
            {"type": "text", "literal_values": []}, "o", "s"
        )
        assert "references" not in out
        assert "literal_values" not in out, "the marker is stripped either way"

    def test_other_attributes_survive(self):
        out = transform_column_for_enum(
            {"type": "text", "nullable": False, "literal_values": ["a"]}, "o", "s"
        )
        assert out["nullable"] is False

    def test_the_input_column_is_not_mutated(self):
        original = {"type": "text", "literal_values": ["a"]}
        transform_column_for_enum(original, "o", "s")
        assert original["literal_values"] == ["a"]


class TestExpandSchemaEnums:
    def test_a_schema_with_no_enums_is_returned_intact(self):
        schema = {"orders": {"columns": {"id": {"type": "integer"}}}}
        assert expand_schema_enums(schema) == schema

    def test_a_literal_column_produces_a_lookup_table(self):
        schema = {"orders": {"columns": {"status": {"literal_values": ["a", "b"]}}}}
        out = expand_schema_enums(schema)
        assert "_dp_enum_orders_status" in out

    def test_the_column_is_rewritten_to_reference_the_lookup(self):
        schema = {"orders": {"columns": {"status": {"literal_values": ["a"]}}}}
        out = expand_schema_enums(schema)
        assert out["orders"]["columns"]["status"]["references"] == (
            "_dp_enum_orders_status.value"
        )

    def test_lookup_tables_are_ordered_before_the_tables_that_reference_them(self):
        """A FK cannot be created before its parent exists."""
        schema = {"orders": {"columns": {"status": {"literal_values": ["a"]}}}}
        assert list(expand_schema_enums(schema)) == [
            "_dp_enum_orders_status",
            "orders",
        ]

    def test_an_existing_enum_table_is_passed_through_untouched(self):
        schema = {"_dp_enum_orders_status": {"columns": {"value": {}}}}
        assert expand_schema_enums(schema) == schema

    def test_non_enum_columns_are_left_alone(self):
        schema = {
            "orders": {
                "columns": {
                    "id": {"type": "integer"},
                    "status": {"literal_values": ["a"]},
                }
            }
        }
        out = expand_schema_enums(schema)
        assert out["orders"]["columns"]["id"] == {"type": "integer"}

    def test_two_enum_columns_produce_two_lookup_tables(self):
        schema = {
            "orders": {
                "columns": {
                    "status": {"literal_values": ["a"]},
                    "priority": {"literal_values": ["hi"]},
                }
            }
        }
        out = expand_schema_enums(schema)
        assert "_dp_enum_orders_status" in out
        assert "_dp_enum_orders_priority" in out

    def test_table_level_keys_survive_expansion(self):
        schema = {
            "orders": {
                "columns": {"status": {"literal_values": ["a"]}},
                "primary_key": ["id"],
            }
        }
        assert expand_schema_enums(schema)["orders"]["primary_key"] == ["id"]

    def test_a_table_with_no_columns_key_is_tolerated(self):
        assert expand_schema_enums({"t": {}})["t"]["columns"] == {}


class TestDiffEnumValues:
    def test_adding_a_value_is_reported_as_an_addition(self):
        add, remove = diff_enum_values(["a"], ["a", "b"])
        assert add == ["b"] and remove == []

    def test_removing_a_value_is_reported_as_a_removal(self):
        add, remove = diff_enum_values(["a", "b"], ["a"])
        assert add == [] and remove == ["b"]

    def test_an_unchanged_list_produces_nothing(self):
        assert diff_enum_values(["a", "b"], ["b", "a"]) == ([], [])

    def test_none_on_the_left_means_everything_is_new(self):
        add, remove = diff_enum_values(None, ["a"])
        assert add == ["a"] and remove == []

    def test_none_on_the_right_means_everything_goes(self):
        add, remove = diff_enum_values(["a"], None)
        assert add == [] and remove == ["a"]

    def test_two_nones_produce_nothing(self):
        assert diff_enum_values(None, None) == ([], [])

    def test_a_replacement_is_one_addition_and_one_removal(self):
        add, remove = diff_enum_values(["a"], ["b"])
        assert add == ["b"] and remove == ["a"]
