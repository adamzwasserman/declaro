"""Schema validation: what is fatal, what is only a warning, and why.

`validator.py` carried 56 branches and 0% coverage — one of the six modules
holding the Slop Audit L1.19 gap at 49.7% (declaro-xu0).

The whole module is pure: schema dict in, (warnings, errors) out. So every
test is an assertion on a return value, no mock anywhere.

The distinction the module draws is the thing worth pinning down. An
unresolvable reference is an ERROR — the DDL cannot be emitted. An unknown
type or a circular dependency is a WARNING — a custom type may exist, and a
cycle is legal with deferrable constraints. Tests assert which list a
finding lands in, not merely that something was said.
"""

import pytest

from declaro_persistum.exceptions import ValidationError
from declaro_persistum.validator import (
    KNOWN_TYPES,
    _check_circular_dependencies,
    _validate_column,
    _validate_index,
    _validate_reference,
    validate_schema,
    validate_schema_strict,
)


def _t(columns=None, **rest):
    return {"columns": columns or {}, **rest}


USERS = _t({"id": {"type": "integer"}, "name": {"type": "text"}})


class TestKnownTypes:
    def test_the_common_sql_types_are_recognised(self):
        for t in ("integer", "text", "boolean", "numeric", "uuid"):
            assert t in KNOWN_TYPES, f"{t} should not warn"

    def test_types_are_stored_lowercase(self):
        assert all(t == t.lower() for t in KNOWN_TYPES)


class TestColumnTypes:
    def test_a_known_type_produces_nothing(self):
        w, e = _validate_column("users", "id", {"type": "integer"}, {})
        assert (w, e) == ([], [])

    def test_an_unknown_type_warns_and_does_not_error(self):
        w, e = _validate_column("users", "id", {"type": "wat"}, {})
        assert e == [], "an unknown type may be a custom type or enum"
        assert len(w) == 1
        assert "Unknown type 'wat'" in w[0]

    def test_a_type_is_matched_case_insensitively(self):
        w, e = _validate_column("users", "id", {"type": "INTEGER"}, {})
        assert (w, e) == ([], [])

    def test_a_size_specifier_is_stripped_before_matching(self):
        w, e = _validate_column("users", "n", {"type": "varchar(255)"}, {})
        assert w == [], "varchar(255) is varchar"

    def test_whitespace_around_a_size_specifier_is_stripped(self):
        w, _ = _validate_column("users", "n", {"type": "numeric (10, 2)"}, {})
        assert w == []

    def test_any_array_type_is_accepted_without_being_listed(self):
        w, _ = _validate_column("users", "tags", {"type": "customtype[]"}, {})
        assert w == [], "the [] suffix is accepted on its own"

    def test_a_column_with_no_type_defaults_to_text_and_does_not_warn(self):
        w, e = _validate_column("users", "id", {}, {})
        assert (w, e) == ([], [])


class TestColumnWarnings:
    def test_a_nullable_primary_key_warns(self):
        w, e = _validate_column(
            "users", "id", {"type": "integer", "primary_key": True}, {}
        )
        assert e == []
        assert any("should be NOT NULL" in m for m in w)

    def test_a_not_null_primary_key_does_not_warn(self):
        w, _ = _validate_column(
            "users",
            "id",
            {"type": "integer", "primary_key": True, "nullable": False},
            {},
        )
        assert w == []

    def test_a_new_not_null_column_without_a_default_warns(self):
        w, _ = _validate_column(
            "users",
            "email",
            {"type": "text", "is_new": True, "nullable": False},
            {},
        )
        assert any("without default may fail" in m for m in w)

    def test_a_new_not_null_column_with_a_default_does_not_warn(self):
        w, _ = _validate_column(
            "users",
            "email",
            {"type": "text", "is_new": True, "nullable": False, "default": "''"},
            {},
        )
        assert w == []

    def test_a_new_nullable_column_does_not_warn(self):
        w, _ = _validate_column(
            "users", "email", {"type": "text", "is_new": True}, {}
        )
        assert w == []

    def test_an_existing_not_null_column_without_a_default_does_not_warn(self):
        """Only NEW columns can fail against rows that already exist."""
        w, _ = _validate_column(
            "users", "email", {"type": "text", "nullable": False}, {}
        )
        assert w == []


class TestReferences:
    def test_a_valid_reference_returns_none(self):
        assert _validate_reference("posts", "author", "users.id", {"users": USERS}) is None

    def test_a_reference_without_a_dot_is_an_error(self):
        msg = _validate_reference("posts", "author", "users", {"users": USERS})
        assert msg is not None
        assert "Invalid reference format" in msg

    def test_a_reference_to_a_missing_table_is_an_error(self):
        msg = _validate_reference("posts", "author", "ghosts.id", {"users": USERS})
        assert "non-existent table 'ghosts'" in msg

    def test_a_reference_to_a_missing_column_is_an_error(self):
        msg = _validate_reference("posts", "author", "users.nope", {"users": USERS})
        assert "non-existent column 'users.nope'" in msg

    def test_a_dotted_column_name_splits_only_once(self):
        schema = {"users": _t({"a.b": {"type": "text"}})}
        assert _validate_reference("posts", "x", "users.a.b", schema) is None

    def test_a_reference_to_a_table_with_no_columns_key_is_an_error(self):
        msg = _validate_reference("posts", "author", "empty.id", {"empty": {}})
        assert "non-existent column" in msg

    def test_a_bad_reference_reaches_the_errors_list_not_the_warnings(self):
        w, e = _validate_column(
            "posts", "author", {"type": "integer", "references": "ghosts.id"}, {}
        )
        assert w == []
        assert len(e) == 1


class TestIndexes:
    def test_an_index_over_existing_columns_is_clean(self):
        assert _validate_index("users", "ix", {"columns": ["id"]}, USERS["columns"]) == []

    def test_an_index_with_no_columns_is_an_error(self):
        errs = _validate_index("users", "ix", {"columns": []}, USERS["columns"])
        assert len(errs) == 1
        assert "at least one column" in errs[0]

    def test_an_index_with_no_columns_key_is_an_error(self):
        errs = _validate_index("users", "ix", {}, USERS["columns"])
        assert "at least one column" in errs[0]

    def test_an_empty_index_reports_once_and_stops(self):
        """The empty check returns early rather than also walking columns."""
        assert len(_validate_index("users", "ix", {"columns": []}, {})) == 1

    def test_an_index_on_a_missing_column_is_an_error(self):
        errs = _validate_index("users", "ix", {"columns": ["nope"]}, USERS["columns"])
        assert "Column 'nope' does not exist" in errs[0]

    def test_every_missing_index_column_is_reported(self):
        errs = _validate_index(
            "users", "ix", {"columns": ["nope", "also_nope"]}, USERS["columns"]
        )
        assert len(errs) == 2


class TestCircularDependencies:
    def test_an_acyclic_schema_warns_about_nothing(self):
        schema = {
            "users": USERS,
            "posts": _t({"author": {"type": "integer", "references": "users.id"}}),
        }
        assert _check_circular_dependencies(schema) == []

    def test_a_two_table_cycle_warns(self):
        schema = {
            "a": _t({"x": {"type": "integer", "references": "b.id"}}),
            "b": _t({"y": {"type": "integer", "references": "a.id"}}),
        }
        w = _check_circular_dependencies(schema)
        assert len(w) == 1
        assert "Circular foreign key dependency" in w[0]
        assert "deferrable" in w[0]

    def test_a_self_reference_is_not_a_cycle(self):
        """A tree table pointing at its own parent column is ordinary."""
        schema = {"nodes": _t({"parent": {"type": "integer", "references": "nodes.id"}})}
        assert _check_circular_dependencies(schema) == []

    def test_a_cycle_is_reported_once_not_once_per_member(self):
        schema = {
            "a": _t({"x": {"type": "integer", "references": "b.id"}}),
            "b": _t({"y": {"type": "integer", "references": "c.id"}}),
            "c": _t({"z": {"type": "integer", "references": "a.id"}}),
        }
        assert len(_check_circular_dependencies(schema)) == 1

    def test_the_warning_names_the_path_around_the_cycle(self):
        schema = {
            "a": _t({"x": {"type": "integer", "references": "b.id"}}),
            "b": _t({"y": {"type": "integer", "references": "a.id"}}),
        }
        assert "->" in _check_circular_dependencies(schema)[0]

    def test_a_cycle_is_a_warning_never_an_error(self):
        """Deferrable constraints make a cycle legal, so it cannot be fatal.

        Both tables carry the `id` the other points at, so the ONLY finding
        available here is the cycle itself.
        """
        schema = {
            "a": _t({
                "id": {"type": "integer"},
                "x": {"type": "integer", "references": "b.id"},
            }),
            "b": _t({
                "id": {"type": "integer"},
                "y": {"type": "integer", "references": "a.id"},
            }),
        }
        warnings, errors = validate_schema(schema)
        assert errors == []
        assert any("Circular foreign key dependency" in m for m in warnings)

    def test_an_empty_schema_has_no_cycles(self):
        assert _check_circular_dependencies({}) == []


class TestValidateSchema:
    def test_an_empty_schema_is_clean(self):
        assert validate_schema({}) == ([], [])

    def test_a_valid_schema_is_clean(self):
        schema = {
            "users": USERS,
            "posts": _t(
                {"id": {"type": "integer"},
                 "author": {"type": "integer", "references": "users.id"}},
                indexes={"ix_author": {"columns": ["author"]}},
                primary_key=["id"],
            ),
        }
        assert validate_schema(schema) == ([], [])

    def test_a_primary_key_naming_a_missing_column_is_an_error(self):
        schema = {"users": _t({"id": {"type": "integer"}}, primary_key=["nope"])}
        _, errors = validate_schema(schema)
        assert len(errors) == 1
        assert "Primary key column 'nope' does not exist" in errors[0]

    def test_a_table_with_no_columns_key_is_tolerated(self):
        assert validate_schema({"t": {}}) == ([], [])

    def test_index_errors_are_collected_across_tables(self):
        schema = {
            "a": _t({"id": {"type": "integer"}}, indexes={"ix": {"columns": ["no"]}}),
            "b": _t({"id": {"type": "integer"}}, indexes={"ix": {"columns": ["no"]}}),
        }
        _, errors = validate_schema(schema)
        assert len(errors) == 2

    def test_warnings_and_errors_are_kept_apart(self):
        schema = {
            "posts": _t({
                "weird": {"type": "wat"},
                "author": {"type": "integer", "references": "ghosts.id"},
            })
        }
        warnings, errors = validate_schema(schema)
        assert len(warnings) == 1 and "Unknown type" in warnings[0]
        assert len(errors) == 1 and "non-existent table" in errors[0]


class TestValidateSchemaStrict:
    def test_a_clean_schema_returns_none(self):
        assert validate_schema_strict({"users": USERS}) is None

    def test_warnings_alone_do_not_raise(self):
        assert validate_schema_strict({"t": _t({"c": {"type": "wat"}})}) is None

    def test_an_error_raises_validation_error(self):
        schema = {"posts": _t({"a": {"type": "integer", "references": "ghosts.id"}})}
        with pytest.raises(ValidationError):
            validate_schema_strict(schema)

    def test_the_message_counts_the_errors_and_lists_them(self):
        schema = {
            "posts": _t({
                "a": {"type": "integer", "references": "ghosts.id"},
                "b": {"type": "integer", "references": "spooks.id"},
            })
        }
        with pytest.raises(ValidationError, match="2 error") as exc:
            validate_schema_strict(schema)
        assert "ghosts" in str(exc.value) and "spooks" in str(exc.value)
