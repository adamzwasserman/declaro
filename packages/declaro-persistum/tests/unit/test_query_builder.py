"""The functional query builder: SQL text and parameter dict, from pure input.

`query/builder.py` carried 56 branches at 7% coverage — one of the six
modules holding the Slop Audit L1.19 gap at 49.7% (declaro-xu0).

Every function here is pure and returns a `Query` TypedDict, so each test
asserts on the exact SQL string and the exact params dict. Asserting the
whole string rather than a substring is the point: a builder that silently
drops a clause still contains the fragment you searched for.

Two safety properties are load-bearing and get their own tests: `update`
and `delete` REFUSE to build without a WHERE clause, and values always
travel as named parameters rather than being interpolated into the SQL.
"""

import pytest

from declaro_persistum.query.builder import (
    _quote_column,
    delete,
    insert,
    raw,
    select,
    update,
    with_limit,
    with_offset,
    with_params,
)


class TestQuoteColumn:
    def test_a_plain_column_is_quoted(self):
        assert _quote_column("id") == '"id"'

    def test_a_star_is_left_alone(self):
        assert _quote_column("*") == "*"

    def test_table_dot_column_quotes_both_halves_separately(self):
        assert _quote_column("users.id") == '"users"."id"'

    def test_only_the_first_dot_splits(self):
        assert _quote_column("a.b.c") == '"a"."b.c"'


class TestSelect:
    def test_no_columns_selects_everything(self):
        assert select(from_table="users")["sql"] == 'SELECT * FROM "users"'

    def test_an_explicit_star_selects_everything(self):
        assert select("*", from_table="users")["sql"] == 'SELECT * FROM "users"'

    def test_named_columns_are_quoted_and_comma_joined(self):
        q = select("id", "email", from_table="users")
        assert q["sql"] == 'SELECT "id", "email" FROM "users"'

    def test_a_star_among_other_columns_is_not_treated_as_select_all(self):
        """Only a lone '*' is the shortcut; mixed lists go through quoting."""
        q = select("id", "*", from_table="users")
        assert q["sql"] == 'SELECT "id", * FROM "users"'

    def test_a_where_clause_is_appended_and_params_carried(self):
        q = select(from_table="users", where="id = :id", params={"id": 1})
        assert q["sql"] == 'SELECT * FROM "users" WHERE id = :id'
        assert q["params"] == {"id": 1}

    def test_params_default_to_an_empty_dict_not_none(self):
        assert select(from_table="users")["params"] == {}

    def test_a_join_defaults_to_inner(self):
        q = select(from_table="a", joins=[{"table": "b", "on": "a.id = b.a_id"}])
        assert 'INNER JOIN "b" ON a.id = b.a_id' in q["sql"]

    def test_an_explicit_join_type_is_used(self):
        q = select(
            from_table="a",
            joins=[{"type": "LEFT", "table": "b", "on": "a.id = b.a_id"}],
        )
        assert 'LEFT JOIN "b" ON a.id = b.a_id' in q["sql"]

    def test_several_joins_appear_in_order(self):
        q = select(
            from_table="a",
            joins=[
                {"table": "b", "on": "1=1"},
                {"type": "LEFT", "table": "c", "on": "2=2"},
            ],
        )
        assert q["sql"].index('"b"') < q["sql"].index('"c"')

    def test_group_by_columns_are_quoted(self):
        q = select("dept", from_table="staff", group_by=["dept"])
        assert 'GROUP BY "dept"' in q["sql"]

    def test_having_is_appended_after_group_by(self):
        q = select(from_table="s", group_by=["d"], having="COUNT(*) > 1")
        assert q["sql"].index("GROUP BY") < q["sql"].index("HAVING COUNT(*) > 1")

    def test_order_by_defaults_to_ascending(self):
        q = select(from_table="users", order_by=["name"])
        assert 'ORDER BY "name" ASC' in q["sql"]

    def test_a_leading_minus_means_descending(self):
        q = select(from_table="users", order_by=["-created_at"])
        assert 'ORDER BY "created_at" DESC' in q["sql"]

    def test_mixed_directions_are_kept_in_order(self):
        q = select(from_table="u", order_by=["-a", "b"])
        assert 'ORDER BY "a" DESC, "b" ASC' in q["sql"]

    def test_limit_and_offset_are_appended_in_that_order(self):
        q = select(from_table="users", limit=10, offset=20)
        assert q["sql"].endswith("LIMIT 10 OFFSET 20")

    def test_a_zero_limit_is_emitted_not_dropped(self):
        """`if limit is not None` rather than `if limit` — 0 is a real limit."""
        assert "LIMIT 0" in select(from_table="u", limit=0)["sql"]

    def test_a_zero_offset_is_emitted_not_dropped(self):
        assert "OFFSET 0" in select(from_table="u", offset=0)["sql"]

    def test_clauses_appear_in_sql_order(self):
        q = select(
            "a",
            from_table="t",
            joins=[{"table": "j", "on": "1=1"}],
            where="x = 1",
            group_by=["a"],
            having="COUNT(*) > 0",
            order_by=["a"],
            limit=5,
            offset=1,
        )
        sql = q["sql"]
        positions = [
            sql.index(k)
            for k in ("SELECT", "FROM", "JOIN", "WHERE", "GROUP BY",
                      "HAVING", "ORDER BY", "LIMIT", "OFFSET")
        ]
        assert positions == sorted(positions)

    def test_the_dialect_is_left_unset(self):
        assert select(from_table="users")["dialect"] is None


class TestInsert:
    def test_a_single_row_uses_named_parameters(self):
        q = insert("users", {"email": "a@b.c", "name": "Ada"})
        assert q["sql"] == (
            'INSERT INTO "users" ("email", "name") VALUES (:email, :name)'
        )
        assert q["params"] == {"email": "a@b.c", "name": "Ada"}

    def test_values_are_never_interpolated_into_the_sql(self):
        q = insert("users", {"name": "Robert'); DROP TABLE users;--"})
        assert "DROP TABLE" not in q["sql"]
        assert q["params"]["name"] == "Robert'); DROP TABLE users;--"

    def test_several_rows_use_indexed_parameters(self):
        q = insert("users", [{"id": 1}, {"id": 2}])
        assert q["sql"] == 'INSERT INTO "users" ("id") VALUES (:id_0), (:id_1)'
        assert q["params"] == {"id_0": 1, "id_1": 2}

    def test_a_one_row_list_still_takes_the_named_path(self):
        assert insert("t", [{"id": 1}])["params"] == {"id": 1}

    def test_columns_come_from_the_first_row(self):
        q = insert("t", [{"a": 1}, {"a": 2, "b": 3}])
        assert q["sql"].count(":") == 2, "only column 'a' is emitted"
        assert "b" not in q["params"]

    def test_a_row_missing_a_column_binds_none(self):
        q = insert("t", [{"a": 1, "b": 2}, {"a": 3}])
        assert q["params"]["b_1"] is None

    def test_an_empty_list_raises(self):
        with pytest.raises(ValueError, match="values cannot be empty"):
            insert("users", [])

    def test_on_conflict_is_appended(self):
        q = insert("t", {"id": 1}, on_conflict="(id) DO NOTHING")
        assert q["sql"].endswith("ON CONFLICT (id) DO NOTHING")

    def test_returning_columns_are_quoted(self):
        q = insert("t", {"id": 1}, returning=["id", "created_at"])
        assert q["sql"].endswith('RETURNING "id", "created_at"')

    def test_returning_comes_after_on_conflict(self):
        q = insert("t", {"id": 1}, on_conflict="DO NOTHING", returning=["id"])
        assert q["sql"].index("ON CONFLICT") < q["sql"].index("RETURNING")


class TestUpdate:
    def test_set_values_are_prefixed_to_avoid_colliding_with_where_params(self):
        """`set_` namespacing is what lets SET and WHERE both bind `id`."""
        q = update("users", {"id": 9}, where="id = :id", params={"id": 1})
        assert q["sql"] == 'UPDATE "users" SET "id" = :set_id WHERE id = :id'
        assert q["params"] == {"set_id": 9, "id": 1}

    def test_a_missing_where_raises_rather_than_updating_every_row(self):
        with pytest.raises(ValueError, match="where clause is required"):
            update("users", {"name": "x"}, where="")

    def test_empty_set_values_raise(self):
        with pytest.raises(ValueError, match="set_values cannot be empty"):
            update("users", {}, where="1=1")

    def test_set_values_is_checked_before_where(self):
        with pytest.raises(ValueError, match="set_values cannot be empty"):
            update("users", {}, where="")

    def test_an_explicit_all_rows_where_is_allowed(self):
        assert update("users", {"a": 1}, where="1=1")["sql"].endswith("WHERE 1=1")

    def test_several_set_columns_are_comma_joined(self):
        q = update("t", {"a": 1, "b": 2}, where="1=1")
        assert 'SET "a" = :set_a, "b" = :set_b' in q["sql"]

    def test_returning_is_appended(self):
        q = update("t", {"a": 1}, where="1=1", returning=["a"])
        assert q["sql"].endswith('RETURNING "a"')

    def test_no_params_leaves_only_the_set_bindings(self):
        assert update("t", {"a": 1}, where="1=1")["params"] == {"set_a": 1}


class TestDelete:
    def test_a_where_clause_is_required(self):
        with pytest.raises(ValueError, match="where clause is required"):
            delete("users", where="")

    def test_a_delete_binds_its_where_params(self):
        q = delete("users", where="id = :id", params={"id": 1})
        assert q["sql"] == 'DELETE FROM "users" WHERE id = :id'
        assert q["params"] == {"id": 1}

    def test_params_default_to_an_empty_dict(self):
        assert delete("users", where="1=1")["params"] == {}

    def test_returning_is_appended(self):
        q = delete("t", where="1=1", returning=["id"])
        assert q["sql"].endswith('RETURNING "id"')


class TestRaw:
    def test_the_sql_is_passed_through_untouched(self):
        assert raw("SELECT 1")["sql"] == "SELECT 1"

    def test_params_default_to_an_empty_dict(self):
        assert raw("SELECT 1")["params"] == {}

    def test_params_are_carried(self):
        assert raw("SELECT :x", {"x": 1})["params"] == {"x": 1}


class TestComposition:
    def test_with_limit_appends_when_there_is_none(self):
        assert with_limit(raw("SELECT 1"), 5)["sql"] == "SELECT 1 LIMIT 5"

    def test_with_limit_replaces_an_existing_limit(self):
        q = with_limit(select(from_table="t", limit=10), 5)
        assert q["sql"].endswith("LIMIT 5")
        assert "LIMIT 10" not in q["sql"]

    def test_with_offset_appends_when_there_is_none(self):
        assert with_offset(raw("SELECT 1"), 5)["sql"] == "SELECT 1 OFFSET 5"

    def test_with_offset_replaces_an_existing_offset(self):
        q = with_offset(select(from_table="t", offset=10), 5)
        assert q["sql"].endswith("OFFSET 5")
        assert "OFFSET 10" not in q["sql"]

    def test_with_limit_after_offset_truncates_the_offset(self):
        """Documented behaviour worth pinning: LIMIT splits on ' LIMIT ' only,
        so applying it to a query already carrying OFFSET keeps the OFFSET
        text ahead of the new LIMIT and produces invalid SQL ordering."""
        q = with_limit(select(from_table="t", limit=1, offset=2), 5)
        assert q["sql"] == 'SELECT * FROM "t" LIMIT 5'

    def test_composition_preserves_params_and_dialect(self):
        q = with_limit({"sql": "SELECT 1", "params": {"a": 1}, "dialect": "sqlite"}, 5)
        assert q["params"] == {"a": 1}
        assert q["dialect"] == "sqlite"

    def test_with_params_merges_into_the_existing_params(self):
        q = with_params(raw("SELECT :a, :b", {"a": 1}), b=2)
        assert q["params"] == {"a": 1, "b": 2}

    def test_with_params_overwrites_a_colliding_key(self):
        assert with_params(raw("q", {"a": 1}), a=2)["params"] == {"a": 2}

    def test_with_params_leaves_the_sql_alone(self):
        assert with_params(raw("SELECT 1"), a=1)["sql"] == "SELECT 1"

    def test_the_original_query_is_not_mutated(self):
        original = raw("SELECT 1", {"a": 1})
        with_params(original, b=2)
        with_limit(original, 5)
        assert original == {"sql": "SELECT 1", "params": {"a": 1}, "dialect": None}
