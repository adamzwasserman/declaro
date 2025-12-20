"""
Unit tests for dialect-aware function translation.

Tests the FUNCTION_TRANSLATIONS mapping and translate_function helper.
"""

import pytest
from typing import Any


class TestFunctionTranslations:
    """Tests for FUNCTION_TRANSLATIONS dict."""

    def test_translations_has_now(self):
        """Translations include NOW function."""
        from declaro_persistum.functions.translations import FUNCTION_TRANSLATIONS

        assert "now" in FUNCTION_TRANSLATIONS
        assert "postgresql" in FUNCTION_TRANSLATIONS["now"]
        assert "sqlite" in FUNCTION_TRANSLATIONS["now"]

    def test_translations_has_uuid(self):
        """Translations include gen_random_uuid function."""
        from declaro_persistum.functions.translations import FUNCTION_TRANSLATIONS

        assert "gen_random_uuid" in FUNCTION_TRANSLATIONS
        assert "postgresql" in FUNCTION_TRANSLATIONS["gen_random_uuid"]
        assert "sqlite" in FUNCTION_TRANSLATIONS["gen_random_uuid"]

    def test_translations_has_string_agg(self):
        """Translations include string_agg function."""
        from declaro_persistum.functions.translations import FUNCTION_TRANSLATIONS

        assert "string_agg" in FUNCTION_TRANSLATIONS
        assert "postgresql" in FUNCTION_TRANSLATIONS["string_agg"]
        assert "sqlite" in FUNCTION_TRANSLATIONS["string_agg"]


class TestTranslateFunction:
    """Tests for translate_function helper."""

    def test_translate_now_postgresql(self):
        """Translate NOW for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("now", "postgresql")
        assert "NOW()" in sql or "CURRENT_TIMESTAMP" in sql

    def test_translate_now_sqlite(self):
        """Translate NOW for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("now", "sqlite")
        assert "datetime('now')" in sql or "CURRENT_TIMESTAMP" in sql

    def test_translate_uuid_postgresql(self):
        """Translate gen_random_uuid for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("gen_random_uuid", "postgresql")
        assert "gen_random_uuid()" in sql

    def test_translate_uuid_sqlite(self):
        """Translate gen_random_uuid for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("gen_random_uuid", "sqlite")
        # SQLite uses hex/randomblob
        assert "randomblob" in sql.lower() or "hex" in sql.lower()

    def test_translate_string_agg_postgresql(self):
        """Translate string_agg for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("string_agg", "postgresql", column="name", separator=", ")
        assert "STRING_AGG" in sql

    def test_translate_string_agg_sqlite(self):
        """Translate string_agg for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("string_agg", "sqlite", column="name", separator=", ")
        assert "GROUP_CONCAT" in sql

    def test_translate_array_agg_postgresql(self):
        """Translate array_agg for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("array_agg", "postgresql", column="id")
        assert "ARRAY_AGG" in sql

    def test_translate_array_agg_sqlite(self):
        """Translate array_agg for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("array_agg", "sqlite", column="id")
        assert "JSON_GROUP_ARRAY" in sql

    def test_translate_extract_year_postgresql(self):
        """Translate extract_year for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("extract_year", "postgresql", column="created_at")
        assert "EXTRACT(YEAR FROM" in sql

    def test_translate_extract_year_sqlite(self):
        """Translate extract_year for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("extract_year", "sqlite", column="created_at")
        assert "strftime('%Y'" in sql


class TestUnknownFunctionHandling:
    """Tests for handling unknown functions."""

    def test_unknown_function_raises_error(self):
        """Unknown function raises KeyError."""
        from declaro_persistum.functions.translations import translate_function

        with pytest.raises(KeyError):
            translate_function("nonexistent_func", "postgresql")

    def test_unknown_dialect_raises_error(self):
        """Unknown dialect raises KeyError."""
        from declaro_persistum.functions.translations import translate_function

        with pytest.raises(KeyError):
            translate_function("now", "mysql")


class TestTranslationWithParameters:
    """Tests for translations with parameters."""

    def test_concat_postgresql(self):
        """Translate concat for PostgreSQL with args."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("concat", "postgresql", args=["first_name", "' '", "last_name"])
        assert "CONCAT" in sql or "||" in sql

    def test_concat_sqlite(self):
        """Translate concat for SQLite with args."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("concat", "sqlite", args=["first_name", "' '", "last_name"])
        assert "||" in sql

    def test_date_add_postgresql(self):
        """Translate date_add for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("date_add", "postgresql", column="created_at", days=7)
        assert "INTERVAL" in sql or "+" in sql

    def test_date_add_sqlite(self):
        """Translate date_add for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("date_add", "sqlite", column="created_at", days=7)
        assert "date" in sql.lower() or "datetime" in sql.lower()


class TestILikeTranslation:
    """Tests for ILIKE translation (PostgreSQL-specific)."""

    def test_ilike_postgresql(self):
        """ILIKE stays as ILIKE for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("ilike", "postgresql", column="name", pattern="%john%")
        assert "ILIKE" in sql

    def test_ilike_sqlite(self):
        """ILIKE becomes LIKE with LOWER for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("ilike", "sqlite", column="name", pattern="%john%")
        # SQLite doesn't have ILIKE, use LOWER(col) LIKE LOWER(pattern)
        assert "LOWER" in sql
        assert "LIKE" in sql


class TestJsonFunctions:
    """Tests for JSON function translations."""

    def test_jsonb_extract_postgresql(self):
        """JSON extraction for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("json_extract", "postgresql", column="data", path="$.name")
        assert "->" in sql or "->>" in sql or "jsonb_extract_path" in sql.lower()

    def test_json_extract_sqlite(self):
        """JSON extraction for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("json_extract", "sqlite", column="data", path="$.name")
        assert "json_extract" in sql.lower()


class TestBooleanLiteralTranslation:
    """Tests for boolean literal translation."""

    def test_true_postgresql(self):
        """TRUE literal for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("bool_true", "postgresql")
        assert "TRUE" in sql or "true" in sql

    def test_true_sqlite(self):
        """TRUE literal for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("bool_true", "sqlite")
        assert "1" in sql

    def test_false_postgresql(self):
        """FALSE literal for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("bool_false", "postgresql")
        assert "FALSE" in sql or "false" in sql

    def test_false_sqlite(self):
        """FALSE literal for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("bool_false", "sqlite")
        assert "0" in sql


class TestCurrentDateFunctions:
    """Tests for current date/time functions."""

    def test_current_date_postgresql(self):
        """CURRENT_DATE for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("current_date", "postgresql")
        assert "CURRENT_DATE" in sql

    def test_current_date_sqlite(self):
        """CURRENT_DATE for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("current_date", "sqlite")
        assert "date('now')" in sql or "CURRENT_DATE" in sql

    def test_current_timestamp_postgresql(self):
        """CURRENT_TIMESTAMP for PostgreSQL."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("current_timestamp", "postgresql")
        assert "CURRENT_TIMESTAMP" in sql or "NOW()" in sql

    def test_current_timestamp_sqlite(self):
        """CURRENT_TIMESTAMP for SQLite."""
        from declaro_persistum.functions.translations import translate_function

        sql = translate_function("current_timestamp", "sqlite")
        assert "datetime('now')" in sql or "CURRENT_TIMESTAMP" in sql
