"""Unit tests for denormalize.py.

All tests run offline — no Snowflake connection required.
"""

import json
from unittest.mock import MagicMock

import pytest
import snowflake.connector.errors

from denormalize import (
    Config,
    check_view_exists,
    discover_synced_tables,
    generate_aliases,
    generate_view_sql,
    get_choice_fields,
    get_references,
    parse_references_json,
    resolve_field_lists,
    validate_identifier,
)


def make_config(**overrides):
    defaults = {
        "snowflake_account": "test_account",
        "snowflake_user": "test_user",
        "snowflake_warehouse": "test_wh",
        "connector_database": "CONNECTOR_DB",
        "connector_schema": "PUBLIC",
        "dest_database": "DEST_DB",
        "dest_schema": "DEST_SCHEMA",
    }
    defaults.update(overrides)
    return Config(**defaults)


# =============================================================================
# validate_identifier
# =============================================================================


class TestValidateIdentifier:
    def test_valid_simple(self):
        assert validate_identifier("incident") is True

    def test_valid_with_underscore(self):
        assert validate_identifier("sys_user") is True

    def test_valid_with_numbers(self):
        assert validate_identifier("cmdb_ci_2") is True

    def test_valid_starts_with_underscore(self):
        assert validate_identifier("_private") is True

    def test_invalid_starts_with_number(self):
        assert validate_identifier("2fast") is False

    def test_invalid_has_space(self):
        assert validate_identifier("has space") is False

    def test_invalid_has_dot(self):
        assert validate_identifier("db.schema") is False

    def test_invalid_has_semicolon(self):
        assert validate_identifier("table; DROP TABLE") is False

    def test_empty_string(self):
        assert validate_identifier("") is False


# =============================================================================
# parse_references_json
# =============================================================================


class TestParseReferencesJson:
    def test_basic_references(self):
        json_data = json.dumps(
            {
                "references": [
                    {
                        "columnName": "opened_by",
                        "referencedColumnName": "name",
                        "referencedTableName": "sys_user",
                    },
                    {
                        "columnName": "assignment_group",
                        "referencedColumnName": "name",
                        "referencedTableName": "sys_user_group",
                    },
                ]
            }
        )
        result = parse_references_json(json_data)
        assert len(result) == 2
        assert result[0]["column_name"] == "OPENED_BY"
        assert result[0]["referenced_table"] == "sys_user"
        assert result[0]["referenced_column"] == "NAME"

    def test_empty_references(self):
        result = parse_references_json('{"references": []}')
        assert result == []

    def test_missing_references_key(self):
        result = parse_references_json("{}")
        assert result == []

    def test_incomplete_reference_entry(self):
        json_data = json.dumps(
            {"references": [{"columnName": "opened_by"}]}
        )
        result = parse_references_json(json_data)
        assert result == []

    def test_invalid_identifier_in_reference(self):
        json_data = json.dumps(
            {
                "references": [
                    {
                        "columnName": "opened by",
                        "referencedColumnName": "name",
                        "referencedTableName": "sys_user",
                    }
                ]
            }
        )
        result = parse_references_json(json_data)
        assert result == []

    def test_dict_input_instead_of_string(self):
        data = {
            "references": [
                {
                    "columnName": "caller_id",
                    "referencedColumnName": "name",
                    "referencedTableName": "sys_user",
                }
            ]
        }
        result = parse_references_json(data)
        assert len(result) == 1

    def test_uppercase_normalization(self):
        json_data = json.dumps(
            {
                "references": [
                    {
                        "columnName": "Opened_By",
                        "referencedColumnName": "Name",
                        "referencedTableName": "Sys_User",
                    }
                ]
            }
        )
        result = parse_references_json(json_data)
        assert result[0]["column_name"] == "OPENED_BY"
        assert result[0]["referenced_column"] == "NAME"
        assert result[0]["referenced_table"] == "sys_user"


# =============================================================================
# resolve_field_lists
# =============================================================================


class TestResolveFieldLists:
    def test_no_overlap(self):
        refs = [
            {
                "column_name": "OPENED_BY",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            }
        ]
        choices = ["STATE", "PRIORITY"]
        result_refs, result_choices = resolve_field_lists(refs, choices)
        assert len(result_refs) == 1
        assert result_choices == ["STATE", "PRIORITY"]

    def test_overlap_reference_wins(self):
        refs = [
            {
                "column_name": "PRIORITY",
                "referenced_table": "some_table",
                "referenced_column": "NAME",
            }
        ]
        choices = ["PRIORITY", "STATE"]
        result_refs, result_choices = resolve_field_lists(refs, choices)
        assert len(result_refs) == 1
        assert "PRIORITY" not in result_choices
        assert "STATE" in result_choices

    def test_all_choices_overlap(self):
        refs = [
            {
                "column_name": "STATE",
                "referenced_table": "t1",
                "referenced_column": "NAME",
            },
            {
                "column_name": "PRIORITY",
                "referenced_table": "t2",
                "referenced_column": "NAME",
            },
        ]
        choices = ["STATE", "PRIORITY"]
        _, result_choices = resolve_field_lists(refs, choices)
        assert result_choices == []

    def test_empty_inputs(self):
        result_refs, result_choices = resolve_field_lists([], [])
        assert result_refs == []
        assert result_choices == []


# =============================================================================
# generate_aliases
# =============================================================================


class TestGenerateAliases:
    def test_unique_tables(self):
        refs = [
            {
                "column_name": "OPENED_BY",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            },
            {
                "column_name": "ASSIGNMENT_GROUP",
                "referenced_table": "sys_user_group",
                "referenced_column": "NAME",
            },
        ]
        choices = ["STATE", "PRIORITY"]
        ref_aliases, choice_aliases = generate_aliases(refs, choices)
        assert ref_aliases["OPENED_BY"] == "ref_sys_user"
        assert ref_aliases["ASSIGNMENT_GROUP"] == "ref_sys_user_group"
        assert choice_aliases["STATE"] == "choice_state"
        assert choice_aliases["PRIORITY"] == "choice_priority"

    def test_duplicate_table_references(self):
        refs = [
            {
                "column_name": "OPENED_BY",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            },
            {
                "column_name": "CLOSED_BY",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            },
        ]
        ref_aliases, _ = generate_aliases(refs, [])
        assert ref_aliases["OPENED_BY"] == "ref_opened_by"
        assert ref_aliases["CLOSED_BY"] == "ref_closed_by"

    def test_three_columns_same_table(self):
        refs = [
            {
                "column_name": "OPENED_BY",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            },
            {
                "column_name": "CLOSED_BY",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            },
            {
                "column_name": "ASSIGNED_TO",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            },
        ]
        ref_aliases, _ = generate_aliases(refs, [])
        assert ref_aliases["OPENED_BY"] == "ref_opened_by"
        assert ref_aliases["CLOSED_BY"] == "ref_closed_by"
        assert ref_aliases["ASSIGNED_TO"] == "ref_assigned_to"

    def test_empty_inputs(self):
        ref_aliases, choice_aliases = generate_aliases([], [])
        assert ref_aliases == {}
        assert choice_aliases == {}

    def test_mixed_unique_and_duplicate(self):
        refs = [
            {
                "column_name": "OPENED_BY",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            },
            {
                "column_name": "CLOSED_BY",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            },
            {
                "column_name": "ASSIGNMENT_GROUP",
                "referenced_table": "sys_user_group",
                "referenced_column": "NAME",
            },
        ]
        ref_aliases, _ = generate_aliases(refs, [])
        assert ref_aliases["OPENED_BY"] == "ref_opened_by"
        assert ref_aliases["CLOSED_BY"] == "ref_closed_by"
        assert ref_aliases["ASSIGNMENT_GROUP"] == "ref_sys_user_group"


# =============================================================================
# generate_view_sql
# =============================================================================


class TestGenerateViewSql:
    def test_basic_reference_join(self):
        config = make_config()
        refs = [
            {
                "column_name": "OPENED_BY",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            }
        ]
        ref_aliases = {"OPENED_BY": "ref_sys_user"}
        sql = generate_view_sql(config, "incident", refs, [], ref_aliases, {})

        assert "CREATE OR REPLACE VIEW DEST_DB.DEST_SCHEMA.INCIDENT__VIEW_DENORMALIZED" in sql
        assert "FROM DEST_DB.DEST_SCHEMA.INCIDENT__VIEW t" in sql
        assert "ref_sys_user.NAME AS OPENED_BY_DISPLAY" in sql
        assert "LEFT JOIN DEST_DB.DEST_SCHEMA.SYS_USER__VIEW ref_sys_user" in sql
        assert "PARSE_JSON(t.OPENED_BY):value::STRING = ref_sys_user.SYS_ID" in sql

    def test_basic_choice_join(self):
        config = make_config()
        choice_aliases = {"STATE": "choice_state"}
        sql = generate_view_sql(
            config, "incident", [], ["STATE"], {}, choice_aliases
        )

        assert "choice_state.LABEL AS STATE_DISPLAY" in sql
        assert "LEFT JOIN DEST_DB.DEST_SCHEMA.SYS_CHOICE__VIEW choice_state" in sql
        assert "choice_state.NAME = 'incident'" in sql
        assert "choice_state.ELEMENT = 'state'" in sql
        assert "choice_state.VALUE = t.STATE" in sql

    def test_no_joins(self):
        config = make_config()
        sql = generate_view_sql(config, "incident", [], [], {}, {})
        assert "SELECT\n    t.*\nFROM" in sql
        assert "LEFT JOIN" not in sql

    def test_mixed_references_and_choices(self):
        config = make_config()
        refs = [
            {
                "column_name": "OPENED_BY",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            }
        ]
        ref_aliases = {"OPENED_BY": "ref_sys_user"}
        choice_aliases = {"STATE": "choice_state"}
        sql = generate_view_sql(
            config, "incident", refs, ["STATE"], ref_aliases, choice_aliases
        )

        assert "OPENED_BY_DISPLAY" in sql
        assert "STATE_DISPLAY" in sql
        # Reference join should appear before choice join
        ref_pos = sql.index("ref_sys_user.SYS_ID")
        choice_pos = sql.index("choice_state.NAME")
        assert ref_pos < choice_pos

    def test_sql_starts_with_create(self):
        config = make_config()
        sql = generate_view_sql(config, "incident", [], [], {}, {})
        assert sql.startswith("CREATE OR REPLACE VIEW")

    def test_fully_qualified_names(self):
        config = make_config()
        refs = [
            {
                "column_name": "OPENED_BY",
                "referenced_table": "sys_user",
                "referenced_column": "NAME",
            }
        ]
        ref_aliases = {"OPENED_BY": "ref_sys_user"}
        sql = generate_view_sql(config, "incident", refs, [], ref_aliases, {})

        # Every view reference should be fully qualified
        assert "DEST_DB.DEST_SCHEMA.INCIDENT__VIEW_DENORMALIZED" in sql
        assert "DEST_DB.DEST_SCHEMA.INCIDENT__VIEW t" in sql
        assert "DEST_DB.DEST_SCHEMA.SYS_USER__VIEW" in sql


# =============================================================================
# discover_synced_tables (mocked cursor)
# =============================================================================


class TestDiscoverSyncedTables:
    def test_discovers_basic_views(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("2024-01-01", "INCIDENT__VIEW", "DB", "SCHEMA"),
            ("2024-01-01", "SYS_USER__VIEW", "DB", "SCHEMA"),
            ("2024-01-01", "SYS_CHOICE__VIEW", "DB", "SCHEMA"),
        ]
        config = make_config()
        result = discover_synced_tables(mock_cursor, config)
        assert "incident" in result
        assert "sys_user" in result
        assert "sys_choice" in result

    def test_excludes_with_deleted_views(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("2024-01-01", "INCIDENT__VIEW", "DB", "SCHEMA"),
            ("2024-01-01", "INCIDENT__VIEW_WITH_DELETED", "DB", "SCHEMA"),
        ]
        config = make_config()
        result = discover_synced_tables(mock_cursor, config)
        assert result == ["incident"]

    def test_excludes_denormalized_views(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("2024-01-01", "INCIDENT__VIEW", "DB", "SCHEMA"),
            ("2024-01-01", "INCIDENT__VIEW_DENORMALIZED", "DB", "SCHEMA"),
        ]
        config = make_config()
        result = discover_synced_tables(mock_cursor, config)
        assert result == ["incident"]

    def test_empty_schema(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        config = make_config()
        result = discover_synced_tables(mock_cursor, config)
        assert result == []

    def test_returns_sorted(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("2024-01-01", "SYS_USER__VIEW", "DB", "SCHEMA"),
            ("2024-01-01", "INCIDENT__VIEW", "DB", "SCHEMA"),
            ("2024-01-01", "CHANGE_REQUEST__VIEW", "DB", "SCHEMA"),
        ]
        config = make_config()
        result = discover_synced_tables(mock_cursor, config)
        assert result == ["change_request", "incident", "sys_user"]


# =============================================================================
# get_references (mocked cursor)
# =============================================================================


class TestGetReferences:
    def test_basic_call(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (
            json.dumps(
                {
                    "references": [
                        {
                            "columnName": "opened_by",
                            "referencedColumnName": "name",
                            "referencedTableName": "sys_user",
                        }
                    ]
                }
            ),
        )
        config = make_config()
        result = get_references(mock_cursor, config, "incident")
        assert len(result) == 1
        assert result[0]["column_name"] == "OPENED_BY"

    def test_empty_result(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        config = make_config()
        result = get_references(mock_cursor, config, "incident")
        assert result == []

    def test_programming_error(self):
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = (
            snowflake.connector.errors.ProgrammingError("test error")
        )
        config = make_config()
        result = get_references(mock_cursor, config, "incident")
        assert result == []


# =============================================================================
# get_choice_fields (mocked cursor)
# =============================================================================


class TestGetChoiceFields:
    def test_basic_choices(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("state",), ("priority",)]
        config = make_config()
        result = get_choice_fields(mock_cursor, config, "incident")
        assert "STATE" in result
        assert "PRIORITY" in result

    def test_invalid_element_filtered(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("state",), ("has space",)]
        config = make_config()
        result = get_choice_fields(mock_cursor, config, "incident")
        assert result == ["STATE"]

    def test_programming_error(self):
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = (
            snowflake.connector.errors.ProgrammingError("test error")
        )
        config = make_config()
        result = get_choice_fields(mock_cursor, config, "incident")
        assert result == []


# =============================================================================
# check_view_exists (mocked cursor)
# =============================================================================


class TestCheckViewExists:
    def test_view_exists(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("some_row",)
        config = make_config()
        assert check_view_exists(mock_cursor, config, "SYS_USER__VIEW") is True

    def test_view_not_exists(self):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        config = make_config()
        assert check_view_exists(mock_cursor, config, "SYS_USER__VIEW") is False

    def test_programming_error(self):
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = (
            snowflake.connector.errors.ProgrammingError("test error")
        )
        config = make_config()
        assert check_view_exists(mock_cursor, config, "SYS_USER__VIEW") is False
