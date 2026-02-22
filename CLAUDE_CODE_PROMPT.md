I need you to build a Python tool that connects to Snowflake and automatically creates denormalized views of ServiceNow tables that were ingested via the native Snowflake Connector for ServiceNow.

## Context

The Snowflake Connector for ServiceNow ingests tables and creates flattened views (e.g., `incident__view`), but reference fields contain JSON objects like `{"link": "...", "value": "<sys_id>"}` instead of human-readable values, and choice fields store raw integers instead of labels. I want to automate the creation of views that resolve both.

## Architecture

- Stored procedures like `SHOW_REFERENCES_OF_TABLE` live in database `SNOWFLAKE_CONNECTOR_FOR_SERVICENOW`, schema `PUBLIC`
- Data tables and views live in a destination database/schema (configurable, defaults to `SNOWFLAKE_CONNECTOR_FOR_SERVICENOW_DEST_DB.SNOWFLAKE_CONNECTOR_FOR_SERVICENOW_DEST_SCHEMA`)
- The connector creates `<table>__view` for each synced table
- Configuration is via `.env` file — see `.env.example` for the template

## What the tool should do

1. Accept one or more table names as input (e.g., `python denormalize.py --table incident`)
2. Connect to Snowflake using credentials from `.env`
3. Switch to the connector database and call `SHOW_REFERENCES_OF_TABLE('<table_name>')` to discover reference fields — each returns `columnName`, `referencedColumnName`, and `referencedTableName`
4. Switch to the destination database and query `SYS_DICTIONARY__VIEW` to identify choice fields for that table (where the `CHOICE` column is not null/empty, filtering by `NAME = '<table_name>'` and using `ELEMENT` for the column name)
5. Generate a `CREATE OR REPLACE VIEW <table_name>__view_denormalized` statement that:
   - Includes all original columns from `<table_name>__view`
   - For each **reference field**: LEFT JOINs to `<referencedTableName>__view` using `PARSE_JSON(<table>__view.<column>):value = <ref_table>__view.SYS_ID` and selects the `referencedColumnName` aliased as `<column>_display`
   - For each **choice field**: LEFT JOINs to `SYS_CHOICE__VIEW` matching on `NAME = '<table_name>'` AND `ELEMENT = '<column_name>'` AND `VALUE = <table>__view.<column>`, selecting `LABEL` aliased as `<column>_display`
6. Execute the CREATE VIEW statement in the destination database/schema

## Key technical details

- Reference field values in the flattened views are Snowflake VARIANT type containing JSON like: `{"link": "https://...", "value": "6816f79cc0a8016401c5a33be04be441"}`
- The JOIN pattern for references: `PARSE_JSON(table__view.column):value = ref_table__view.SYS_ID`
- The JOIN pattern for choices: `SYS_CHOICE__VIEW.NAME = 'table_name' AND SYS_CHOICE__VIEW.ELEMENT = 'column_name' AND SYS_CHOICE__VIEW.VALUE = table__view.column`
- Column names in views are UPPERCASE
- If a column is both a reference and a choice field, reference should take priority

## Edge cases to handle

- Referenced tables that haven't been synced yet (the `__view` won't exist) — skip with a warning
- Duplicate JOIN alias conflicts (multiple columns might reference the same table, e.g., `opened_by` and `closed_by` both reference `sys_user`)
- Choice fields where CHOICE column has value 0 vs null — test both to determine which indicates "not a choice field"

## Requirements

- Use `snowflake-connector-python` and `python-dotenv`
- Log the generated SQL to console before executing
- Include a `--dry-run` flag that prints the SQL without executing
- Structure: `denormalize.py` as the main entry point

Please review the existing README.md and project files, then build `denormalize.py`.
