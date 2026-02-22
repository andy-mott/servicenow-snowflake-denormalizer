#!/usr/bin/env python3
"""Create denormalized Snowflake views of ServiceNow tables.

Resolves reference fields (JSON with sys_id) and choice fields (raw integers)
into human-readable display values via LEFT JOINs.
"""

import argparse
import dataclasses
import getpass
import json
import logging
import os
import re
import sys
from collections import defaultdict

import snowflake.connector
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@dataclasses.dataclass
class Config:
    snowflake_account: str
    snowflake_user: str
    snowflake_warehouse: str
    connector_database: str
    connector_schema: str
    dest_database: str
    dest_schema: str
    # Auth: one of these will be set
    snowflake_password: str = ""
    snowflake_private_key_path: str = ""
    snowflake_private_key_passphrase: str = ""
    auth_method: str = "password"  # "password", "keypair", or "browser"


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config() -> Config:
    load_dotenv()
    always_required = {
        "SNOWFLAKE_ACCOUNT": "snowflake_account",
        "SNOWFLAKE_USER": "snowflake_user",
        "SNOWFLAKE_WAREHOUSE": "snowflake_warehouse",
        "CONNECTOR_DATABASE": "connector_database",
        "CONNECTOR_SCHEMA": "connector_schema",
        "DEST_DATABASE": "dest_database",
        "DEST_SCHEMA": "dest_schema",
    }
    values = {}
    missing = []
    for env_var, field in always_required.items():
        val = os.environ.get(env_var)
        if not val:
            missing.append(env_var)
        else:
            values[field] = val
    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    # Determine auth method
    private_key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "")
    password = os.environ.get("SNOWFLAKE_PASSWORD", "")
    auth_method = os.environ.get("SNOWFLAKE_AUTH_METHOD", "").lower()

    if auth_method == "browser":
        values["auth_method"] = "browser"
    elif private_key_path:
        values["auth_method"] = "keypair"
        values["snowflake_private_key_path"] = private_key_path
        values["snowflake_private_key_passphrase"] = os.environ.get(
            "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", ""
        )
    elif password:
        values["auth_method"] = "password"
        values["snowflake_password"] = password
    else:
        logger.error(
            "No authentication configured. Set one of:\n"
            "  SNOWFLAKE_PRIVATE_KEY_PATH (recommended for automation)\n"
            "  SNOWFLAKE_PASSWORD\n"
            "  SNOWFLAKE_AUTH_METHOD=browser (for interactive use)"
        )
        sys.exit(1)

    return Config(**values)


def _load_private_key(key_path: str, passphrase: str):
    """Load an RSA private key from a PEM file."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_snowflake_connection(config: Config):
    try:
        connect_args = {
            "account": config.snowflake_account,
            "user": config.snowflake_user,
            "warehouse": config.snowflake_warehouse,
        }

        if config.auth_method == "keypair":
            logger.info("Authenticating with key pair")
            connect_args["private_key"] = _load_private_key(
                config.snowflake_private_key_path,
                config.snowflake_private_key_passphrase,
            )
        elif config.auth_method == "browser":
            logger.info("Authenticating via browser SSO")
            connect_args["authenticator"] = "externalbrowser"
        else:
            logger.info("Authenticating with password")
            connect_args["password"] = config.snowflake_password
            # Prompt for TOTP code if MFA is enabled
            passcode = getpass.getpass("Enter MFA code (or press Enter to skip): ").strip()
            if passcode:
                connect_args["passcode"] = passcode

        return snowflake.connector.connect(**connect_args)
    except snowflake.connector.errors.Error as e:
        logger.error("Failed to connect to Snowflake: %s", e)
        sys.exit(1)


def validate_identifier(name: str) -> bool:
    return bool(IDENTIFIER_RE.match(name))


def parse_references_json(json_data) -> list:
    """Parse the JSON response from SHOW_REFERENCES_OF_TABLE into reference dicts.

    Args:
        json_data: A JSON string or dict with a "references" key.

    Returns:
        List of dicts with keys: column_name, referenced_table, referenced_column.
    """
    data = json.loads(json_data) if isinstance(json_data, str) else json_data
    ref_list = data.get("references", [])

    references = []
    for ref in ref_list:
        column_name = ref.get("columnName", "")
        referenced_column = ref.get("referencedColumnName", "")
        referenced_table = ref.get("referencedTableName", "")

        if not all([column_name, referenced_column, referenced_table]):
            logger.warning("Skipping incomplete reference entry: %s", ref)
            continue

        if not all(
            validate_identifier(n)
            for n in [column_name, referenced_column, referenced_table]
        ):
            logger.warning(
                "Skipping reference with invalid identifier: %s -> %s.%s",
                column_name,
                referenced_table,
                referenced_column,
            )
            continue

        references.append(
            {
                "column_name": column_name.upper(),
                "referenced_table": referenced_table.lower(),
                "referenced_column": referenced_column.upper(),
            }
        )
    return references


def get_references(cursor, config: Config, table_name: str) -> list:
    """Call SHOW_REFERENCES_OF_TABLE to discover reference fields.

    The stored procedure returns a single row with a single column containing
    a JSON string like: {"references": [{"columnName": "...", ...}, ...]}
    """
    try:
        cursor.execute(f"USE DATABASE {config.connector_database}")
        cursor.execute(f"USE SCHEMA {config.connector_schema}")
        cursor.execute(
            f"CALL {config.connector_database}.{config.connector_schema}"
            f".SHOW_REFERENCES_OF_TABLE('{table_name}')"
        )
        row = cursor.fetchone()
        if not row:
            return []

        return parse_references_json(row[0])
    except snowflake.connector.errors.ProgrammingError as e:
        logger.error(
            "Failed to get references for table '%s': %s", table_name, e
        )
        return []


def get_choice_fields(cursor, config: Config, table_name: str) -> list:
    """Discover choice fields by finding distinct columns in SYS_CHOICE__VIEW.

    Since sys_dictionary is not always available via the connector, we derive
    choice fields directly from SYS_CHOICE__VIEW — any column that has entries
    there is a choice field.
    """
    try:
        cursor.execute(f"USE DATABASE {config.dest_database}")
        cursor.execute(f"USE SCHEMA {config.dest_schema}")
        cursor.execute(
            f"SELECT DISTINCT ELEMENT "
            f"FROM {config.dest_database}.{config.dest_schema}.SYS_CHOICE__VIEW "
            f"WHERE NAME = '{table_name}'"
        )
        rows = cursor.fetchall()
        fields = []
        for row in rows:
            element = row[0]
            if element and validate_identifier(element):
                fields.append(element.upper())
        return fields
    except snowflake.connector.errors.ProgrammingError as e:
        logger.error(
            "Failed to get choice fields for table '%s': %s", table_name, e
        )
        return []


def check_view_exists(cursor, config: Config, view_name: str) -> bool:
    """Check if a view exists in the destination database/schema."""
    try:
        cursor.execute(
            f"SHOW VIEWS LIKE '{view_name}' IN {config.dest_database}.{config.dest_schema}"
        )
        return cursor.fetchone() is not None
    except snowflake.connector.errors.ProgrammingError:
        return False


def discover_synced_tables(cursor, config: Config) -> list:
    """Discover all synced ServiceNow tables by listing *__VIEW views.

    Excludes *__VIEW_WITH_DELETED and *__VIEW_DENORMALIZED views.
    Returns a sorted list of lowercase table names.
    """
    try:
        cursor.execute(f"USE DATABASE {config.dest_database}")
        cursor.execute(f"USE SCHEMA {config.dest_schema}")
        cursor.execute(
            f"SHOW VIEWS IN {config.dest_database}.{config.dest_schema}"
        )

        tables = []
        view_pattern = re.compile(r"^(.+)__VIEW$", re.IGNORECASE)
        exclude_pattern = re.compile(
            r"__(VIEW_WITH_DELETED|VIEW_DENORMALIZED)$", re.IGNORECASE
        )

        for row in cursor.fetchall():
            view_name = row[1]  # "name" column from SHOW VIEWS
            if exclude_pattern.search(view_name):
                continue
            match = view_pattern.match(view_name)
            if match:
                tables.append(match.group(1).lower())

        return sorted(tables)
    except snowflake.connector.errors.ProgrammingError as e:
        logger.error("Failed to discover synced tables: %s", e)
        return []


def resolve_field_lists(references: list, choice_fields: list) -> tuple:
    """Remove choice fields that overlap with reference fields (reference wins)."""
    ref_columns = {r["column_name"] for r in references}
    filtered_choices = [c for c in choice_fields if c not in ref_columns]
    for c in choice_fields:
        if c in ref_columns:
            logger.debug(
                "Column %s is both reference and choice — using reference", c
            )
    return references, filtered_choices


def generate_aliases(references: list, choice_fields: list) -> tuple:
    """Generate unique SQL aliases for all JOINs."""
    # Group references by referenced table to detect duplicates
    table_counts = defaultdict(list)
    for ref in references:
        table_counts[ref["referenced_table"]].append(ref["column_name"])

    ref_aliases = {}
    for ref in references:
        table = ref["referenced_table"]
        col = ref["column_name"]
        if len(table_counts[table]) > 1:
            # Multiple columns reference same table — use column name for uniqueness
            ref_aliases[col] = f"ref_{col.lower()}"
        else:
            ref_aliases[col] = f"ref_{table}"

    choice_aliases = {}
    for col in choice_fields:
        choice_aliases[col] = f"choice_{col.lower()}"

    return ref_aliases, choice_aliases


def generate_view_sql(
    config: Config,
    table_name: str,
    references: list,
    choice_fields: list,
    ref_aliases: dict,
    choice_aliases: dict,
) -> str:
    """Build the CREATE OR REPLACE VIEW statement."""
    dest = f"{config.dest_database}.{config.dest_schema}"
    table_upper = table_name.upper()
    source_view = f"{dest}.{table_upper}__VIEW"
    target_view = f"{dest}.{table_upper}__VIEW_DENORMALIZED"

    # SELECT columns
    select_parts = ["    t.*"]
    for ref in references:
        alias = ref_aliases[ref["column_name"]]
        select_parts.append(
            f"    {alias}.{ref['referenced_column']} AS {ref['column_name']}_DISPLAY"
        )
    for col in choice_fields:
        alias = choice_aliases[col]
        select_parts.append(f"    {alias}.LABEL AS {col}_DISPLAY")

    select_clause = ",\n".join(select_parts)

    # FROM clause
    from_clause = f"{source_view} t"

    # JOIN clauses
    join_parts = []
    for ref in references:
        alias = ref_aliases[ref["column_name"]]
        ref_view = f"{dest}.{ref['referenced_table'].upper()}__VIEW"
        join_parts.append(
            f"LEFT JOIN {ref_view} {alias}\n"
            f"    ON PARSE_JSON(t.{ref['column_name']}):value::STRING = {alias}.SYS_ID"
        )
    for col in choice_fields:
        alias = choice_aliases[col]
        join_parts.append(
            f"LEFT JOIN {dest}.SYS_CHOICE__VIEW {alias}\n"
            f"    ON {alias}.NAME = '{table_name}'\n"
            f"    AND {alias}.ELEMENT = '{col.lower()}'\n"
            f"    AND {alias}.VALUE = t.{col}"
        )

    joins = "\n".join(join_parts)

    sql = f"CREATE OR REPLACE VIEW {target_view} AS\nSELECT\n{select_clause}\nFROM {from_clause}"
    if joins:
        sql += f"\n{joins}"

    return sql


def process_table(cursor, config: Config, table_name: str, dry_run: bool) -> bool:
    """Process a single table: discover fields, generate SQL, execute."""
    table_name = table_name.lower()
    logger.info("Processing table: %s", table_name)

    if not validate_identifier(table_name):
        logger.error("Invalid table name: %s", table_name)
        return False

    try:
        # 1. Discover reference fields
        references = get_references(cursor, config, table_name)
        logger.info("Found %d reference fields for %s", len(references), table_name)

        # 2. Discover choice fields
        choice_fields = get_choice_fields(cursor, config, table_name)
        logger.info("Found %d choice fields for %s", len(choice_fields), table_name)

        # 3. Resolve overlaps (reference wins)
        references, choice_fields = resolve_field_lists(references, choice_fields)

        # 4. Check which referenced views exist
        filtered_refs = []
        for ref in references:
            view_name = f"{ref['referenced_table'].upper()}__VIEW"
            if check_view_exists(cursor, config, view_name):
                filtered_refs.append(ref)
            else:
                logger.warning(
                    "View %s does not exist — skipping reference for column %s",
                    view_name,
                    ref["column_name"],
                )
        references = filtered_refs

        # 5. Generate aliases
        ref_aliases, choice_aliases = generate_aliases(references, choice_fields)

        # 6. Generate SQL
        sql = generate_view_sql(
            config, table_name, references, choice_fields, ref_aliases, choice_aliases
        )
        logger.info("Generated SQL for %s:\n%s", table_name, sql)

        # 7. Execute or dry-run
        if dry_run:
            logger.info("Dry run — skipping execution for %s", table_name)
            return True

        cursor.execute(f"USE DATABASE {config.dest_database}")
        cursor.execute(f"USE SCHEMA {config.dest_schema}")
        cursor.execute(sql)
        logger.info(
            "Successfully created view %s__VIEW_DENORMALIZED", table_name.upper()
        )
        return True

    except Exception as e:
        logger.error("Failed to process table '%s': %s", table_name, e)
        return False


def main():
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Create denormalized Snowflake views of ServiceNow tables"
    )
    parser.add_argument(
        "--table",
        action="append",
        default=None,
        help="ServiceNow table name to denormalize (can be repeated)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Auto-discover and denormalize all synced ServiceNow tables",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and log SQL without executing",
    )
    args = parser.parse_args()

    if not args.table and not args.all:
        parser.error("Either --table or --all is required")

    config = load_config()
    conn = get_snowflake_connection(config)
    cursor = conn.cursor()

    if args.all:
        tables = discover_synced_tables(cursor, config)
        logger.info(
            "Discovered %d synced tables: %s", len(tables), ", ".join(tables)
        )
        if not tables:
            logger.warning("No synced tables found")
            cursor.close()
            conn.close()
            sys.exit(0)
    else:
        tables = args.table

    successes = 0
    failures = 0
    try:
        for table in tables:
            if process_table(cursor, config, table, args.dry_run):
                successes += 1
            else:
                failures += 1
    finally:
        cursor.close()
        conn.close()

    total = successes + failures
    logger.info("Completed: %d succeeded, %d failed out of %d tables", successes, failures, total)
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
