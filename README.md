# ServiceNow Snowflake Denormalizer

Automatically creates denormalized Snowflake views from ServiceNow tables ingested via the [Snowflake Connector for ServiceNow](https://docs.snowflake.com/en/user-guide/snowflake-connector-servicenow).

## The Problem

The Snowflake Connector for ServiceNow V2 ingests tables and creates flattened views (e.g., `INCIDENT__VIEW`), but:

- **Reference fields** (like `opened_by`, `assigned_to`) contain JSON objects with sys_id GUIDs instead of human-readable names
- **Choice fields** (like `state`, `priority`) store raw integers instead of labels like "New" or "High"

This tool creates `*__VIEW_DENORMALIZED` views that resolve both into human-readable `_DISPLAY` columns via LEFT JOINs.

## Two Deployment Modes

| | Python CLI | Snowflake Native |
|---|---|---|
| **Best for** | Development, testing, one-off runs | Production, automated scheduling |
| **Runs where** | Your machine or CI/CD | Inside Snowflake (stored procedures + task) |
| **Auth** | Password, key pair, or browser SSO | Inherits caller's Snowflake session |
| **Scheduling** | External (cron, Airflow, etc.) | Built-in Snowflake TASK (hourly default) |
| **Setup** | `pip install` + `.env` file | Run `setup.sql` in a Snowflake worksheet |

## Architecture

```
ServiceNow Instance
        │
        ▼
┌──────────────────────────────────────────────────┐
│  Snowflake Connector for ServiceNow V2           │
│  (syncs tables → raw data + flattened __VIEW)    │
└──────────┬───────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────┐
│  Connector DB                                    │
│  └─ PUBLIC                                       │
│     └─ SHOW_REFERENCES_OF_TABLE (proc)           │
├──────────────────────────────────────────────────┤
│  Destination DB                                  │
│  └─ Destination Schema                           │
│     ├─ INCIDENT__VIEW            (from connector)│
│     ├─ SYS_USER__VIEW            (from connector)│
│     ├─ SYS_CHOICE__VIEW          (from connector)│
│     ├─ ...                                       │
│     │                                            │
│     ├─ DENORMALIZE_SERVICENOW_TABLE      (proc)  │  ◄── setup.sql
│     ├─ DENORMALIZE_ALL_SERVICENOW_TABLES (proc)  │  ◄── setup.sql
│     ├─ DENORMALIZE_SERVICENOW_TASK       (task)  │  ◄── setup.sql
│     │                                            │
│     └─ INCIDENT__VIEW_DENORMALIZED       (OUTPUT)│
│        ├─ t.*  (all original columns)            │
│        ├─ OPENED_BY_DISPLAY  (ref → sys_user)    │
│        ├─ PRIORITY_DISPLAY   (choice → label)    │
│        └─ ...                                    │
└──────────────────────────────────────────────────┘
           ▲                           ▲
           │                           │
    Python CLI                  Snowflake Task
  (denormalize.py)            (runs hourly via
                               setup.sql)
```

## Prerequisites

- A Snowflake account with the [Snowflake Connector for ServiceNow V2](https://docs.snowflake.com/en/user-guide/snowflake-connector-servicenow) installed and syncing tables
- Tables already syncing with flattened `__VIEW` views available
- `SYS_CHOICE` table synced via the connector (for choice field resolution)

> **Important:** The ServiceNow user configured in the connector must have the **"Internal Integration User"** checkbox enabled. Without this, `sys_dictionary` may return 0 rows and flattened views will not be created.

## Option A: Snowflake Native (Recommended for Production)

### Setup

1. Open `setup.sql` and edit the configuration variables at the top:

   ```sql
   SET CONNECTOR_DB = 'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW';
   SET CONNECTOR_SCHEMA = 'PUBLIC';
   SET DEST_DB = 'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW_DEST_DB';
   SET DEST_SCHEMA = 'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW_DEST_SCHEMA';
   SET WAREHOUSE_NAME = 'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW_WAREHOUSE';
   ```

2. Run the entire `setup.sql` file in a Snowflake worksheet (Snowsight).

This creates two stored procedures and a scheduled task that runs hourly.

### Usage

```sql
-- Denormalize a single table
CALL DENORMALIZE_SERVICENOW_TABLE(
    'incident',
    'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW', 'PUBLIC',
    'DEST_DB', 'DEST_SCHEMA'
);

-- Denormalize all synced tables
CALL DENORMALIZE_ALL_SERVICENOW_TABLES(
    'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW', 'PUBLIC',
    'DEST_DB', 'DEST_SCHEMA'
);

-- The scheduled task runs automatically every hour.
-- Monitor task history:
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'DENORMALIZE_SERVICENOW_TASK'
ORDER BY SCHEDULED_TIME DESC;

-- Pause/resume the task:
ALTER TASK DENORMALIZE_SERVICENOW_TASK SUSPEND;
ALTER TASK DENORMALIZE_SERVICENOW_TASK RESUME;
```

## Option B: Python CLI (Development & Testing)

### Setup

1. Clone this repo and create a virtual environment:

   ```bash
   git clone https://github.com/andymott/servicenow-snowflake-denormalizer.git
   cd servicenow-snowflake-denormalizer
   python3 -m venv venv
   source venv/bin/activate
   ```

2. Install dependencies:

   ```bash
   pip install -e ".[dev]"
   ```

3. Copy the config template and fill in your Snowflake details:

   ```bash
   cp .env.example .env
   # Edit .env with your values
   ```

### Usage

```bash
# Denormalize a single table
servicenow-denormalize --table incident

# Denormalize multiple tables
servicenow-denormalize --table incident --table cmdb_ci --table change_request

# Auto-discover and denormalize all synced tables
servicenow-denormalize --all

# Preview generated SQL without executing
servicenow-denormalize --table incident --dry-run

# Or run directly with Python:
python denormalize.py --all --dry-run
```

### Authentication

The Python CLI supports three authentication methods (configure in `.env`):

| Method | Best for | Configuration |
|---|---|---|
| **Key pair** | Automation, CI/CD, production | `SNOWFLAKE_PRIVATE_KEY_PATH` + optional `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` |
| **Password** | Simple setups (prompts for MFA if enabled) | `SNOWFLAKE_PASSWORD` |
| **Browser SSO** | Interactive use with IdP-based MFA | `SNOWFLAKE_AUTH_METHOD=browser` |

See `.env.example` for full configuration details.

## Configuration Reference

### Environment Variables (Python CLI)

| Variable | Required | Description |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | Yes | Account identifier (e.g., `abc12345.us-east-2.aws`) |
| `SNOWFLAKE_USER` | Yes | Snowflake username |
| `SNOWFLAKE_WAREHOUSE` | Yes | Warehouse name |
| `CONNECTOR_DATABASE` | Yes | Connector database (default: `SNOWFLAKE_CONNECTOR_FOR_SERVICENOW`) |
| `CONNECTOR_SCHEMA` | Yes | Connector schema (default: `PUBLIC`) |
| `DEST_DATABASE` | Yes | Destination database where synced views live |
| `DEST_SCHEMA` | Yes | Destination schema where synced views live |
| `SNOWFLAKE_PASSWORD` | * | Password (one auth method required) |
| `SNOWFLAKE_PRIVATE_KEY_PATH` | * | Path to RSA private key `.p8` file |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | No | Passphrase for encrypted private key |
| `SNOWFLAKE_AUTH_METHOD` | * | Set to `browser` for SSO auth |

### SQL Variables (setup.sql)

| Variable | Description |
|---|---|
| `CONNECTOR_DB` | Connector database name |
| `CONNECTOR_SCHEMA` | Connector schema (typically `PUBLIC`) |
| `DEST_DB` | Destination database |
| `DEST_SCHEMA` | Destination schema |
| `WAREHOUSE_NAME` | Warehouse for the scheduled task |

## How It Works

For each ServiceNow table:

1. **Discover reference fields** &mdash; Calls `SHOW_REFERENCES_OF_TABLE('<table>')` in the connector database, which returns a JSON object listing columns that reference other tables
2. **Discover choice fields** &mdash; Queries `SYS_CHOICE__VIEW` for columns with choice mappings for that table
3. **Resolve overlaps** &mdash; If a column appears as both a reference and a choice, reference wins
4. **Verify referenced views exist** &mdash; Skips references to tables that aren't synced
5. **Generate aliases** &mdash; Creates unique SQL aliases to handle multiple JOINs to the same table
6. **Build and execute SQL** &mdash; Creates `<TABLE>__VIEW_DENORMALIZED` with LEFT JOINs for all references and choices

### Example Output

For the `incident` table, the generated view might include:

| Original Column | Display Column | Source |
|---|---|---|
| `OPENED_BY` (JSON with sys_id) | `OPENED_BY_DISPLAY` | LEFT JOIN to `SYS_USER__VIEW` |
| `ASSIGNED_TO` (JSON with sys_id) | `ASSIGNED_TO_DISPLAY` | LEFT JOIN to `SYS_USER__VIEW` |
| `PRIORITY` (integer `1`) | `PRIORITY_DISPLAY` (`"Critical"`) | LEFT JOIN to `SYS_CHOICE__VIEW` |
| `STATE` (integer `2`) | `STATE_DISPLAY` (`"In Progress"`) | LEFT JOIN to `SYS_CHOICE__VIEW` |

## Troubleshooting

### Flattened `__VIEW` views are not being created by the connector

The connector requires `sys_dictionary` to have data in order to create flattened views.

- Verify the ServiceNow connector user has **"Internal Integration User"** checked (ServiceNow > User record > scroll to bottom)
- After enabling, restart the connector and re-sync the `sys_dictionary` table
- Check in Snowflake: `SELECT COUNT(*) FROM <dest_db>.<dest_schema>.SYS_DICTIONARY;` should return rows

### `SYS_CHOICE__VIEW` does not exist

The `sys_choice` table must be added to the connector's sync list. In Snowsight, go to **Data Products > Apps > Snowflake Connector for ServiceNow** and add `sys_choice` to the synced tables.

### MFA / TOTP errors with password auth

If your Snowflake account requires MFA, the Python CLI will prompt for a TOTP code. For automated/production use, switch to **key pair authentication** which bypasses MFA.

### Referenced table not synced (warning: "View X does not exist")

The tool skips references to tables that haven't been synced by the connector. This is normal and logged as a warning. To resolve references to those tables, add them to the connector sync list.

### Connector shows 0 rows for `sys_dictionary`

This is usually caused by the ServiceNow user not having the "Internal Integration User" flag. See the first troubleshooting item above.

### Views not appearing after connector sync

After adding tables to the connector, you may need to:
1. Trigger a manual sync from the connector management page
2. Wait for `sys_dictionary` to populate
3. Views are created from the flattened data once `sys_dictionary` has schema information

## Development

```bash
# Run tests
pytest

# Run tests with coverage
pytest --cov=denormalize --cov-report=term-missing

# Run a specific test class
pytest tests/test_denormalize.py::TestGenerateViewSql
```

## License

MIT
