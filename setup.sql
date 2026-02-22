-- =============================================================================
-- ServiceNow Snowflake Denormalizer - Setup Script
--
-- Creates stored procedures and a scheduled task to automatically generate
-- denormalized views of ServiceNow tables synced via the Snowflake Connector.
--
-- BEFORE RUNNING: Edit the configuration variables below to match your
-- Snowflake environment, then execute this entire file in a Snowflake worksheet.
-- =============================================================================

-- Configuration (EDIT THESE VALUES)
SET CONNECTOR_DB = 'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW';
SET CONNECTOR_SCHEMA = 'PUBLIC';
SET DEST_DB = 'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW_DEST_DB';
SET DEST_SCHEMA = 'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW_DEST_SCHEMA';
SET WAREHOUSE_NAME = 'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW_WAREHOUSE';

-- Switch to destination database/schema
USE DATABASE IDENTIFIER($DEST_DB);
USE SCHEMA IDENTIFIER($DEST_SCHEMA);
USE WAREHOUSE IDENTIFIER($WAREHOUSE_NAME);

-- =============================================================================
-- Procedure 1: DENORMALIZE_SERVICENOW_TABLE
--
-- Processes a single ServiceNow table and creates a denormalized view with
-- _DISPLAY columns for all reference and choice fields.
--
-- Usage:
--   CALL DENORMALIZE_SERVICENOW_TABLE(
--       'incident',
--       'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW', 'PUBLIC',
--       'DEST_DB', 'DEST_SCHEMA'
--   );
-- =============================================================================
CREATE OR REPLACE PROCEDURE DENORMALIZE_SERVICENOW_TABLE(
    TABLE_NAME VARCHAR,
    CONNECTOR_DB VARCHAR,
    CONNECTOR_SCHEMA VARCHAR,
    DEST_DB VARCHAR,
    DEST_SCHEMA VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    var identifierRegex = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
    var tableName = TABLE_NAME.toLowerCase();

    if (!identifierRegex.test(tableName)) {
        return 'ERROR: Invalid table name: ' + TABLE_NAME;
    }

    // Step 1: Get references from the connector stored procedure
    snowflake.execute({sqlText: 'USE DATABASE ' + CONNECTOR_DB});
    snowflake.execute({sqlText: 'USE SCHEMA ' + CONNECTOR_SCHEMA});

    var refStmt = snowflake.execute({
        sqlText: "CALL " + CONNECTOR_DB + "." + CONNECTOR_SCHEMA +
                 ".SHOW_REFERENCES_OF_TABLE('" + tableName + "')"
    });

    var references = [];
    if (refStmt.next()) {
        var data = JSON.parse(refStmt.getColumnValueAsString(1));
        var refList = data.references || [];
        for (var i = 0; i < refList.length; i++) {
            var ref = refList[i];
            var colName = ref.columnName || '';
            var refCol = ref.referencedColumnName || '';
            var refTable = ref.referencedTableName || '';
            if (colName && refCol && refTable &&
                identifierRegex.test(colName) &&
                identifierRegex.test(refCol) &&
                identifierRegex.test(refTable)) {
                references.push({
                    column_name: colName.toUpperCase(),
                    referenced_table: refTable.toLowerCase(),
                    referenced_column: refCol.toUpperCase()
                });
            }
        }
    }

    // Step 2: Get choice fields from SYS_CHOICE__VIEW
    snowflake.execute({sqlText: 'USE DATABASE ' + DEST_DB});
    snowflake.execute({sqlText: 'USE SCHEMA ' + DEST_SCHEMA});

    var choiceFields = [];
    try {
        var choiceStmt = snowflake.execute({
            sqlText: "SELECT DISTINCT ELEMENT FROM " + DEST_DB + "." + DEST_SCHEMA +
                     ".SYS_CHOICE__VIEW WHERE NAME = '" + tableName + "'"
        });
        while (choiceStmt.next()) {
            var element = choiceStmt.getColumnValueAsString(1);
            if (element && identifierRegex.test(element)) {
                choiceFields.push(element.toUpperCase());
            }
        }
    } catch (e) {
        // SYS_CHOICE__VIEW may not exist yet; continue without choice fields
    }

    // Step 3: Resolve overlaps (reference wins over choice)
    var refColumns = {};
    for (var i = 0; i < references.length; i++) {
        refColumns[references[i].column_name] = true;
    }
    var filteredChoices = [];
    for (var i = 0; i < choiceFields.length; i++) {
        if (!refColumns[choiceFields[i]]) {
            filteredChoices.push(choiceFields[i]);
        }
    }
    choiceFields = filteredChoices;

    // Step 4: Check which referenced views exist
    var filteredRefs = [];
    for (var i = 0; i < references.length; i++) {
        var viewName = references[i].referenced_table.toUpperCase() + '__VIEW';
        try {
            var checkStmt = snowflake.execute({
                sqlText: "SHOW VIEWS LIKE '" + viewName + "' IN " + DEST_DB + "." + DEST_SCHEMA
            });
            if (checkStmt.next()) {
                filteredRefs.push(references[i]);
            }
        } catch (e) {
            // View does not exist, skip
        }
    }
    references = filteredRefs;

    // Step 5: Generate aliases (handle duplicate table references)
    var tableCounts = {};
    for (var i = 0; i < references.length; i++) {
        var t = references[i].referenced_table;
        if (!tableCounts[t]) tableCounts[t] = [];
        tableCounts[t].push(references[i].column_name);
    }

    var refAliases = {};
    for (var i = 0; i < references.length; i++) {
        var t = references[i].referenced_table;
        var col = references[i].column_name;
        if (tableCounts[t].length > 1) {
            refAliases[col] = 'ref_' + col.toLowerCase();
        } else {
            refAliases[col] = 'ref_' + t;
        }
    }

    var choiceAliases = {};
    for (var i = 0; i < choiceFields.length; i++) {
        choiceAliases[choiceFields[i]] = 'choice_' + choiceFields[i].toLowerCase();
    }

    // Step 6: Generate the CREATE VIEW SQL
    var dest = DEST_DB + '.' + DEST_SCHEMA;
    var tableUpper = tableName.toUpperCase();
    var sourceView = dest + '.' + tableUpper + '__VIEW';
    var targetView = dest + '.' + tableUpper + '__VIEW_DENORMALIZED';

    var selectParts = ['    t.*'];
    for (var i = 0; i < references.length; i++) {
        var alias = refAliases[references[i].column_name];
        selectParts.push('    ' + alias + '.' + references[i].referenced_column +
                         ' AS ' + references[i].column_name + '_DISPLAY');
    }
    for (var i = 0; i < choiceFields.length; i++) {
        var alias = choiceAliases[choiceFields[i]];
        selectParts.push('    ' + alias + '.LABEL AS ' + choiceFields[i] + '_DISPLAY');
    }

    var joinParts = [];
    for (var i = 0; i < references.length; i++) {
        var alias = refAliases[references[i].column_name];
        var refView = dest + '.' + references[i].referenced_table.toUpperCase() + '__VIEW';
        joinParts.push(
            'LEFT JOIN ' + refView + ' ' + alias + '\n' +
            '    ON PARSE_JSON(t.' + references[i].column_name +
            '):value::STRING = ' + alias + '.SYS_ID'
        );
    }
    for (var i = 0; i < choiceFields.length; i++) {
        var alias = choiceAliases[choiceFields[i]];
        joinParts.push(
            'LEFT JOIN ' + dest + '.SYS_CHOICE__VIEW ' + alias + '\n' +
            "    ON " + alias + ".NAME = '" + tableName + "'\n" +
            "    AND " + alias + ".ELEMENT = '" + choiceFields[i].toLowerCase() + "'\n" +
            '    AND ' + alias + '.VALUE = t.' + choiceFields[i]
        );
    }

    var sql = 'CREATE OR REPLACE VIEW ' + targetView + ' AS\nSELECT\n' +
              selectParts.join(',\n') + '\nFROM ' + sourceView + ' t';
    if (joinParts.length > 0) {
        sql += '\n' + joinParts.join('\n');
    }

    // Step 7: Execute
    snowflake.execute({sqlText: sql});

    return 'SUCCESS: Created ' + targetView +
           ' (' + references.length + ' reference joins, ' +
           choiceFields.length + ' choice joins)';

} catch (err) {
    return 'ERROR processing ' + TABLE_NAME + ': ' + err.message;
}
$$;

-- =============================================================================
-- Procedure 2: DENORMALIZE_ALL_SERVICENOW_TABLES
--
-- Discovers all synced ServiceNow tables and creates denormalized views
-- for each one.
--
-- Usage:
--   CALL DENORMALIZE_ALL_SERVICENOW_TABLES(
--       'SNOWFLAKE_CONNECTOR_FOR_SERVICENOW', 'PUBLIC',
--       'DEST_DB', 'DEST_SCHEMA'
--   );
-- =============================================================================
CREATE OR REPLACE PROCEDURE DENORMALIZE_ALL_SERVICENOW_TABLES(
    CONNECTOR_DB VARCHAR,
    CONNECTOR_SCHEMA VARCHAR,
    DEST_DB VARCHAR,
    DEST_SCHEMA VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Discover all synced tables by listing *__VIEW views
    snowflake.execute({sqlText: 'USE DATABASE ' + DEST_DB});
    snowflake.execute({sqlText: 'USE SCHEMA ' + DEST_SCHEMA});

    var viewStmt = snowflake.execute({
        sqlText: "SHOW VIEWS IN " + DEST_DB + "." + DEST_SCHEMA
    });

    var tables = [];
    var viewRegex = /^(.+)__VIEW$/i;
    var excludeRegex = /__(VIEW_WITH_DELETED|VIEW_DENORMALIZED)$/i;

    while (viewStmt.next()) {
        var viewName = viewStmt.getColumnValueAsString(2); // "name" column
        if (excludeRegex.test(viewName)) continue;
        var match = viewRegex.exec(viewName);
        if (match) {
            tables.push(match[1].toLowerCase());
        }
    }

    if (tables.length === 0) {
        return 'No synced tables found in ' + DEST_DB + '.' + DEST_SCHEMA;
    }

    var results = [];
    var successes = 0;
    var failures = 0;

    for (var i = 0; i < tables.length; i++) {
        var callStmt = snowflake.execute({
            sqlText: "CALL " + DEST_DB + "." + DEST_SCHEMA +
                     ".DENORMALIZE_SERVICENOW_TABLE('" + tables[i] + "', '" +
                     CONNECTOR_DB + "', '" + CONNECTOR_SCHEMA + "', '" +
                     DEST_DB + "', '" + DEST_SCHEMA + "')"
        });
        if (callStmt.next()) {
            var result = callStmt.getColumnValueAsString(1);
            results.push(result);
            if (result.indexOf('SUCCESS') === 0) {
                successes++;
            } else {
                failures++;
            }
        }
    }

    return 'Processed ' + tables.length + ' tables (' +
           successes + ' succeeded, ' + failures + ' failed):\n' +
           results.join('\n');

} catch (err) {
    return 'ERROR: ' + err.message;
}
$$;

-- =============================================================================
-- Task: DENORMALIZE_SERVICENOW_TASK
--
-- Runs DENORMALIZE_ALL_SERVICENOW_TABLES on a schedule.
-- Default: every hour. Adjust the CRON expression as needed.
--
-- To monitor: SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
--             WHERE NAME = 'DENORMALIZE_SERVICENOW_TASK' ORDER BY SCHEDULED_TIME DESC;
-- =============================================================================
CREATE OR REPLACE TASK DENORMALIZE_SERVICENOW_TASK
    WAREHOUSE = IDENTIFIER($WAREHOUSE_NAME)
    SCHEDULE = 'USING CRON 0 * * * * UTC'
    COMMENT = 'Auto-denormalize ServiceNow tables after connector sync'
AS
    CALL DENORMALIZE_ALL_SERVICENOW_TABLES(
        $CONNECTOR_DB, $CONNECTOR_SCHEMA, $DEST_DB, $DEST_SCHEMA
    );

-- Tasks are created in SUSPENDED state by default; resume to activate:
ALTER TASK DENORMALIZE_SERVICENOW_TASK RESUME;

-- =============================================================================
-- Verification
-- =============================================================================
SHOW PROCEDURES LIKE 'DENORMALIZE_%';
SHOW TASKS LIKE 'DENORMALIZE_%';

SELECT 'Setup complete. The task will run hourly.' AS STATUS;
