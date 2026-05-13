-- ============================================================================
-- EPF Data Purge - PL/SQL Package Body
-- ============================================================================
-- Implements all purge logic with:
--   - BULK COLLECT + FORALL for batch processing
--   - Autonomous transaction logging (survives rollbacks)
--   - Dry-run mode (counts without deleting)
--   - Optional space reclamation
-- ============================================================================

CREATE OR REPLACE PACKAGE BODY oppayments.epf_purge_pkg
AS

    -- ========================================================================
    -- Private constants
    -- ========================================================================
    C_PKG_NAME CONSTANT VARCHAR2(30) := 'EPF_PURGE_PKG';

    -- List of all tables that may be purged (used by reclaim_space)
    TYPE t_table_list IS TABLE OF VARCHAR2(128) INDEX BY PLS_INTEGER;

    -- ========================================================================
    -- Private: log_entry
    -- ========================================================================
    -- Writes a single row to epf_purge_log using an autonomous transaction
    -- so the log entry persists even if the calling transaction rolls back.
    PROCEDURE log_entry(
        p_run_id          IN RAW,
        p_module          IN VARCHAR2,
        p_operation       IN VARCHAR2,
        p_table_name      IN VARCHAR2 DEFAULT NULL,
        p_rows_affected   IN NUMBER   DEFAULT 0,
        p_batch_number    IN NUMBER   DEFAULT NULL,
        p_retention_days  IN NUMBER   DEFAULT NULL,
        p_status          IN VARCHAR2,
        p_message         IN VARCHAR2 DEFAULT NULL,
        p_error_code      IN VARCHAR2 DEFAULT NULL,
        p_error_message   IN VARCHAR2 DEFAULT NULL,
        p_elapsed_seconds IN NUMBER   DEFAULT NULL
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO oppayments.epf_purge_log (
            run_id, log_timestamp, module, operation, table_name,
            rows_affected, batch_number, retention_days, status,
            message, error_code, error_message, elapsed_seconds
        ) VALUES (
            p_run_id, SYSTIMESTAMP, p_module, p_operation, p_table_name,
            p_rows_affected, p_batch_number, p_retention_days, p_status,
            p_message, p_error_code, p_error_message, p_elapsed_seconds
        );
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            -- If logging itself fails, output to DBMS_OUTPUT as a fallback
            DBMS_OUTPUT.PUT_LINE('WARNING: Failed to write log entry: ' || SQLERRM);
            ROLLBACK;
    END log_entry;

    -- ========================================================================
    -- Private: get_elapsed_seconds
    -- ========================================================================
    -- Returns elapsed seconds between two timestamps.
    FUNCTION get_elapsed_seconds(
        p_start IN TIMESTAMP,
        p_end   IN TIMESTAMP
    ) RETURN NUMBER
    IS
        l_interval INTERVAL DAY TO SECOND;
    BEGIN
        l_interval := p_end - p_start;
        RETURN EXTRACT(DAY FROM l_interval) * 86400
             + EXTRACT(HOUR FROM l_interval) * 3600
             + EXTRACT(MINUTE FROM l_interval) * 60
             + EXTRACT(SECOND FROM l_interval);
    END get_elapsed_seconds;

    -- ========================================================================
    -- Private: tbl_exists
    -- ========================================================================
    -- Returns TRUE if the specified table exists in ALL_TABLES.
    -- Used to guard dynamic SQL against missing tables (some EPF schemas
    -- may not have every table depending on the deployed application version).
    FUNCTION tbl_exists(p_owner VARCHAR2, p_table VARCHAR2) RETURN BOOLEAN
    IS
        l_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO l_cnt
        FROM all_tables
        WHERE owner = UPPER(p_owner)
          AND table_name = UPPER(p_table);
        RETURN l_cnt > 0;
    END tbl_exists;

    -- ========================================================================
    -- Private: clear_table_clobs (SYS.ODCINUMBERLIST overload)
    -- ========================================================================
    -- Dynamically discovers all CLOB/BLOB columns on the target table via
    -- ALL_LOBS and clears them (SET col = EMPTY_CLOB()) for rows matching
    -- the supplied ID list.
    --
    -- Returns the total number of row-level updates across all LOB columns.
    -- If the table does not exist or has no LOB columns, returns 0.
    FUNCTION clear_table_clobs(
        p_run_id     IN RAW,
        p_module     IN VARCHAR2,
        p_owner      IN VARCHAR2,
        p_table_name IN VARCHAR2,
        p_id_column  IN VARCHAR2,
        p_id_list    IN SYS.ODCINUMBERLIST
    ) RETURN NUMBER
    IS
        l_total   NUMBER := 0;
        l_updated NUMBER;
        l_col_list VARCHAR2(4000);
    BEGIN
        IF p_id_list IS NULL OR p_id_list.COUNT = 0 THEN
            RETURN 0;
        END IF;
        IF NOT tbl_exists(p_owner, p_table_name) THEN
            RETURN 0;
        END IF;

        FOR rec IN (
            SELECT column_name
            FROM all_lobs
            WHERE owner = UPPER(p_owner)
              AND table_name = UPPER(p_table_name)
            ORDER BY column_name
        ) LOOP
            EXECUTE IMMEDIATE
                'UPDATE ' || p_owner || '.' || p_table_name
                || ' SET ' || rec.column_name || ' = EMPTY_CLOB()'
                || ' WHERE ' || p_id_column
                || ' IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                || ' AND ' || rec.column_name || ' IS NOT NULL'
                USING p_id_list;
            l_updated := SQL%ROWCOUNT;
            l_total := l_total + l_updated;
            IF l_col_list IS NOT NULL THEN
                l_col_list := l_col_list || ', ';
            END IF;
            l_col_list := l_col_list || rec.column_name || '(' || l_updated || ')';
        END LOOP;

        IF l_total > 0 THEN
            log_entry(
                p_run_id       => p_run_id,
                p_module       => p_module,
                p_operation    => 'CLOB_CLEAR',
                p_table_name   => p_owner || '.' || p_table_name,
                p_rows_affected => l_total,
                p_status       => 'SUCCESS',
                p_message      => l_col_list
            );
            DBMS_OUTPUT.PUT_LINE(p_table_name || ': ' || l_total
                || ' CLOBs cleared [' || l_col_list || ']');
        END IF;

        RETURN l_total;
    END clear_table_clobs;

    -- ========================================================================
    -- Private: clear_table_clobs (epf_number_tab overload)
    -- ========================================================================
    -- Same as above but accepts oppayments.epf_number_tab (used for
    -- materialised payment_id lists in purge_bulk_payments).
    FUNCTION clear_table_clobs(
        p_run_id     IN RAW,
        p_module     IN VARCHAR2,
        p_owner      IN VARCHAR2,
        p_table_name IN VARCHAR2,
        p_id_column  IN VARCHAR2,
        p_id_list    IN oppayments.epf_number_tab
    ) RETURN NUMBER
    IS
        l_total   NUMBER := 0;
        l_updated NUMBER;
        l_col_list VARCHAR2(4000);
    BEGIN
        IF p_id_list IS NULL OR p_id_list.COUNT = 0 THEN
            RETURN 0;
        END IF;
        IF NOT tbl_exists(p_owner, p_table_name) THEN
            RETURN 0;
        END IF;

        FOR rec IN (
            SELECT column_name
            FROM all_lobs
            WHERE owner = UPPER(p_owner)
              AND table_name = UPPER(p_table_name)
            ORDER BY column_name
        ) LOOP
            EXECUTE IMMEDIATE
                'UPDATE ' || p_owner || '.' || p_table_name
                || ' SET ' || rec.column_name || ' = EMPTY_CLOB()'
                || ' WHERE ' || p_id_column
                || ' IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                || ' AND ' || rec.column_name || ' IS NOT NULL'
                USING p_id_list;
            l_updated := SQL%ROWCOUNT;
            l_total := l_total + l_updated;
            IF l_col_list IS NOT NULL THEN
                l_col_list := l_col_list || ', ';
            END IF;
            l_col_list := l_col_list || rec.column_name || '(' || l_updated || ')';
        END LOOP;

        IF l_total > 0 THEN
            log_entry(
                p_run_id       => p_run_id,
                p_module       => p_module,
                p_operation    => 'CLOB_CLEAR',
                p_table_name   => p_owner || '.' || p_table_name,
                p_rows_affected => l_total,
                p_status       => 'SUCCESS',
                p_message      => l_col_list
            );
            DBMS_OUTPUT.PUT_LINE(p_table_name || ': ' || l_total
                || ' CLOBs cleared [' || l_col_list || ']');
        END IF;

        RETURN l_total;
    END clear_table_clobs;

    -- ========================================================================
    -- Private: clear_table_clobs_by_date
    -- ========================================================================
    -- Variant for tables filtered by a date column (e.g. file_integration,
    -- audit_trail, spec_trt_log). Discovers LOB columns dynamically and
    -- clears them in ROWNUM-batched updates.
    -- Returns total rows updated across all LOB columns.
    FUNCTION clear_table_clobs_by_date(
        p_run_id      IN RAW,
        p_module      IN VARCHAR2,
        p_owner       IN VARCHAR2,
        p_table_name  IN VARCHAR2,
        p_date_column IN VARCHAR2,
        p_cutoff_date IN DATE,
        p_batch_size  IN NUMBER,
        p_dry_run     IN BOOLEAN DEFAULT FALSE
    ) RETURN NUMBER
    IS
        l_total      NUMBER := 0;
        l_updated    NUMBER;
        l_col_list   VARCHAR2(4000);
        l_has_lobs   BOOLEAN := FALSE;
    BEGIN
        IF NOT tbl_exists(p_owner, p_table_name) THEN
            RETURN 0;
        END IF;

        FOR rec IN (
            SELECT column_name
            FROM all_lobs
            WHERE owner = UPPER(p_owner)
              AND table_name = UPPER(p_table_name)
            ORDER BY column_name
        ) LOOP
            l_has_lobs := TRUE;

            IF p_dry_run THEN
                EXECUTE IMMEDIATE
                    'SELECT COUNT(*) FROM ' || p_owner || '.' || p_table_name
                    || ' WHERE ' || p_date_column || ' < :1'
                    || ' AND ' || rec.column_name || ' IS NOT NULL'
                    INTO l_updated USING p_cutoff_date;
                l_total := l_total + l_updated;
            ELSE
                -- Batched update loop
                LOOP
                    EXECUTE IMMEDIATE
                        'UPDATE ' || p_owner || '.' || p_table_name
                        || ' SET ' || rec.column_name || ' = EMPTY_CLOB()'
                        || ' WHERE ' || p_date_column || ' < :1'
                        || ' AND ' || rec.column_name || ' IS NOT NULL'
                        || ' AND ROWNUM <= :2'
                        USING p_cutoff_date, p_batch_size;
                    l_updated := SQL%ROWCOUNT;
                    EXIT WHEN l_updated = 0;
                    l_total := l_total + l_updated;
                    COMMIT;
                END LOOP;
            END IF;

            IF l_col_list IS NOT NULL THEN
                l_col_list := l_col_list || ', ';
            END IF;
            l_col_list := l_col_list || rec.column_name || '(' || l_updated || ')';
        END LOOP;

        IF NOT l_has_lobs THEN
            DBMS_OUTPUT.PUT_LINE(p_table_name || ': no LOB columns found, skipped');
            log_entry(
                p_run_id    => p_run_id,
                p_module    => p_module,
                p_operation => 'CLOB_CLEAR',
                p_status    => 'INFO',
                p_message   => p_owner || '.' || p_table_name
                               || ': no LOB columns found, skipped'
            );
            RETURN 0;
        END IF;

        IF l_total > 0 THEN
            log_entry(
                p_run_id       => p_run_id,
                p_module       => p_module,
                p_operation    => CASE WHEN p_dry_run THEN 'DRY_RUN_COUNT'
                                      ELSE 'CLOB_CLEAR' END,
                p_table_name   => p_owner || '.' || p_table_name,
                p_rows_affected => l_total,
                p_status       => 'SUCCESS',
                p_message      => CASE WHEN p_dry_run
                                       THEN 'Dry run: ' || l_total || ' CLOBs would be cleared'
                                       ELSE l_col_list END
            );
            DBMS_OUTPUT.PUT_LINE(
                CASE WHEN p_dry_run THEN '[DRY RUN] ' ELSE '' END
                || p_table_name || ': ' || l_total
                || ' CLOBs ' || CASE WHEN p_dry_run THEN 'would be ' ELSE '' END
                || 'cleared [' || l_col_list || ']');
        ELSE
            DBMS_OUTPUT.PUT_LINE(p_table_name || ': 0 CLOBs to clear');
        END IF;

        RETURN l_total;
    END clear_table_clobs_by_date;

    -- ========================================================================
    -- Private: get_purged_tables
    -- ========================================================================
    -- Returns the list of all tables involved in purge operations.
    FUNCTION get_purged_tables RETURN t_table_list
    IS
        l_tables t_table_list;
    BEGIN
        l_tables(1)  := 'oppayments.bulk_payment_additional_info';
        l_tables(2)  := 'oppayments.bulk_signature';
        l_tables(3)  := 'oppayments.mandatory_signers';
        l_tables(4)  := 'oppayments.oidc_request_token';
        l_tables(5)  := 'oppayments.payment_audit';
        l_tables(6)  := 'oppayments.import_audit_messages';
        l_tables(7)  := 'oppayments.import_audit';
        l_tables(8)  := 'oppayments.transmission_execution_audit';
        l_tables(9)  := 'oppayments.transmission_execution';
        l_tables(10) := 'oppayments.transmission_exception';
        l_tables(11) := 'oppayments.notification_execution';
        l_tables(12) := 'oppayments.approbation_execution';
        l_tables(13) := 'oppayments.approbation_execution_opt';
        l_tables(14) := 'oppayments.workflow_execution';
        l_tables(15) := 'oppayments.workflow_execution_opt';
        l_tables(16) := 'oppayments.bulkpayment_exception';
        l_tables(17) := 'oppayments.invoice_additional_info';
        l_tables(18) := 'oppayments.invoice';
        l_tables(19) := 'oppayments.payment_additional_info';
        l_tables(20) := 'oppayments.payment';
        l_tables(21) := 'oppayments.bulk_payment';
        l_tables(22) := 'oppayments.file_integration';
        l_tables(23) := 'oppayments.audit_archive';
        l_tables(24) := 'oppayments.audit_trail';
        l_tables(25) := 'op.spec_trt_log';
        l_tables(26) := 'oppayments.directory_dispatching';
        l_tables(27) := 'oppayments.file_dispatching';
        RETURN l_tables;
    END get_purged_tables;

    -- ========================================================================
    -- ensure_log_table
    -- ========================================================================
    PROCEDURE ensure_log_table
    IS
        l_table_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO l_table_exists
        FROM user_tables
        WHERE table_name = 'EPF_PURGE_LOG';

        IF l_table_exists = 0 THEN
            EXECUTE IMMEDIATE '
                CREATE TABLE oppayments.epf_purge_log (
                    log_id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    run_id            RAW(16)        NOT NULL,
                    log_timestamp     TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL,
                    module            VARCHAR2(50)   NOT NULL,
                    operation         VARCHAR2(50)   NOT NULL,
                    table_name        VARCHAR2(128),
                    rows_affected     NUMBER         DEFAULT 0,
                    batch_number      NUMBER,
                    retention_days    NUMBER,
                    status            VARCHAR2(20)   NOT NULL,
                    message           VARCHAR2(4000),
                    error_code        VARCHAR2(50),
                    error_message     VARCHAR2(4000),
                    elapsed_seconds   NUMBER(10,3),
                    CONSTRAINT chk_purge_log_status
                        CHECK (status IN (''SUCCESS'', ''ERROR'', ''WARNING'', ''INFO''))
                )';

            EXECUTE IMMEDIATE '
                CREATE INDEX oppayments.idx_epf_purge_log_run
                ON oppayments.epf_purge_log (run_id)';

            EXECUTE IMMEDIATE '
                CREATE INDEX oppayments.idx_epf_purge_log_ts
                ON oppayments.epf_purge_log (log_timestamp)';

            EXECUTE IMMEDIATE '
                CREATE INDEX oppayments.idx_epf_purge_log_module
                ON oppayments.epf_purge_log (module)';

            DBMS_OUTPUT.PUT_LINE('EPF_PURGE_LOG table created.');
        END IF;

        -- Ensure space snapshot table exists
        SELECT COUNT(*) INTO l_table_exists
        FROM user_tables
        WHERE table_name = 'EPF_PURGE_SPACE_SNAPSHOT';

        IF l_table_exists = 0 THEN
            EXECUTE IMMEDIATE '
                CREATE TABLE oppayments.epf_purge_space_snapshot (
                    snapshot_id       NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    run_id            RAW(16)        NOT NULL,
                    snapshot_phase    VARCHAR2(20)   NOT NULL,
                    snapshot_timestamp TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL,
                    owner             VARCHAR2(128)  NOT NULL,
                    segment_name      VARCHAR2(128)  NOT NULL,
                    segment_type      VARCHAR2(30),
                    parent_table      VARCHAR2(128),
                    size_bytes        NUMBER         NOT NULL,
                    size_mb           NUMBER(12,2),
                    module            VARCHAR2(20),
                    CONSTRAINT chk_snapshot_phase
                        CHECK (snapshot_phase IN (''BEFORE'', ''AFTER''))
                )';

            EXECUTE IMMEDIATE '
                CREATE INDEX oppayments.idx_epf_space_snap_run
                ON oppayments.epf_purge_space_snapshot (run_id, snapshot_phase)';

            DBMS_OUTPUT.PUT_LINE('EPF_PURGE_SPACE_SNAPSHOT table created.');
        ELSE
            -- Migration: add module column if it doesn't exist (older installs)
            DECLARE
                l_col_exists NUMBER;
            BEGIN
                SELECT COUNT(*) INTO l_col_exists
                FROM user_tab_columns
                WHERE table_name = 'EPF_PURGE_SPACE_SNAPSHOT'
                  AND column_name = 'MODULE';
                IF l_col_exists = 0 THEN
                    EXECUTE IMMEDIATE
                        'ALTER TABLE oppayments.epf_purge_space_snapshot ADD (module VARCHAR2(20))';
                END IF;
            END;
        END IF;
    END ensure_log_table;

    -- ========================================================================
    -- purge_file_integrations
    -- ========================================================================
    -- Purges file_integration records older than the cutoff date.
    -- Uses simple ROWNUM-batched deletes (single table, no FK dependencies).
    PROCEDURE purge_file_integrations(
        p_run_id      IN RAW,
        p_cutoff_date IN DATE,
        p_batch_size  IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run     IN BOOLEAN  DEFAULT FALSE,
        p_purge_mode  IN VARCHAR2 DEFAULT 'FULL'
    )
    IS
        l_batch_count  NUMBER := 0;
        l_total_count  NUMBER := 0;
        l_rows_deleted NUMBER;
        l_start_ts     TIMESTAMP;
        l_batch_start  TIMESTAMP;
    BEGIN
        l_start_ts := SYSTIMESTAMP;

        -- Dynamically detect and clear any CLOB columns in CLOB-only modes
        IF p_purge_mode IN (C_MODE_CLOB_ONLY, C_MODE_CLOB_N_LOGS) THEN
            DECLARE
                l_clob_total NUMBER;
            BEGIN
                l_clob_total := clear_table_clobs_by_date(
                    p_run_id       => p_run_id,
                    p_module       => 'FILE_INTEGRATION',
                    p_owner        => 'OPPAYMENTS',
                    p_table_name   => 'FILE_INTEGRATION',
                    p_date_column  => 'INTEGRATION_DATE',
                    p_cutoff_date  => p_cutoff_date,
                    p_batch_size   => p_batch_size,
                    p_dry_run      => p_dry_run
                );
            END;
            RETURN;
        END IF;

        -- Table may not exist in all EPF schemas
        IF NOT tbl_exists('OPPAYMENTS', 'FILE_INTEGRATION') THEN
            log_entry(p_run_id => p_run_id, p_module => 'FILE_INTEGRATION',
                p_operation => 'DELETE', p_status => 'WARNING',
                p_message => 'Skipped: table oppayments.file_integration does not exist');
            DBMS_OUTPUT.PUT_LINE('file_integration: skipped (table does not exist)');
            RETURN;
        END IF;

        log_entry(
            p_run_id    => p_run_id,
            p_module    => 'FILE_INTEGRATION',
            p_operation => 'DELETE',
            p_status    => 'INFO',
            p_message   => 'Starting file integration purge. Cutoff date: '
                           || TO_CHAR(p_cutoff_date, 'YYYY-MM-DD')
        );

        IF p_dry_run THEN
            EXECUTE IMMEDIATE
                'SELECT COUNT(*) FROM oppayments.file_integration
                 WHERE integration_date < :1'
                INTO l_total_count USING p_cutoff_date;

            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'FILE_INTEGRATION',
                p_operation     => 'DRY_RUN_COUNT',
                p_table_name    => 'oppayments.file_integration',
                p_rows_affected => l_total_count,
                p_status        => 'SUCCESS',
                p_message       => 'Dry run: ' || l_total_count || ' rows would be deleted',
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('[DRY RUN] file_integration: ' || l_total_count || ' rows would be deleted');
            RETURN;
        END IF;

        LOOP
            l_batch_start := SYSTIMESTAMP;
            l_batch_count := l_batch_count + 1;

            EXECUTE IMMEDIATE
                'DELETE FROM oppayments.file_integration
                 WHERE integration_date < :1 AND ROWNUM <= :2'
                USING p_cutoff_date, p_batch_size;

            l_rows_deleted := SQL%ROWCOUNT;
            EXIT WHEN l_rows_deleted = 0;

            l_total_count := l_total_count + l_rows_deleted;
            COMMIT;

            log_entry(
                p_run_id          => p_run_id,
                p_module          => 'FILE_INTEGRATION',
                p_operation       => 'DELETE',
                p_table_name      => 'oppayments.file_integration',
                p_rows_affected   => l_rows_deleted,
                p_batch_number    => l_batch_count,
                p_status          => 'SUCCESS',
                p_elapsed_seconds => get_elapsed_seconds(l_batch_start, SYSTIMESTAMP)
            );
        END LOOP;

        log_entry(
            p_run_id          => p_run_id,
            p_module          => 'FILE_INTEGRATION',
            p_operation       => 'DELETE',
            p_table_name      => 'oppayments.file_integration',
            p_rows_affected   => l_total_count,
            p_status          => 'SUCCESS',
            p_message         => 'Completed. Total rows deleted: ' || l_total_count,
            p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
        );

        DBMS_OUTPUT.PUT_LINE('file_integration: ' || l_total_count || ' rows deleted');

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'FILE_INTEGRATION',
                p_operation     => 'DELETE',
                p_table_name    => 'oppayments.file_integration',
                p_rows_affected => l_total_count,
                p_status        => 'ERROR',
                p_error_code    => SQLCODE,
                p_error_message => SQLERRM,
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('ERROR in purge_file_integrations: ' || SQLERRM);
    END purge_file_integrations;

    -- ========================================================================
    -- purge_tech_logs
    -- ========================================================================
    -- Purges technical log records from op.spec_trt_log.
    -- Uses ROWNUM-batched deletes.
    PROCEDURE purge_tech_logs(
        p_run_id      IN RAW,
        p_cutoff_date IN DATE,
        p_batch_size  IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run     IN BOOLEAN  DEFAULT FALSE,
        p_purge_mode  IN VARCHAR2 DEFAULT 'FULL'
    )
    IS
        l_batch_count  NUMBER := 0;
        l_total_count  NUMBER := 0;
        l_rows_deleted NUMBER;
        l_start_ts     TIMESTAMP;
        l_batch_start  TIMESTAMP;
    BEGIN
        l_start_ts := SYSTIMESTAMP;

        -- Dynamically detect and clear any CLOB columns in CLOB-only modes
        -- (CLOB_N_LOGS is dispatched as FULL by run_purge, but handle defensively)
        IF p_purge_mode IN (C_MODE_CLOB_ONLY, C_MODE_CLOB_N_LOGS) THEN
            DECLARE
                l_clob_total NUMBER;
            BEGIN
                l_clob_total := clear_table_clobs_by_date(
                    p_run_id       => p_run_id,
                    p_module       => 'TECH_LOGS',
                    p_owner        => 'OP',
                    p_table_name   => 'SPEC_TRT_LOG',
                    p_date_column  => 'DTLOG',
                    p_cutoff_date  => p_cutoff_date,
                    p_batch_size   => p_batch_size,
                    p_dry_run      => p_dry_run
                );
            END;
            RETURN;
        END IF;

        log_entry(
            p_run_id    => p_run_id,
            p_module    => 'TECH_LOGS',
            p_operation => 'DELETE',
            p_status    => 'INFO',
            p_message   => 'Starting technical logs purge. Cutoff date: '
                           || TO_CHAR(p_cutoff_date, 'YYYY-MM-DD')
        );

        IF p_dry_run THEN
            EXECUTE IMMEDIATE
                'SELECT COUNT(*) FROM op.spec_trt_log WHERE dtlog < :1'
                INTO l_total_count USING p_cutoff_date;

            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'TECH_LOGS',
                p_operation     => 'DRY_RUN_COUNT',
                p_table_name    => 'op.spec_trt_log',
                p_rows_affected => l_total_count,
                p_status        => 'SUCCESS',
                p_message       => 'Dry run: ' || l_total_count || ' rows would be deleted',
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('[DRY RUN] op.spec_trt_log: ' || l_total_count || ' rows would be deleted');
            RETURN;
        END IF;

        LOOP
            l_batch_start := SYSTIMESTAMP;
            l_batch_count := l_batch_count + 1;

            EXECUTE IMMEDIATE
                'DELETE FROM op.spec_trt_log WHERE dtlog < :1 AND ROWNUM <= :2'
                USING p_cutoff_date, p_batch_size;

            l_rows_deleted := SQL%ROWCOUNT;
            EXIT WHEN l_rows_deleted = 0;

            l_total_count := l_total_count + l_rows_deleted;
            COMMIT;

            log_entry(
                p_run_id          => p_run_id,
                p_module          => 'TECH_LOGS',
                p_operation       => 'DELETE',
                p_table_name      => 'op.spec_trt_log',
                p_rows_affected   => l_rows_deleted,
                p_batch_number    => l_batch_count,
                p_status          => 'SUCCESS',
                p_elapsed_seconds => get_elapsed_seconds(l_batch_start, SYSTIMESTAMP)
            );
        END LOOP;

        log_entry(
            p_run_id          => p_run_id,
            p_module          => 'TECH_LOGS',
            p_operation       => 'DELETE',
            p_table_name      => 'op.spec_trt_log',
            p_rows_affected   => l_total_count,
            p_status          => 'SUCCESS',
            p_message         => 'Completed. Total rows deleted: ' || l_total_count,
            p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
        );

        DBMS_OUTPUT.PUT_LINE('op.spec_trt_log: ' || l_total_count || ' rows deleted');

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'TECH_LOGS',
                p_operation     => 'DELETE',
                p_table_name    => 'op.spec_trt_log',
                p_rows_affected => l_total_count,
                p_status        => 'ERROR',
                p_error_code    => SQLCODE,
                p_error_message => SQLERRM,
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('ERROR in purge_tech_logs: ' || SQLERRM);
    END purge_tech_logs;

    -- ========================================================================
    -- purge_audit_logs
    -- ========================================================================
    -- Purges audit_trail and audit_archive records using BULK COLLECT.
    -- Deletes audit_archive first (child), then audit_trail (parent).
    PROCEDURE purge_audit_logs(
        p_run_id      IN RAW,
        p_cutoff_date IN DATE,
        p_batch_size  IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run     IN BOOLEAN  DEFAULT FALSE,
        p_purge_mode  IN VARCHAR2 DEFAULT 'FULL'
    )
    IS
        l_audit_ids    SYS.ODCINUMBERLIST;
        l_archive_ids  SYS.ODCINUMBERLIST;
        l_batch_count  NUMBER := 0;
        l_total_audit  NUMBER := 0;
        l_total_archive NUMBER := 0;
        l_rows_deleted NUMBER;
        l_start_ts     TIMESTAMP;
        l_batch_start  TIMESTAMP;
        l_has_audit_trail  BOOLEAN;
        l_has_audit_archive BOOLEAN;

        l_refcur       SYS_REFCURSOR;
    BEGIN
        l_start_ts := SYSTIMESTAMP;

        -- Dynamically detect and clear any CLOB columns in CLOB-only modes.
        -- (CLOB_N_LOGS is dispatched as FULL by run_purge, but handle defensively.)
        -- Uses the same batched cursor as FULL mode since audit_archive is linked
        -- by audit_archive_id (not by a date column).
        IF p_purge_mode IN (C_MODE_CLOB_ONLY, C_MODE_CLOB_N_LOGS) THEN
            -- audit_trail CLOBs: filter directly by date
            DECLARE
                l_clob_total NUMBER := 0;
            BEGIN
                l_clob_total := clear_table_clobs_by_date(
                    p_run_id       => p_run_id,
                    p_module       => 'AUDIT_LOGS',
                    p_owner        => 'OPPAYMENTS',
                    p_table_name   => 'AUDIT_TRAIL',
                    p_date_column  => 'AUDIT_TIMESTAMP',
                    p_cutoff_date  => p_cutoff_date,
                    p_batch_size   => p_batch_size,
                    p_dry_run      => p_dry_run
                );
            END;

            -- audit_archive CLOBs: must use ID-based approach via audit_trail
            IF tbl_exists('OPPAYMENTS', 'AUDIT_ARCHIVE')
               AND tbl_exists('OPPAYMENTS', 'AUDIT_TRAIL')
            THEN
                DECLARE
                    l_arc_refcur SYS_REFCURSOR;
                    l_arc_ids    SYS.ODCINUMBERLIST;
                    l_arc_total  NUMBER := 0;
                BEGIN
                    OPEN l_arc_refcur FOR
                        'SELECT DISTINCT aa.audit_archive_id
                         FROM oppayments.audit_archive aa
                         INNER JOIN oppayments.audit_trail at2
                             ON at2.audit_archive_id = aa.audit_archive_id
                         WHERE at2.audit_timestamp < :1'
                        USING p_cutoff_date;
                    LOOP
                        FETCH l_arc_refcur BULK COLLECT INTO l_arc_ids
                            LIMIT p_batch_size;
                        EXIT WHEN l_arc_ids.COUNT = 0;
                        l_arc_total := l_arc_total + clear_table_clobs(
                            p_run_id, 'AUDIT_LOGS', 'OPPAYMENTS',
                            'AUDIT_ARCHIVE', 'AUDIT_ARCHIVE_ID', l_arc_ids);
                        COMMIT;
                    END LOOP;
                    CLOSE l_arc_refcur;
                EXCEPTION
                    WHEN OTHERS THEN
                        IF l_arc_refcur%ISOPEN THEN CLOSE l_arc_refcur; END IF;
                        -- audit_archive may not have CLOBs — log and continue
                        DBMS_OUTPUT.PUT_LINE('audit_archive CLOBs: ' || SQLERRM);
                END;
            END IF;

            RETURN;
        END IF;

        -- Check table existence
        l_has_audit_trail  := tbl_exists('OPPAYMENTS', 'AUDIT_TRAIL');
        l_has_audit_archive := tbl_exists('OPPAYMENTS', 'AUDIT_ARCHIVE');

        IF NOT l_has_audit_trail THEN
            log_entry(p_run_id => p_run_id, p_module => 'AUDIT_LOGS',
                p_operation => 'DELETE', p_status => 'WARNING',
                p_message => 'Skipped: table oppayments.audit_trail does not exist');
            DBMS_OUTPUT.PUT_LINE('audit_logs: skipped (audit_trail table does not exist)');
            RETURN;
        END IF;

        log_entry(
            p_run_id    => p_run_id,
            p_module    => 'AUDIT_LOGS',
            p_operation => 'DELETE',
            p_status    => 'INFO',
            p_message   => 'Starting audit logs purge. Cutoff date: '
                           || TO_CHAR(p_cutoff_date, 'YYYY-MM-DD')
        );

        IF p_dry_run THEN
            EXECUTE IMMEDIATE
                'SELECT COUNT(*) FROM oppayments.audit_trail
                 WHERE audit_timestamp < :1'
                INTO l_total_audit USING p_cutoff_date;

            IF l_has_audit_archive THEN
                EXECUTE IMMEDIATE
                    'SELECT COUNT(*) FROM oppayments.audit_archive aa
                     WHERE EXISTS (
                         SELECT 1 FROM oppayments.audit_trail at2
                         WHERE at2.audit_archive_id = aa.audit_archive_id
                           AND at2.audit_timestamp < :1
                     )'
                    INTO l_total_archive USING p_cutoff_date;
            END IF;

            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'AUDIT_LOGS',
                p_operation     => 'DRY_RUN_COUNT',
                p_table_name    => 'oppayments.audit_trail',
                p_rows_affected => l_total_audit,
                p_status        => 'SUCCESS',
                p_message       => 'Dry run: ' || l_total_audit || ' audit_trail + '
                                   || l_total_archive || ' audit_archive rows would be deleted',
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('[DRY RUN] audit_trail: ' || l_total_audit
                || ', audit_archive: ' || l_total_archive || ' rows would be deleted');
            RETURN;
        END IF;

        OPEN l_refcur FOR
            'SELECT audit_id, audit_archive_id
             FROM oppayments.audit_trail
             WHERE audit_timestamp < :1
             ORDER BY audit_id'
            USING p_cutoff_date;
        LOOP
            l_batch_start := SYSTIMESTAMP;
            l_batch_count := l_batch_count + 1;

            FETCH l_refcur BULK COLLECT INTO l_audit_ids, l_archive_ids
                LIMIT p_batch_size;

            EXIT WHEN l_audit_ids.COUNT = 0;

            -- Delete audit_archive first (child table)
            IF l_has_audit_archive THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.audit_archive
                     WHERE audit_archive_id IN (
                         SELECT COLUMN_VALUE FROM TABLE(:1)
                     ) AND COLUMN_VALUE IS NOT NULL'
                    USING l_archive_ids;

                l_rows_deleted := SQL%ROWCOUNT;
                l_total_archive := l_total_archive + l_rows_deleted;
            ELSE
                l_rows_deleted := 0;
            END IF;

            log_entry(
                p_run_id          => p_run_id,
                p_module          => 'AUDIT_LOGS',
                p_operation       => 'DELETE',
                p_table_name      => 'oppayments.audit_archive',
                p_rows_affected   => l_rows_deleted,
                p_batch_number    => l_batch_count,
                p_status          => 'SUCCESS',
                p_elapsed_seconds => get_elapsed_seconds(l_batch_start, SYSTIMESTAMP)
            );

            -- Delete audit_trail (parent table)
            EXECUTE IMMEDIATE
                'DELETE FROM oppayments.audit_trail
                 WHERE audit_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                USING l_audit_ids;

            l_rows_deleted := SQL%ROWCOUNT;
            l_total_audit := l_total_audit + l_rows_deleted;

            log_entry(
                p_run_id          => p_run_id,
                p_module          => 'AUDIT_LOGS',
                p_operation       => 'DELETE',
                p_table_name      => 'oppayments.audit_trail',
                p_rows_affected   => l_rows_deleted,
                p_batch_number    => l_batch_count,
                p_status          => 'SUCCESS',
                p_elapsed_seconds => get_elapsed_seconds(l_batch_start, SYSTIMESTAMP)
            );

            COMMIT;
        END LOOP;
        CLOSE l_refcur;

        log_entry(
            p_run_id          => p_run_id,
            p_module          => 'AUDIT_LOGS',
            p_operation       => 'DELETE',
            p_status          => 'SUCCESS',
            p_message         => 'Completed. audit_trail: ' || l_total_audit
                                 || ', audit_archive: ' || l_total_archive || ' rows deleted',
            p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
        );

        DBMS_OUTPUT.PUT_LINE('audit_trail: ' || l_total_audit || ', audit_archive: '
            || l_total_archive || ' rows deleted');

    EXCEPTION
        WHEN OTHERS THEN
            IF l_refcur%ISOPEN THEN CLOSE l_refcur; END IF;
            ROLLBACK;
            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'AUDIT_LOGS',
                p_operation     => 'DELETE',
                p_status        => 'ERROR',
                p_error_code    => SQLCODE,
                p_error_message => SQLERRM,
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('ERROR in purge_audit_logs: ' || SQLERRM);
    END purge_audit_logs;

    -- ========================================================================
    -- purge_bank_statements
    -- ========================================================================
    -- Purges bank statement dispatching records using BULK COLLECT.
    -- Deletes directory_dispatching first (child), then file_dispatching (parent).
    -- Handles deduplication: one file_dispatching can have multiple directories.
    PROCEDURE purge_bank_statements(
        p_run_id      IN RAW,
        p_cutoff_date IN DATE,
        p_batch_size  IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run     IN BOOLEAN  DEFAULT FALSE,
        p_purge_mode  IN VARCHAR2 DEFAULT 'FULL'
    )
    IS
        l_fd_ids       SYS.ODCINUMBERLIST;
        l_dd_ids       SYS.ODCINUMBERLIST;
        l_batch_count  NUMBER := 0;
        l_total_fd     NUMBER := 0;
        l_total_dd     NUMBER := 0;
        l_rows_deleted NUMBER;
        l_start_ts     TIMESTAMP;
        l_batch_start  TIMESTAMP;
        l_has_dd       BOOLEAN;
        l_has_fd       BOOLEAN;

        l_refcur       SYS_REFCURSOR;
    BEGIN
        l_start_ts := SYSTIMESTAMP;

        -- Check table existence
        l_has_dd := tbl_exists('OPPAYMENTS', 'DIRECTORY_DISPATCHING');
        l_has_fd := tbl_exists('OPPAYMENTS', 'FILE_DISPATCHING');

        IF NOT l_has_fd OR NOT l_has_dd THEN
            log_entry(p_run_id => p_run_id, p_module => 'BANK_STATEMENTS',
                p_operation => 'DELETE', p_status => 'WARNING',
                p_message => 'Skipped: required tables do not exist'
                    || CASE WHEN NOT l_has_fd THEN ' (file_dispatching)' ELSE '' END
                    || CASE WHEN NOT l_has_dd THEN ' (directory_dispatching)' ELSE '' END);
            DBMS_OUTPUT.PUT_LINE('bank_statements: skipped (required tables do not exist)');
            RETURN;
        END IF;

        log_entry(
            p_run_id    => p_run_id,
            p_module    => 'BANK_STATEMENTS',
            p_operation => 'DELETE',
            p_status    => 'INFO',
            p_message   => 'Starting bank statements purge. Cutoff date: '
                           || TO_CHAR(p_cutoff_date, 'YYYY-MM-DD')
        );

        IF p_dry_run THEN
            EXECUTE IMMEDIATE
                'SELECT COUNT(*)
                 FROM oppayments.directory_dispatching dd
                 INNER JOIN oppayments.file_dispatching fd
                     ON fd.file_dispatching_id = dd.file_dispatching_id
                 WHERE fd.date_reception < :1'
                INTO l_total_dd USING p_cutoff_date;

            EXECUTE IMMEDIATE
                'SELECT COUNT(*) FROM oppayments.file_dispatching
                 WHERE date_reception < :1'
                INTO l_total_fd USING p_cutoff_date;

            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'BANK_STATEMENTS',
                p_operation     => 'DRY_RUN_COUNT',
                p_status        => 'SUCCESS',
                p_message       => 'Dry run: ' || l_total_dd || ' directory_dispatching + '
                                   || l_total_fd || ' file_dispatching '
                                   || CASE WHEN p_purge_mode = C_MODE_FULL
                                           THEN 'rows would be deleted'
                                           ELSE 'CLOBs would be cleared' END,
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('[DRY RUN] directory_dispatching: ' || l_total_dd
                || ', file_dispatching: ' || l_total_fd
                || CASE WHEN p_purge_mode = C_MODE_FULL
                        THEN ' rows would be deleted'
                        ELSE ' CLOBs would be cleared' END);
            RETURN;
        END IF;

        OPEN l_refcur FOR
            'SELECT fd.file_dispatching_id, dd.directory_dispatching_id
             FROM oppayments.directory_dispatching dd
             INNER JOIN oppayments.file_dispatching fd
                 ON fd.file_dispatching_id = dd.file_dispatching_id
             WHERE fd.date_reception < :1
             ORDER BY fd.file_dispatching_id'
            USING p_cutoff_date;
        LOOP
            l_batch_start := SYSTIMESTAMP;
            l_batch_count := l_batch_count + 1;

            FETCH l_refcur BULK COLLECT INTO l_fd_ids, l_dd_ids
                LIMIT p_batch_size;

            EXIT WHEN l_dd_ids.COUNT = 0;

            IF p_purge_mode = C_MODE_FULL THEN
                -- FULL mode: delete entire rows

                -- Delete directory_dispatching first (child table)
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.directory_dispatching
                     WHERE directory_dispatching_id IN (
                         SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_dd_ids;

                l_rows_deleted := SQL%ROWCOUNT;
                l_total_dd := l_total_dd + l_rows_deleted;

                -- Delete file_dispatching (parent table)
                -- Deduplication: only delete if no remaining directory_dispatching rows
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.file_dispatching
                     WHERE file_dispatching_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))
                       AND NOT EXISTS (
                           SELECT 1 FROM oppayments.directory_dispatching dd2
                           WHERE dd2.file_dispatching_id = oppayments.file_dispatching.file_dispatching_id
                       )'
                    USING l_fd_ids;

                l_total_fd := l_total_fd + SQL%ROWCOUNT;

            ELSE
                -- CLOB_ONLY / CLOB_N_LOGS: dynamically discover and clear
                -- all LOB columns on both bank-statement tables, keep rows.

                l_rows_deleted := clear_table_clobs(
                    p_run_id, 'BANK_STATEMENTS', 'OPPAYMENTS',
                    'DIRECTORY_DISPATCHING', 'DIRECTORY_DISPATCHING_ID', l_dd_ids);
                l_total_dd := l_total_dd + l_rows_deleted;

                l_rows_deleted := clear_table_clobs(
                    p_run_id, 'BANK_STATEMENTS', 'OPPAYMENTS',
                    'FILE_DISPATCHING', 'FILE_DISPATCHING_ID', l_fd_ids);
                l_total_fd := l_total_fd + l_rows_deleted;

            END IF;

            -- Single per-batch log entry
            log_entry(
                p_run_id          => p_run_id,
                p_module          => 'BANK_STATEMENTS',
                p_operation       => CASE WHEN p_purge_mode = C_MODE_FULL
                                         THEN 'DELETE' ELSE 'CLOB_CLEAR' END,
                p_rows_affected   => l_rows_deleted,
                p_batch_number    => l_batch_count,
                p_status          => 'SUCCESS',
                p_elapsed_seconds => get_elapsed_seconds(l_batch_start, SYSTIMESTAMP)
            );

            COMMIT;
        END LOOP;
        CLOSE l_refcur;

        log_entry(
            p_run_id          => p_run_id,
            p_module          => 'BANK_STATEMENTS',
            p_operation       => CASE WHEN p_purge_mode = C_MODE_FULL
                                     THEN 'DELETE' ELSE 'CLOB_CLEAR' END,
            p_status          => 'SUCCESS',
            p_message         => 'Completed. directory_dispatching: ' || l_total_dd
                                 || ', file_dispatching: ' || l_total_fd
                                 || CASE WHEN p_purge_mode = C_MODE_FULL
                                         THEN ' rows deleted'
                                         ELSE ' CLOBs cleared' END,
            p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
        );

        DBMS_OUTPUT.PUT_LINE('directory_dispatching: ' || l_total_dd
            || ', file_dispatching: ' || l_total_fd
            || CASE WHEN p_purge_mode = C_MODE_FULL
                    THEN ' rows deleted' ELSE ' CLOBs cleared' END);

    EXCEPTION
        WHEN OTHERS THEN
            IF l_refcur%ISOPEN THEN CLOSE l_refcur; END IF;
            ROLLBACK;
            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'BANK_STATEMENTS',
                p_operation     => 'DELETE',
                p_status        => 'ERROR',
                p_error_code    => SQLCODE,
                p_error_message => SQLERRM,
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('ERROR in purge_bank_statements: ' || SQLERRM);
    END purge_bank_statements;

    -- ========================================================================
    -- purge_bulk_payments
    -- ========================================================================
    -- The most complex module. Purges bulk payments and all 21 dependent
    -- child tables in correct FK-dependency order using BULK COLLECT + FORALL.
    --
    -- Performance optimizations (v2):
    --   1. payment_ids are collected ONCE per batch into a nested table
    --      (oppayments.epf_number_tab) and reused across all 7 payment-level
    --      deletes — eliminates 6 redundant joins per batch.
    --   2. Default batch size raised to 5000 (was 1000).
    --   3. DBMS_APPLICATION_INFO updates V$SESSION every batch so progress is
    --      visible instantly from another session without querying the log table.
    PROCEDURE purge_bulk_payments(
        p_run_id      IN RAW,
        p_cutoff_date IN DATE,
        p_batch_size  IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run     IN BOOLEAN  DEFAULT FALSE,
        p_purge_mode  IN VARCHAR2 DEFAULT 'FULL'
    )
    IS
        l_bp_ids       SYS.ODCINUMBERLIST;           -- bulk_payment_id values for current batch
        l_pay_ids      oppayments.epf_number_tab;    -- payment_ids materialised ONCE per batch
        l_batch_count  NUMBER := 0;
        l_rows_deleted NUMBER;
        l_start_ts     TIMESTAMP;
        l_batch_start  TIMESTAMP;

        -- Per-table running totals
        l_tot_bp_addl  NUMBER := 0;
        l_tot_bulk_sig NUMBER := 0;
        l_tot_mand_sign NUMBER := 0;
        l_tot_oidc_req NUMBER := 0;
        l_tot_pay_aud1 NUMBER := 0;  -- payment_audit by bulk_payment_id
        l_tot_tx_aud   NUMBER := 0;
        l_tot_imp_msg  NUMBER := 0;
        l_tot_imp_aud  NUMBER := 0;
        l_tot_notif    NUMBER := 0;
        l_tot_tx_exec  NUMBER := 0;
        l_tot_tx_exc   NUMBER := 0;
        l_tot_approv_opt NUMBER := 0;
        l_tot_wf_opt   NUMBER := 0;
        l_tot_approv   NUMBER := 0;
        l_tot_wf_exec  NUMBER := 0;
        l_tot_pay_aud2 NUMBER := 0;  -- payment_audit by payment_id
        l_tot_bp_exc   NUMBER := 0;
        l_tot_inv_addl NUMBER := 0;
        l_tot_invoice  NUMBER := 0;
        l_tot_pay_addl NUMBER := 0;
        l_tot_payment  NUMBER := 0;
        l_tot_bp       NUMBER := 0;

        -- Table existence flags (checked once before loop)
        l_has_bp           BOOLEAN;
        l_has_payment      BOOLEAN;
        l_has_bp_addl      BOOLEAN;
        l_has_bulk_sig     BOOLEAN;
        l_has_mand_sign    BOOLEAN;
        l_has_oidc_req     BOOLEAN;
        l_has_pay_aud      BOOLEAN;
        l_has_tx_aud       BOOLEAN;
        l_has_imp_msg      BOOLEAN;
        l_has_imp_aud      BOOLEAN;
        l_has_notif        BOOLEAN;
        l_has_tx_exec      BOOLEAN;
        l_has_tx_exc       BOOLEAN;
        l_has_approv_opt   BOOLEAN;
        l_has_wf_opt       BOOLEAN;
        l_has_approv       BOOLEAN;
        l_has_wf_exec      BOOLEAN;
        l_has_bp_exc       BOOLEAN;
        l_has_inv_addl     BOOLEAN;
        l_has_invoice      BOOLEAN;
        l_has_pay_addl     BOOLEAN;

        l_refcur       SYS_REFCURSOR;
    BEGIN
        l_start_ts := SYSTIMESTAMP;

        -- Check anchor table existence
        l_has_bp := tbl_exists('OPPAYMENTS', 'BULK_PAYMENT');
        IF NOT l_has_bp THEN
            log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS',
                p_operation => 'DELETE', p_status => 'WARNING',
                p_message => 'Skipped: table oppayments.bulk_payment does not exist');
            DBMS_OUTPUT.PUT_LINE('payments: skipped (bulk_payment table does not exist)');
            RETURN;
        END IF;

        -- Check all child table existence (once, before the loop)
        l_has_payment   := tbl_exists('OPPAYMENTS', 'PAYMENT');
        l_has_bp_addl   := tbl_exists('OPPAYMENTS', 'BULK_PAYMENT_ADDITIONAL_INFO');
        l_has_bulk_sig  := tbl_exists('OPPAYMENTS', 'BULK_SIGNATURE');
        l_has_mand_sign := tbl_exists('OPPAYMENTS', 'MANDATORY_SIGNERS');
        l_has_oidc_req  := tbl_exists('OPPAYMENTS', 'OIDC_REQUEST_TOKEN');
        l_has_pay_aud   := tbl_exists('OPPAYMENTS', 'PAYMENT_AUDIT');
        l_has_tx_aud    := tbl_exists('OPPAYMENTS', 'TRANSMISSION_EXECUTION_AUDIT');
        l_has_imp_msg   := tbl_exists('OPPAYMENTS', 'IMPORT_AUDIT_MESSAGES');
        l_has_imp_aud   := tbl_exists('OPPAYMENTS', 'IMPORT_AUDIT');
        l_has_notif     := tbl_exists('OPPAYMENTS', 'NOTIFICATION_EXECUTION');
        l_has_tx_exec   := tbl_exists('OPPAYMENTS', 'TRANSMISSION_EXECUTION');
        l_has_tx_exc    := tbl_exists('OPPAYMENTS', 'TRANSMISSION_EXCEPTION');
        l_has_approv_opt := tbl_exists('OPPAYMENTS', 'APPROBATION_EXECUTION_OPT');
        l_has_wf_opt    := tbl_exists('OPPAYMENTS', 'WORKFLOW_EXECUTION_OPT');
        l_has_approv    := tbl_exists('OPPAYMENTS', 'APPROBATION_EXECUTION');
        l_has_wf_exec   := tbl_exists('OPPAYMENTS', 'WORKFLOW_EXECUTION');
        l_has_bp_exc    := tbl_exists('OPPAYMENTS', 'BULKPAYMENT_EXCEPTION');
        l_has_inv_addl  := tbl_exists('OPPAYMENTS', 'INVOICE_ADDITIONAL_INFO');
        l_has_invoice   := tbl_exists('OPPAYMENTS', 'INVOICE');
        l_has_pay_addl  := tbl_exists('OPPAYMENTS', 'PAYMENT_ADDITIONAL_INFO');

        log_entry(
            p_run_id    => p_run_id,
            p_module    => 'PAYMENTS',
            p_operation => 'DELETE',
            p_status    => 'INFO',
            p_message   => 'Starting bulk payments purge. Cutoff date: '
                           || TO_CHAR(p_cutoff_date, 'YYYY-MM-DD')
        );

        -- Dry run: count affected rows without deleting
        IF p_dry_run THEN
            EXECUTE IMMEDIATE
                'SELECT COUNT(*) FROM oppayments.bulk_payment
                 WHERE value_date < :1'
                INTO l_tot_bp USING p_cutoff_date;

            IF l_has_payment THEN
                EXECUTE IMMEDIATE
                    'SELECT COUNT(*) FROM oppayments.payment p
                     WHERE EXISTS (
                         SELECT 1 FROM oppayments.bulk_payment bp
                         WHERE bp.bulk_payment_id = p.bulk_payment_id
                           AND bp.value_date < :1
                     )'
                    INTO l_tot_payment USING p_cutoff_date;
            END IF;

            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'PAYMENTS',
                p_operation     => 'DRY_RUN_COUNT',
                p_status        => 'SUCCESS',
                p_message       => 'Dry run: ' || l_tot_bp || ' bulk_payment + '
                                   || l_tot_payment || ' payment rows (and dependents) would be deleted',
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('[DRY RUN] bulk_payment: ' || l_tot_bp
                || ', payment: ' || l_tot_payment || ' (and dependents) would be deleted');
            RETURN;
        END IF;

        OPEN l_refcur FOR
            'SELECT bp.bulk_payment_id
             FROM oppayments.bulk_payment bp
             WHERE bp.value_date < :1
             ORDER BY bp.bulk_payment_id'
            USING p_cutoff_date;
        LOOP
            l_batch_start := SYSTIMESTAMP;
            l_batch_count := l_batch_count + 1;

            -- Collect a batch of bulk_payment_ids
            FETCH l_refcur BULK COLLECT INTO l_bp_ids
                LIMIT p_batch_size;

            EXIT WHEN l_bp_ids.COUNT = 0;

            -- =============================================================
            -- OPTIMIZATION: Materialise payment_ids ONCE per batch.
            -- Avoids repeating the payment join 7 times per batch.
            -- Uses oppayments.epf_number_tab (schema-level nested table)
            -- so there is no VARRAY 32K limit.
            -- =============================================================
            IF l_has_payment THEN
                EXECUTE IMMEDIATE
                    'SELECT payment_id FROM oppayments.payment
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    BULK COLLECT INTO l_pay_ids USING l_bp_ids;
            ELSE
                l_pay_ids := oppayments.epf_number_tab();
            END IF;

            IF p_purge_mode = C_MODE_FULL THEN
            -- =============================================================
            -- FULL mode: DELETE in FK-dependency order (leaves first, root last)
            -- All DML uses dynamic SQL so the package compiles even if some
            -- tables do not exist in this EPF schema version.
            -- =============================================================

            -- 1. bulk_payment_additional_info (FK to bulk_payment)
            IF l_has_bp_addl THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.bulk_payment_additional_info
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_bp_addl := l_tot_bp_addl + SQL%ROWCOUNT;
            END IF;

            -- 2. bulk_signature (FK to bulk_payment)
            IF l_has_bulk_sig THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.bulk_signature
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_bulk_sig := l_tot_bulk_sig + SQL%ROWCOUNT;
            END IF;

            -- 3. mandatory_signers (FK to bulk_payment)
            IF l_has_mand_sign THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.mandatory_signers
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_mand_sign := l_tot_mand_sign + SQL%ROWCOUNT;
            END IF;

            -- 4. oidc_request_token (FK to bulk_payment)
            IF l_has_oidc_req THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.oidc_request_token
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_oidc_req := l_tot_oidc_req + SQL%ROWCOUNT;
            END IF;

            -- 5. payment_audit (by bulk_payment_id)
            IF l_has_pay_aud THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.payment_audit
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_pay_aud1 := l_tot_pay_aud1 + SQL%ROWCOUNT;
            END IF;

            -- 6. transmission_execution_audit (FK to transmission_execution
            --    AND possibly import_audit via IMPORT_AUDIT_ID_FK)
            --    Must be deleted BEFORE both import_audit and transmission_execution
            IF l_has_tx_aud THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.transmission_execution_audit
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_tx_aud := l_tot_tx_aud + SQL%ROWCOUNT;
            END IF;

            -- 7. import_audit_messages (FK to import_audit via IMPORT_AUDIT_ID_FK)
            --    Must be deleted BEFORE import_audit
            IF l_has_imp_msg AND l_has_imp_aud THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.import_audit_messages
                     WHERE import_audit_id IN (
                         SELECT import_audit_id FROM oppayments.import_audit
                         WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))
                     )'
                    USING l_bp_ids;
                l_tot_imp_msg := l_tot_imp_msg + SQL%ROWCOUNT;
            END IF;

            -- 7b. notification_execution (FK to import_audit AND transmission_execution)
            --    Must be deleted BEFORE both import_audit and transmission_execution
            IF l_has_notif THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.notification_execution
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_notif := l_tot_notif + SQL%ROWCOUNT;
            END IF;

            -- 8. import_audit (FK to bulk_payment)
            --    Now safe: import_audit_messages + notification_execution + tex_audit deleted
            IF l_has_imp_aud THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.import_audit
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_imp_aud := l_tot_imp_aud + SQL%ROWCOUNT;
            END IF;

            -- 9. transmission_execution (FK to bulk_payment AND transmission_exception)
            --    Now safe: notification_execution + tex_audit already deleted
            IF l_has_tx_exec THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.transmission_execution
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_tx_exec := l_tot_tx_exec + SQL%ROWCOUNT;
            END IF;

            -- 10. transmission_exception (FK to bulk_payment)
            IF l_has_tx_exc THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.transmission_exception
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_tx_exc := l_tot_tx_exc + SQL%ROWCOUNT;
            END IF;

            -- 11. approbation_execution_opt (FK to workflow_execution_opt)
            IF l_has_approv_opt AND l_has_wf_opt THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.approbation_execution_opt
                     WHERE execution_id IN (
                         SELECT execution_id FROM oppayments.workflow_execution_opt
                         WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))
                     )' USING l_bp_ids;
                l_tot_approv_opt := l_tot_approv_opt + SQL%ROWCOUNT;
            END IF;

            -- 12. workflow_execution_opt (FK to bulk_payment)
            IF l_has_wf_opt THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.workflow_execution_opt
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_wf_opt := l_tot_wf_opt + SQL%ROWCOUNT;
            END IF;

            -- ============================================================
            -- Payment-level deletes: use materialised l_pay_ids
            -- (eliminates 6 redundant joins to the payment table)
            -- ============================================================

            -- 13. approbation_execution (FK to workflow_execution)
            IF l_has_approv AND l_has_wf_exec AND l_pay_ids.COUNT > 0 THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.approbation_execution
                     WHERE execution_id IN (
                         SELECT we.execution_id
                         FROM oppayments.workflow_execution we
                         WHERE we.payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))
                     )'
                    USING l_pay_ids;
                l_tot_approv := l_tot_approv + SQL%ROWCOUNT;
            END IF;

            -- 14. workflow_execution (via payment)
            IF l_has_wf_exec AND l_pay_ids.COUNT > 0 THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.workflow_execution
                     WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_pay_ids;
                l_tot_wf_exec := l_tot_wf_exec + SQL%ROWCOUNT;
            END IF;

            -- 15. payment_audit (indirect: via payment_id)
            IF l_has_pay_aud AND l_pay_ids.COUNT > 0 THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.payment_audit
                     WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_pay_ids;
                l_tot_pay_aud2 := l_tot_pay_aud2 + SQL%ROWCOUNT;
            END IF;

            -- 16. bulkpayment_exception (FK to payment)
            IF l_has_bp_exc AND l_pay_ids.COUNT > 0 THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.bulkpayment_exception
                     WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_pay_ids;
                l_tot_bp_exc := l_tot_bp_exc + SQL%ROWCOUNT;
            END IF;

            -- 17. invoice_additional_info (FK to invoice)
            IF l_has_inv_addl AND l_has_invoice AND l_pay_ids.COUNT > 0 THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.invoice_additional_info
                     WHERE invoice_id IN (
                         SELECT invoice_id FROM oppayments.invoice
                         WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))
                     )'
                    USING l_pay_ids;
                l_tot_inv_addl := l_tot_inv_addl + SQL%ROWCOUNT;
            END IF;

            -- 18. invoice (FK to payment)
            IF l_has_invoice AND l_pay_ids.COUNT > 0 THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.invoice
                     WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_pay_ids;
                l_tot_invoice := l_tot_invoice + SQL%ROWCOUNT;
            END IF;

            -- 19. payment_additional_info (via payment)
            IF l_has_pay_addl AND l_pay_ids.COUNT > 0 THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.payment_additional_info
                     WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_pay_ids;
                l_tot_pay_addl := l_tot_pay_addl + SQL%ROWCOUNT;
            END IF;

            -- 20. payment (mid-level)
            IF l_has_payment THEN
                EXECUTE IMMEDIATE
                    'DELETE FROM oppayments.payment
                     WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                    USING l_bp_ids;
                l_tot_payment := l_tot_payment + SQL%ROWCOUNT;
            END IF;

            -- 21. bulk_payment (root)
            EXECUTE IMMEDIATE
                'DELETE FROM oppayments.bulk_payment
                 WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                USING l_bp_ids;
            l_tot_bp := l_tot_bp + SQL%ROWCOUNT;

            ELSE
            -- =============================================================
            -- CLOB_ONLY / CLOB_N_LOGS: dynamically discover and clear all
            -- LOB columns across PAYMENTS module tables, keep rows.
            -- =============================================================

            -- Tables with direct bulk_payment_id column
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'BULK_PAYMENT',
                'BULK_PAYMENT_ID', l_bp_ids);
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'BULK_PAYMENT_ADDITIONAL_INFO',
                'BULK_PAYMENT_ID', l_bp_ids);
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'BULK_SIGNATURE',
                'BULK_PAYMENT_ID', l_bp_ids);
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'MANDATORY_SIGNERS',
                'BULK_PAYMENT_ID', l_bp_ids);
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'OIDC_REQUEST_TOKEN',
                'BULK_PAYMENT_ID', l_bp_ids);
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'PAYMENT_AUDIT',
                'BULK_PAYMENT_ID', l_bp_ids);
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'TRANSMISSION_EXECUTION_AUDIT',
                'BULK_PAYMENT_ID', l_bp_ids);
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'IMPORT_AUDIT',
                'BULK_PAYMENT_ID', l_bp_ids);
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'TRANSMISSION_EXECUTION',
                'BULK_PAYMENT_ID', l_bp_ids);
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'TRANSMISSION_EXCEPTION',
                'BULK_PAYMENT_ID', l_bp_ids);
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'NOTIFICATION_EXECUTION',
                'BULK_PAYMENT_ID', l_bp_ids);
            l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'WORKFLOW_EXECUTION_OPT',
                'BULK_PAYMENT_ID', l_bp_ids);

            -- Tables with direct payment_id column (use materialised pay_ids)
            IF l_pay_ids.COUNT > 0 THEN
                l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                    p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'PAYMENT',
                    'PAYMENT_ID', l_pay_ids);
                l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                    p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'PAYMENT_ADDITIONAL_INFO',
                    'PAYMENT_ID', l_pay_ids);
                l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                    p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'WORKFLOW_EXECUTION',
                    'PAYMENT_ID', l_pay_ids);
                l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                    p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'BULKPAYMENT_EXCEPTION',
                    'PAYMENT_ID', l_pay_ids);
                l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                    p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'INVOICE',
                    'PAYMENT_ID', l_pay_ids);
            END IF;

            -- Tables with indirect FK paths (via import_audit, workflow_execution, or invoice)
            IF l_has_imp_aud AND l_has_imp_msg THEN
                DECLARE
                    l_imp_ids SYS.ODCINUMBERLIST;
                BEGIN
                    EXECUTE IMMEDIATE
                        'SELECT import_audit_id FROM oppayments.import_audit
                         WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                        BULK COLLECT INTO l_imp_ids USING l_bp_ids;
                    l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                        p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'IMPORT_AUDIT_MESSAGES',
                        'IMPORT_AUDIT_ID', l_imp_ids);
                END;
            END IF;

            IF l_has_approv_opt AND l_has_wf_opt THEN
                DECLARE
                    l_exec_ids SYS.ODCINUMBERLIST;
                BEGIN
                    EXECUTE IMMEDIATE
                        'SELECT execution_id FROM oppayments.workflow_execution_opt
                         WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                        BULK COLLECT INTO l_exec_ids USING l_bp_ids;
                    l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                        p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'APPROBATION_EXECUTION_OPT',
                        'EXECUTION_ID', l_exec_ids);
                END;
            END IF;

            IF l_has_approv AND l_has_wf_exec AND l_pay_ids.COUNT > 0 THEN
                DECLARE
                    l_exec_ids SYS.ODCINUMBERLIST;
                BEGIN
                    EXECUTE IMMEDIATE
                        'SELECT execution_id FROM oppayments.workflow_execution
                         WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                        BULK COLLECT INTO l_exec_ids USING l_pay_ids;
                    l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                        p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'APPROBATION_EXECUTION',
                        'EXECUTION_ID', l_exec_ids);
                END;
            END IF;

            IF l_has_inv_addl AND l_has_invoice AND l_pay_ids.COUNT > 0 THEN
                DECLARE
                    l_inv_ids SYS.ODCINUMBERLIST;
                BEGIN
                    EXECUTE IMMEDIATE
                        'SELECT invoice_id FROM oppayments.invoice
                         WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(:1))'
                        BULK COLLECT INTO l_inv_ids USING l_pay_ids;
                    l_tot_tx_aud := l_tot_tx_aud + clear_table_clobs(
                        p_run_id, 'PAYMENTS', 'OPPAYMENTS', 'INVOICE_ADDITIONAL_INFO',
                        'INVOICE_ID', l_inv_ids);
                END;
            END IF;

            END IF; -- p_purge_mode

            COMMIT;

            -- Update V$SESSION so progress is visible from another session
            -- via: SELECT module, action, client_info FROM v$session WHERE ...
            DBMS_APPLICATION_INFO.SET_MODULE(
                module_name => 'EPF_PURGE',
                action_name => 'PAYMENTS batch ' || l_batch_count
            );
            DBMS_APPLICATION_INFO.SET_CLIENT_INFO(
                'bp=' || l_tot_bp || ' pay=' || l_tot_payment
                || ' elapsed=' || ROUND(get_elapsed_seconds(l_start_ts, SYSTIMESTAMP), 0) || 's'
            );

            -- Log every batch (autonomous transaction = immediately queryable
            -- from another session via epf_purge_log)
            log_entry(
                p_run_id          => p_run_id,
                p_module          => 'PAYMENTS',
                p_operation       => CASE WHEN p_purge_mode = C_MODE_FULL
                                         THEN 'DELETE' ELSE 'CLOB_CLEAR' END,
                p_table_name      => 'oppayments.bulk_payment (batch)',
                p_rows_affected   => l_bp_ids.COUNT,
                p_batch_number    => l_batch_count,
                p_status          => 'SUCCESS',
                p_message         => 'Batch ' || l_batch_count || ': '
                                     || CASE WHEN p_purge_mode = C_MODE_FULL
                                             THEN l_tot_bp || ' bulk_payment, '
                                                  || l_tot_payment || ' payment deleted so far'
                                             ELSE l_tot_tx_aud || ' CLOBs cleared so far'
                                        END,
                p_elapsed_seconds => get_elapsed_seconds(l_batch_start, SYSTIMESTAMP)
            );
        END LOOP;
        CLOSE l_refcur;

        -- Clear V$SESSION info
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
        DBMS_APPLICATION_INFO.SET_CLIENT_INFO(NULL);

        -- Log per-table totals
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.bulk_payment_additional_info',
            p_rows_affected => l_tot_bp_addl, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.bulk_signature',
            p_rows_affected => l_tot_bulk_sig, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.mandatory_signers',
            p_rows_affected => l_tot_mand_sign, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.oidc_request_token',
            p_rows_affected => l_tot_oidc_req, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.payment_audit (by bulk_payment_id)',
            p_rows_affected => l_tot_pay_aud1, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.transmission_execution_audit',
            p_rows_affected => l_tot_tx_aud, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.import_audit_messages',
            p_rows_affected => l_tot_imp_msg, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.import_audit',
            p_rows_affected => l_tot_imp_aud, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.notification_execution',
            p_rows_affected => l_tot_notif, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.transmission_execution',
            p_rows_affected => l_tot_tx_exec, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.transmission_exception',
            p_rows_affected => l_tot_tx_exc, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.approbation_execution_opt',
            p_rows_affected => l_tot_approv_opt, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.workflow_execution_opt',
            p_rows_affected => l_tot_wf_opt, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.approbation_execution',
            p_rows_affected => l_tot_approv, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.workflow_execution',
            p_rows_affected => l_tot_wf_exec, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.payment_audit (by payment_id)',
            p_rows_affected => l_tot_pay_aud2, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.bulkpayment_exception',
            p_rows_affected => l_tot_bp_exc, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.invoice_additional_info',
            p_rows_affected => l_tot_inv_addl, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.invoice',
            p_rows_affected => l_tot_invoice, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.payment_additional_info',
            p_rows_affected => l_tot_pay_addl, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.payment',
            p_rows_affected => l_tot_payment, p_status => 'SUCCESS');
        log_entry(p_run_id => p_run_id, p_module => 'PAYMENTS', p_operation => 'DELETE',
            p_table_name => 'oppayments.bulk_payment',
            p_rows_affected => l_tot_bp, p_status => 'SUCCESS');

        -- Final summary log
        log_entry(
            p_run_id          => p_run_id,
            p_module          => 'PAYMENTS',
            p_operation       => CASE WHEN p_purge_mode = C_MODE_FULL
                                     THEN 'DELETE' ELSE 'CLOB_CLEAR' END,
            p_status          => 'SUCCESS',
            p_message         => 'Completed. '
                                 || CASE WHEN p_purge_mode = C_MODE_FULL
                                         THEN 'bulk_payment: ' || l_tot_bp
                                              || ', payment: ' || l_tot_payment
                                              || ' (+ all dependent records across 21 tables)'
                                         ELSE 'tx_execution_audit: ' || l_tot_tx_aud
                                              || ' CLOBs cleared'
                                    END,
            p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
        );

        IF p_purge_mode = C_MODE_FULL THEN
            DBMS_OUTPUT.PUT_LINE('=== Bulk Payments Purge Summary ===');
            DBMS_OUTPUT.PUT_LINE('  bulk_payment:                    ' || l_tot_bp);
            DBMS_OUTPUT.PUT_LINE('  bulk_payment_additional_info:    ' || l_tot_bp_addl);
            DBMS_OUTPUT.PUT_LINE('  bulk_signature:                  ' || l_tot_bulk_sig);
            DBMS_OUTPUT.PUT_LINE('  mandatory_signers:               ' || l_tot_mand_sign);
            DBMS_OUTPUT.PUT_LINE('  oidc_request_token:              ' || l_tot_oidc_req);
            DBMS_OUTPUT.PUT_LINE('  payment:                         ' || l_tot_payment);
            DBMS_OUTPUT.PUT_LINE('  payment_additional_info:         ' || l_tot_pay_addl);
            DBMS_OUTPUT.PUT_LINE('  payment_audit (by bp_id):        ' || l_tot_pay_aud1);
            DBMS_OUTPUT.PUT_LINE('  payment_audit (by payment_id):   ' || l_tot_pay_aud2);
            DBMS_OUTPUT.PUT_LINE('  import_audit_messages:            ' || l_tot_imp_msg);
            DBMS_OUTPUT.PUT_LINE('  import_audit:                    ' || l_tot_imp_aud);
            DBMS_OUTPUT.PUT_LINE('  transmission_execution_audit:    ' || l_tot_tx_aud);
            DBMS_OUTPUT.PUT_LINE('  transmission_execution:          ' || l_tot_tx_exec);
            DBMS_OUTPUT.PUT_LINE('  transmission_exception:          ' || l_tot_tx_exc);
            DBMS_OUTPUT.PUT_LINE('  notification_execution:          ' || l_tot_notif);
            DBMS_OUTPUT.PUT_LINE('  workflow_execution:              ' || l_tot_wf_exec);
            DBMS_OUTPUT.PUT_LINE('  workflow_execution_opt:          ' || l_tot_wf_opt);
            DBMS_OUTPUT.PUT_LINE('  approbation_execution:           ' || l_tot_approv);
            DBMS_OUTPUT.PUT_LINE('  approbation_execution_opt:       ' || l_tot_approv_opt);
            DBMS_OUTPUT.PUT_LINE('  bulkpayment_exception:           ' || l_tot_bp_exc);
            DBMS_OUTPUT.PUT_LINE('  invoice_additional_info:         ' || l_tot_inv_addl);
            DBMS_OUTPUT.PUT_LINE('  invoice:                         ' || l_tot_invoice);
        ELSE
            DBMS_OUTPUT.PUT_LINE('=== Bulk Payments CLOB Clear Summary ===');
            DBMS_OUTPUT.PUT_LINE('  transmission_execution_audit:    ' || l_tot_tx_aud);
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            IF l_refcur%ISOPEN THEN CLOSE l_refcur; END IF;
            ROLLBACK;
            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'PAYMENTS',
                p_operation     => 'DELETE',
                p_status        => 'ERROR',
                p_error_code    => SQLCODE,
                p_error_message => SQLERRM,
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('ERROR in purge_bulk_payments: ' || SQLERRM);
    END purge_bulk_payments;

    -- ========================================================================
    -- reclaim_space (REMOVED)
    -- ========================================================================
    -- The in-package SHRINK/COALESCE/RESIZE approach did not reliably return
    -- disk space to the OS. Disk reclamation is now handled by the standalone
    -- bin/epf_tablespace_reclaim.{sh,bat} tool (export/import/recreate-as-
    -- BIGFILE). The procedure and its helpers have been removed.
    /* -- BEGIN REMOVED reclaim_space
    PROCEDURE reclaim_space(
        p_run_id           IN RAW,
        p_shrink_tables    IN BOOLEAN  DEFAULT TRUE,
        p_coalesce_ts      IN BOOLEAN  DEFAULT FALSE,
        p_resize_datafiles IN BOOLEAN  DEFAULT FALSE
    )
    IS
        l_tables     t_table_list;
        l_start_ts   TIMESTAMP;
        l_tbl_start  TIMESTAMP;
        l_ts_name    VARCHAR2(128);

        CURSOR c_tablespaces IS
            SELECT DISTINCT tablespace_name
            FROM user_tables
            WHERE table_name IN (
                'BULK_PAYMENT', 'BULK_PAYMENT_ADDITIONAL_INFO', 'PAYMENT',
                'PAYMENT_ADDITIONAL_INFO', 'PAYMENT_AUDIT', 'IMPORT_AUDIT',
                'TRANSMISSION_EXECUTION_AUDIT', 'TRANSMISSION_EXECUTION',
                'TRANSMISSION_EXCEPTION', 'NOTIFICATION_EXECUTION',
                'APPROBATION_EXECUTION', 'WORKFLOW_EXECUTION',
                'FILE_INTEGRATION', 'AUDIT_ARCHIVE', 'AUDIT_TRAIL',
                'DIRECTORY_DISPATCHING', 'FILE_DISPATCHING'
            );
    BEGIN
        l_start_ts := SYSTIMESTAMP;
        l_tables := get_purged_tables;

        log_entry(
            p_run_id    => p_run_id,
            p_module    => 'SPACE_RECLAIM',
            p_operation => 'INIT',
            p_status    => 'INFO',
            p_message   => 'Starting space reclamation. shrink='
                           || CASE WHEN p_shrink_tables THEN 'Y' ELSE 'N' END
                           || ', coalesce='
                           || CASE WHEN p_coalesce_ts THEN 'Y' ELSE 'N' END
                           || ', resize='
                           || CASE WHEN p_resize_datafiles THEN 'Y' ELSE 'N' END
        );

        -- Tier 1: SHRINK SPACE on individual tables
        IF p_shrink_tables THEN
            DBMS_OUTPUT.PUT_LINE('=== Space Reclamation: SHRINK SPACE ===');
            FOR i IN 1..l_tables.COUNT LOOP
                l_tbl_start := SYSTIMESTAMP;
                BEGIN
                    EXECUTE IMMEDIATE 'ALTER TABLE ' || l_tables(i) || ' ENABLE ROW MOVEMENT';
                    EXECUTE IMMEDIATE 'ALTER TABLE ' || l_tables(i) || ' SHRINK SPACE CASCADE';

                    log_entry(
                        p_run_id          => p_run_id,
                        p_module          => 'SPACE_RECLAIM',
                        p_operation       => 'SHRINK_SPACE',
                        p_table_name      => l_tables(i),
                        p_status          => 'SUCCESS',
                        p_message         => 'SHRINK SPACE CASCADE completed',
                        p_elapsed_seconds => get_elapsed_seconds(l_tbl_start, SYSTIMESTAMP)
                    );
                    DBMS_OUTPUT.PUT_LINE('  SHRINK OK: ' || l_tables(i));

                EXCEPTION
                    WHEN OTHERS THEN
                        log_entry(
                            p_run_id        => p_run_id,
                            p_module        => 'SPACE_RECLAIM',
                            p_operation     => 'SHRINK_SPACE',
                            p_table_name    => l_tables(i),
                            p_status        => 'WARNING',
                            p_error_code    => SQLCODE,
                            p_error_message => SQLERRM,
                            p_message       => 'SHRINK SPACE failed (may require ASSM tablespace)',
                            p_elapsed_seconds => get_elapsed_seconds(l_tbl_start, SYSTIMESTAMP)
                        );
                        DBMS_OUTPUT.PUT_LINE('  SHRINK FAILED: ' || l_tables(i) || ' - ' || SQLERRM);
                END;
            END LOOP;
        END IF;

        -- Tier 2: COALESCE tablespaces
        IF p_coalesce_ts THEN
            DBMS_OUTPUT.PUT_LINE('=== Space Reclamation: COALESCE ===');
            FOR ts_rec IN c_tablespaces LOOP
                l_tbl_start := SYSTIMESTAMP;
                BEGIN
                    EXECUTE IMMEDIATE 'ALTER TABLESPACE ' || ts_rec.tablespace_name || ' COALESCE';

                    log_entry(
                        p_run_id          => p_run_id,
                        p_module          => 'SPACE_RECLAIM',
                        p_operation       => 'COALESCE',
                        p_table_name      => ts_rec.tablespace_name,
                        p_status          => 'SUCCESS',
                        p_message         => 'COALESCE completed for tablespace',
                        p_elapsed_seconds => get_elapsed_seconds(l_tbl_start, SYSTIMESTAMP)
                    );
                    DBMS_OUTPUT.PUT_LINE('  COALESCE OK: ' || ts_rec.tablespace_name);

                EXCEPTION
                    WHEN OTHERS THEN
                        log_entry(
                            p_run_id        => p_run_id,
                            p_module        => 'SPACE_RECLAIM',
                            p_operation     => 'COALESCE',
                            p_table_name    => ts_rec.tablespace_name,
                            p_status        => 'WARNING',
                            p_error_code    => SQLCODE,
                            p_error_message => SQLERRM,
                            p_elapsed_seconds => get_elapsed_seconds(l_tbl_start, SYSTIMESTAMP)
                        );
                        DBMS_OUTPUT.PUT_LINE('  COALESCE FAILED: ' || ts_rec.tablespace_name
                            || ' - ' || SQLERRM);
                END;
            END LOOP;
        END IF;

        -- Tier 3: RESIZE datafiles (requires DBA privileges)
        -- Uses dynamic SQL because dba_data_files/dba_extents are accessed
        -- via roles, which are not available in static PL/SQL at compile time.
        IF p_resize_datafiles THEN
            DBMS_OUTPUT.PUT_LINE('=== Space Reclamation: RESIZE DATAFILES ===');
            FOR ts_rec IN c_tablespaces LOOP
                l_tbl_start := SYSTIMESTAMP;
                DECLARE
                    l_df_cursor  SYS_REFCURSOR;
                    l_file_name     VARCHAR2(513);
                    l_current_bytes NUMBER;
                    l_hwm_bytes     NUMBER;
                    l_target_bytes  NUMBER;
                BEGIN
                    OPEN l_df_cursor FOR
                        'SELECT df.file_name, df.bytes AS current_bytes,
                                NVL(hwm.hwm_bytes, df.bytes) AS hwm_bytes
                         FROM dba_data_files df
                         LEFT JOIN (
                             SELECT file_id,
                                    MAX(block_id + blocks) * (
                                        SELECT value FROM v$parameter WHERE name = ''db_block_size''
                                    ) AS hwm_bytes
                             FROM dba_extents
                             WHERE tablespace_name = :1
                             GROUP BY file_id
                         ) hwm ON hwm.file_id = df.file_id
                         WHERE df.tablespace_name = :2'
                    USING ts_rec.tablespace_name, ts_rec.tablespace_name;

                    LOOP
                        FETCH l_df_cursor INTO l_file_name, l_current_bytes, l_hwm_bytes;
                        EXIT WHEN l_df_cursor%NOTFOUND;

                        BEGIN
                            -- Add 10% safety margin above high water mark
                            l_target_bytes := GREATEST(
                                l_hwm_bytes * 1.1,
                                1048576  -- minimum 1MB
                            );

                            IF l_target_bytes < l_current_bytes THEN
                                EXECUTE IMMEDIATE 'ALTER DATABASE DATAFILE '''
                                    || l_file_name || ''' RESIZE '
                                    || CEIL(l_target_bytes);

                                log_entry(
                                    p_run_id          => p_run_id,
                                    p_module          => 'SPACE_RECLAIM',
                                    p_operation       => 'RESIZE',
                                    p_table_name      => l_file_name,
                                    p_status          => 'SUCCESS',
                                    p_message         => 'Resized from '
                                        || ROUND(l_current_bytes/1048576) || 'MB to '
                                        || ROUND(l_target_bytes/1048576) || 'MB',
                                    p_elapsed_seconds => get_elapsed_seconds(l_tbl_start, SYSTIMESTAMP)
                                );
                                DBMS_OUTPUT.PUT_LINE('  RESIZE OK: ' || l_file_name
                                    || ' (' || ROUND(l_current_bytes/1048576) || 'MB -> '
                                    || ROUND(l_target_bytes/1048576) || 'MB)');
                            ELSE
                                DBMS_OUTPUT.PUT_LINE('  RESIZE SKIP: ' || l_file_name
                                    || ' (already at or below target size)');
                            END IF;
                        EXCEPTION
                            WHEN OTHERS THEN
                                log_entry(
                                    p_run_id        => p_run_id,
                                    p_module        => 'SPACE_RECLAIM',
                                    p_operation     => 'RESIZE',
                                    p_table_name    => l_file_name,
                                    p_status        => 'WARNING',
                                    p_error_code    => SQLCODE,
                                    p_error_message => SQLERRM,
                                    p_message       => 'RESIZE failed (may require DBA privileges)',
                                    p_elapsed_seconds => get_elapsed_seconds(l_tbl_start, SYSTIMESTAMP)
                                );
                                DBMS_OUTPUT.PUT_LINE('  RESIZE FAILED: ' || l_file_name
                                    || ' - ' || SQLERRM);
                        END;
                    END LOOP;
                    CLOSE l_df_cursor;
                EXCEPTION
                    WHEN OTHERS THEN
                        IF l_df_cursor%ISOPEN THEN CLOSE l_df_cursor; END IF;
                        log_entry(
                            p_run_id        => p_run_id,
                            p_module        => 'SPACE_RECLAIM',
                            p_operation     => 'RESIZE',
                            p_table_name    => ts_rec.tablespace_name,
                            p_status        => 'WARNING',
                            p_error_code    => SQLCODE,
                            p_error_message => SQLERRM,
                            p_message       => 'RESIZE failed for tablespace (may require DBA privileges)',
                            p_elapsed_seconds => get_elapsed_seconds(l_tbl_start, SYSTIMESTAMP)
                        );
                        DBMS_OUTPUT.PUT_LINE('  RESIZE FAILED: ' || ts_rec.tablespace_name
                            || ' - ' || SQLERRM);
                END;
            END LOOP;
        END IF;

        log_entry(
            p_run_id          => p_run_id,
            p_module          => 'SPACE_RECLAIM',
            p_operation       => 'INIT',
            p_status          => 'SUCCESS',
            p_message         => 'Space reclamation completed',
            p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
        );

    EXCEPTION
        WHEN OTHERS THEN
            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'SPACE_RECLAIM',
                p_operation     => 'INIT',
                p_status        => 'ERROR',
                p_error_code    => SQLCODE,
                p_error_message => SQLERRM,
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('ERROR in reclaim_space: ' || SQLERRM);
    END reclaim_space;
    -- END REMOVED reclaim_space */

    -- ========================================================================
    -- print_run_summary
    -- ========================================================================
    -- Queries epf_purge_log for the given run_id and prints a formatted summary.
    PROCEDURE print_run_summary(p_run_id IN RAW)
    IS
        l_total_rows   NUMBER := 0;
        l_total_errors NUMBER := 0;
        l_run_start    TIMESTAMP;
        l_run_end      TIMESTAMP;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('  EPF PURGE RUN SUMMARY');
        DBMS_OUTPUT.PUT_LINE('  Run ID: ' || RAWTOHEX(p_run_id));
        DBMS_OUTPUT.PUT_LINE('============================================================');

        -- Get run time window
        SELECT MIN(log_timestamp), MAX(log_timestamp)
        INTO l_run_start, l_run_end
        FROM oppayments.epf_purge_log
        WHERE run_id = p_run_id;

        DBMS_OUTPUT.PUT_LINE('  Started:  ' || TO_CHAR(l_run_start, 'YYYY-MM-DD HH24:MI:SS'));
        DBMS_OUTPUT.PUT_LINE('  Finished: ' || TO_CHAR(l_run_end, 'YYYY-MM-DD HH24:MI:SS'));
        DBMS_OUTPUT.PUT_LINE('  Duration: '
            || ROUND(get_elapsed_seconds(l_run_start, l_run_end), 1) || 's');
        DBMS_OUTPUT.PUT_LINE('------------------------------------------------------------');

        -- Per-module summary
        -- Include DRY_RUN_COUNT rows so dry-run reports show expected counts
        FOR rec IN (
            SELECT module,
                   SUM(CASE WHEN status IN ('SUCCESS')
                       THEN rows_affected ELSE 0 END) AS total_rows,
                   SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS errors,
                   SUM(CASE WHEN status = 'WARNING' THEN 1 ELSE 0 END) AS warnings,
                   ROUND(MAX(NVL(elapsed_seconds, 0)), 1) AS total_seconds
            FROM oppayments.epf_purge_log
            WHERE run_id = p_run_id
              AND operation NOT IN ('INIT', 'RUN_START', 'RUN_END')
            GROUP BY module
            ORDER BY module
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('  ' || RPAD(rec.module, 20)
                || '  Rows: ' || LPAD(TO_CHAR(rec.total_rows), 10)
                || '  Errors: ' || rec.errors
                || '  Warnings: ' || rec.warnings
                || '  Time: ' || rec.total_seconds || 's');
            l_total_rows := l_total_rows + rec.total_rows;
            l_total_errors := l_total_errors + rec.errors;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('------------------------------------------------------------');
        DBMS_OUTPUT.PUT_LINE('  TOTAL ROWS DELETED: ' || l_total_rows);
        IF l_total_errors > 0 THEN
            DBMS_OUTPUT.PUT_LINE('  *** ERRORS OCCURRED: ' || l_total_errors
                || ' (check epf_purge_log for details) ***');
        END IF;
        DBMS_OUTPUT.PUT_LINE('============================================================');

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR generating run summary: ' || SQLERRM);
    END print_run_summary;

    -- ========================================================================
    -- capture_space_snapshot
    -- ========================================================================
    -- Captures segment sizes for ALL objects in the OPPAYMENTS default
    -- tablespace (not just OPPAYMENTS-owned) so totals match the reclaim
    -- script which also measures by tablespace.  Resolves LOB segment names
    -- to their parent table using dba_lobs.
    -- Falls back to user_segments if DBA views are not accessible (but totals
    -- will then only cover the current user's segments).
    PROCEDURE capture_space_snapshot(
        p_run_id         IN RAW,
        p_snapshot_phase IN VARCHAR2
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        v_tablespace VARCHAR2(128);
        v_dba_sql    VARCHAR2(4000);
        v_spec_trt_count NUMBER := 0;

        -- Module classification CASE expression (kept inline so EXECUTE IMMEDIATE
        -- can use it with bind variables; stored once in a constant for reuse
        -- between the DBA path and the user_segments fallback path).
        c_module_case CONSTANT VARCHAR2(2000) :=
            q'[CASE
                WHEN UPPER(parent_table) IN (
                    'BULK_PAYMENT', 'BULK_PAYMENT_ADDITIONAL_INFO', 'BULK_SIGNATURE',
                    'MANDATORY_SIGNERS', 'OIDC_REQUEST_TOKEN', 'PAYMENT',
                    'PAYMENT_ADDITIONAL_INFO', 'PAYMENT_AUDIT', 'IMPORT_AUDIT',
                    'IMPORT_AUDIT_MESSAGES', 'TRANSMISSION_EXECUTION',
                    'TRANSMISSION_EXECUTION_AUDIT', 'TRANSMISSION_EXCEPTION',
                    'NOTIFICATION_EXECUTION', 'APPROBATION_EXECUTION',
                    'APPROBATION_EXECUTION_OPT', 'WORKFLOW_EXECUTION',
                    'WORKFLOW_EXECUTION_OPT', 'BULKPAYMENT_EXCEPTION',
                    'INVOICE', 'INVOICE_ADDITIONAL_INFO', 'FILE_INTEGRATION'
                ) THEN 'PAYMENTS'
                WHEN UPPER(parent_table) IN ('AUDIT_TRAIL', 'AUDIT_ARCHIVE')
                    THEN 'LOGS'
                WHEN owner = 'OP' AND UPPER(parent_table) = 'SPEC_TRT_LOG'
                    THEN 'LOGS'
                WHEN UPPER(parent_table) IN ('DIRECTORY_DISPATCHING', 'FILE_DISPATCHING')
                    THEN 'BANK_STATEMENTS'
                ELSE 'OTHER'
            END]';
    BEGIN
        -- Determine the OPPAYMENTS default tablespace (user_users needs no DBA grants)
        BEGIN
            SELECT default_tablespace INTO v_tablespace FROM user_users;
        EXCEPTION
            WHEN OTHERS THEN v_tablespace := NULL;
        END;

        -- Build DBA query: capture ALL segments in the tablespace so totals
        -- match the reclaim script (which also measures by tablespace), and
        -- tag each row with its purge module so the comparison report can
        -- group + filter by depth.
        v_dba_sql :=
            'INSERT INTO oppayments.epf_purge_space_snapshot
                (run_id, snapshot_phase, owner, segment_name, segment_type,
                 parent_table, size_bytes, size_mb, module)
            SELECT :run_id, :phase, owner, segment_name, segment_type,
                   parent_table, size_bytes, size_mb, ' || c_module_case || '
            FROM (
                SELECT sg.owner, sg.segment_name, sg.segment_type,
                       COALESCE(l.table_name, sg.segment_name) AS parent_table,
                       sg.total_bytes AS size_bytes,
                       ROUND(sg.total_bytes / 1048576, 2) AS size_mb
                FROM (
                    SELECT owner, segment_name, segment_type, SUM(bytes) AS total_bytes
                    FROM dba_segments
                    WHERE tablespace_name = :ts
                    GROUP BY owner, segment_name, segment_type
                ) sg
                LEFT JOIN (
                    SELECT owner, segment_name, MIN(table_name) AS table_name
                    FROM dba_lobs
                    GROUP BY owner, segment_name
                ) l ON l.owner = sg.owner AND l.segment_name = sg.segment_name
            )';

        DBMS_OUTPUT.PUT_LINE('[SNAPSHOT] Phase: ' || p_snapshot_phase
            || ' | Tablespace: ' || NVL(v_tablespace, '(unknown)'));

        BEGIN
            EXECUTE IMMEDIATE v_dba_sql USING p_run_id, p_snapshot_phase, v_tablespace;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('[SNAPSHOT] DBA path failed: ' || SQLERRM);
                DBMS_OUTPUT.PUT_LINE('[SNAPSHOT] Falling back to user_segments (current user only)...');
                DBMS_OUTPUT.PUT_LINE('[SNAPSHOT] WARNING: Totals will NOT match the reclaim report.');
                DBMS_OUTPUT.PUT_LINE('[SNAPSHOT] To fix, run as SYS:');
                DBMS_OUTPUT.PUT_LINE('[SNAPSHOT]   GRANT SELECT ON sys.dba_segments TO oppayments;');
                DBMS_OUTPUT.PUT_LINE('[SNAPSHOT]   GRANT SELECT ON sys.dba_lobs TO oppayments;');
                DBMS_OUTPUT.PUT_LINE('[SNAPSHOT]   GRANT SELECT ON sys.dba_data_files TO oppayments;');
                -- Fall back to user_segments / user_lobs (no DBA privileges).
                -- USER is the owner constant; module CASE works the same way.
                EXECUTE IMMEDIATE
                    'INSERT INTO oppayments.epf_purge_space_snapshot
                        (run_id, snapshot_phase, owner, segment_name, segment_type,
                         parent_table, size_bytes, size_mb, module)
                    SELECT :run_id, :phase, USER AS owner, segment_name, segment_type,
                           parent_table, size_bytes, size_mb, ' || c_module_case || '
                    FROM (
                        SELECT USER AS owner, sg.segment_name, sg.segment_type,
                               COALESCE(l.table_name, sg.segment_name) AS parent_table,
                               sg.total_bytes AS size_bytes,
                               ROUND(sg.total_bytes / 1048576, 2) AS size_mb
                        FROM (
                            SELECT segment_name, segment_type, SUM(bytes) AS total_bytes
                            FROM user_segments
                            GROUP BY segment_name, segment_type
                        ) sg
                        LEFT JOIN (
                            SELECT segment_name, MIN(table_name) AS table_name
                            FROM user_lobs
                            GROUP BY segment_name
                        ) l ON l.segment_name = sg.segment_name
                    )'
                    USING p_run_id, p_snapshot_phase;
        END;

        -- op.spec_trt_log lives in the OP schema and may be in a different
        -- tablespace than OPPAYMENTS default. The main capture above filters
        -- by tablespace, so spec_trt_log can be missed. If it isn't already
        -- in the snapshot for this run/phase, capture it explicitly here.
        BEGIN
            SELECT COUNT(*) INTO v_spec_trt_count
            FROM oppayments.epf_purge_space_snapshot
            WHERE run_id = p_run_id
              AND snapshot_phase = p_snapshot_phase
              AND owner = 'OP'
              AND UPPER(parent_table) = 'SPEC_TRT_LOG';

            IF v_spec_trt_count = 0 THEN
                EXECUTE IMMEDIATE
                    'INSERT INTO oppayments.epf_purge_space_snapshot
                        (run_id, snapshot_phase, owner, segment_name, segment_type,
                         parent_table, size_bytes, size_mb, module)
                    SELECT :run_id, :phase, owner, segment_name, segment_type,
                           ''SPEC_TRT_LOG'' AS parent_table,
                           SUM(bytes) AS size_bytes,
                           ROUND(SUM(bytes) / 1048576, 2) AS size_mb,
                           ''LOGS'' AS module
                    FROM dba_segments
                    WHERE owner = ''OP'' AND segment_name = ''SPEC_TRT_LOG''
                    GROUP BY owner, segment_name, segment_type'
                    USING p_run_id, p_snapshot_phase;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('[SNAPSHOT] Could not capture op.spec_trt_log explicitly: '
                    || SQLERRM);
        END;

        DECLARE
            v_cnt NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_cnt
            FROM oppayments.epf_purge_space_snapshot
            WHERE run_id = p_run_id AND snapshot_phase = p_snapshot_phase;

            COMMIT;

            DBMS_OUTPUT.PUT_LINE('Space snapshot captured: ' || p_snapshot_phase
                || ' (' || v_cnt || ' segments)');
        END;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('WARNING: Failed to capture space snapshot: ' || SQLERRM);
    END capture_space_snapshot;

    -- ========================================================================
    -- print_space_comparison
    -- ========================================================================
    -- Prints a depth-aware before/after comparison:
    --   - All 27 purged tables shown (always), grouped by module (PAYMENTS,
    --     LOGS, BANK_STATEMENTS), sorted by owner.table_name within each.
    --   - Only modules covered by p_depth are shown. ALL = all three.
    --   - Per-module subtotal at the end of each block.
    --   - PURGED TABLES TOTAL (sum of shown modules) at the bottom.
    --   - TABLESPACE TOTAL (sum of all snapshot rows including OTHER) for
    --     a sanity check against the reclaim report.
    --   - Rows for tables that have zero size in both snapshots still appear
    --     (with 0.00 / 0.00 / 0.0%).
    PROCEDURE print_space_comparison(
        p_run_id IN RAW,
        p_depth  IN VARCHAR2 DEFAULT C_DEPTH_ALL
    )
    IS
        l_d VARCHAR2(200) := UPPER(REPLACE(NVL(p_depth, C_DEPTH_ALL), ' ', ''));
        l_show_payments BOOLEAN :=
            l_d = C_DEPTH_ALL OR INSTR(',' || l_d || ',', ',' || C_DEPTH_PAYMENTS || ',') > 0;
        l_show_logs BOOLEAN :=
            l_d = C_DEPTH_ALL OR INSTR(',' || l_d || ',', ',' || C_DEPTH_LOGS || ',') > 0;
        l_show_bank BOOLEAN :=
            l_d = C_DEPTH_ALL OR INSTR(',' || l_d || ',', ',' || C_DEPTH_BANK_STATEMENTS || ',') > 0;

        l_purged_before NUMBER := 0;
        l_purged_after  NUMBER := 0;

        l_ts_before NUMBER := 0;
        l_ts_after  NUMBER := 0;

        -- Hardcoded master list of the 27 purged tables, in the canonical
        -- module order (PAYMENTS -> LOGS -> BANK_STATEMENTS), alphabetised
        -- inside each module. Ensures a row appears for every purged table
        -- even if it has no segment in the snapshot.
        TYPE t_table_def IS RECORD (
            owner   VARCHAR2(30),
            tname   VARCHAR2(128),
            module  VARCHAR2(20)
        );
        TYPE t_table_def_list IS TABLE OF t_table_def INDEX BY PLS_INTEGER;
        l_tables t_table_def_list;
        l_idx    PLS_INTEGER := 0;

        PROCEDURE add_table(p_owner VARCHAR2, p_tname VARCHAR2, p_module VARCHAR2) IS
        BEGIN
            l_idx := l_idx + 1;
            l_tables(l_idx).owner := p_owner;
            l_tables(l_idx).tname := p_tname;
            l_tables(l_idx).module := p_module;
        END;

        FUNCTION fmt_row(
            p_label   VARCHAR2,
            p_before  NUMBER,
            p_after   NUMBER
        ) RETURN VARCHAR2 IS
            l_freed NUMBER := p_before - p_after;
            l_pct   NUMBER :=
                CASE WHEN p_before > 0 THEN ROUND(l_freed / p_before * 100, 1)
                     ELSE 0 END;
        BEGIN
            RETURN RPAD(p_label, 44)
                || LPAD(TO_CHAR(p_before, '999,990.00'), 13)
                || LPAD(TO_CHAR(p_after,  '999,990.00'), 13)
                || LPAD(TO_CHAR(l_freed,  '999,990.00'), 13)
                || LPAD(TO_CHAR(l_pct, '990.0') || '%', 8);
        END;

        PROCEDURE print_module_block(
            p_module IN VARCHAR2
        ) IS
            l_sub_before NUMBER := 0;
            l_sub_after  NUMBER := 0;
            l_before NUMBER;
            l_after  NUMBER;
        BEGIN
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('[' || p_module || ']');

            FOR i IN 1 .. l_tables.COUNT LOOP
                IF l_tables(i).module = p_module THEN
                    -- Sum BEFORE size_mb for this owner.parent_table
                    BEGIN
                        SELECT NVL(SUM(size_mb), 0) INTO l_before
                        FROM oppayments.epf_purge_space_snapshot
                        WHERE run_id = p_run_id
                          AND snapshot_phase = 'BEFORE'
                          AND owner = l_tables(i).owner
                          AND UPPER(parent_table) = UPPER(l_tables(i).tname);
                    EXCEPTION WHEN OTHERS THEN l_before := 0;
                    END;

                    BEGIN
                        SELECT NVL(SUM(size_mb), 0) INTO l_after
                        FROM oppayments.epf_purge_space_snapshot
                        WHERE run_id = p_run_id
                          AND snapshot_phase = 'AFTER'
                          AND owner = l_tables(i).owner
                          AND UPPER(parent_table) = UPPER(l_tables(i).tname);
                    EXCEPTION WHEN OTHERS THEN l_after := 0;
                    END;

                    DBMS_OUTPUT.PUT_LINE('  ' ||
                        fmt_row(l_tables(i).owner || '.' || l_tables(i).tname,
                                l_before, l_after));

                    l_sub_before := l_sub_before + l_before;
                    l_sub_after  := l_sub_after  + l_after;
                END IF;
            END LOOP;

            DBMS_OUTPUT.PUT_LINE('  ' || RPAD('-', 89, '-'));
            DBMS_OUTPUT.PUT_LINE('  ' ||
                fmt_row(p_module || ' subtotal', l_sub_before, l_sub_after));

            l_purged_before := l_purged_before + l_sub_before;
            l_purged_after  := l_purged_after  + l_sub_after;
        END;

    BEGIN
        -- ------------------------------------------------------------------
        -- Build the master table list (27 tables, fixed order)
        -- ------------------------------------------------------------------
        -- PAYMENTS module (22 tables, alphabetical by name)
        add_table('OPPAYMENTS', 'APPROBATION_EXECUTION',         'PAYMENTS');
        add_table('OPPAYMENTS', 'APPROBATION_EXECUTION_OPT',     'PAYMENTS');
        add_table('OPPAYMENTS', 'BULK_PAYMENT',                  'PAYMENTS');
        add_table('OPPAYMENTS', 'BULK_PAYMENT_ADDITIONAL_INFO',  'PAYMENTS');
        add_table('OPPAYMENTS', 'BULK_SIGNATURE',                'PAYMENTS');
        add_table('OPPAYMENTS', 'BULKPAYMENT_EXCEPTION',         'PAYMENTS');
        add_table('OPPAYMENTS', 'FILE_INTEGRATION',              'PAYMENTS');
        add_table('OPPAYMENTS', 'IMPORT_AUDIT',                  'PAYMENTS');
        add_table('OPPAYMENTS', 'IMPORT_AUDIT_MESSAGES',         'PAYMENTS');
        add_table('OPPAYMENTS', 'INVOICE',                       'PAYMENTS');
        add_table('OPPAYMENTS', 'INVOICE_ADDITIONAL_INFO',       'PAYMENTS');
        add_table('OPPAYMENTS', 'MANDATORY_SIGNERS',             'PAYMENTS');
        add_table('OPPAYMENTS', 'NOTIFICATION_EXECUTION',        'PAYMENTS');
        add_table('OPPAYMENTS', 'OIDC_REQUEST_TOKEN',            'PAYMENTS');
        add_table('OPPAYMENTS', 'PAYMENT',                       'PAYMENTS');
        add_table('OPPAYMENTS', 'PAYMENT_ADDITIONAL_INFO',       'PAYMENTS');
        add_table('OPPAYMENTS', 'PAYMENT_AUDIT',                 'PAYMENTS');
        add_table('OPPAYMENTS', 'TRANSMISSION_EXCEPTION',        'PAYMENTS');
        add_table('OPPAYMENTS', 'TRANSMISSION_EXECUTION',        'PAYMENTS');
        add_table('OPPAYMENTS', 'TRANSMISSION_EXECUTION_AUDIT',  'PAYMENTS');
        add_table('OPPAYMENTS', 'WORKFLOW_EXECUTION',            'PAYMENTS');
        add_table('OPPAYMENTS', 'WORKFLOW_EXECUTION_OPT',        'PAYMENTS');
        -- LOGS module (3 tables, alphabetical)
        add_table('OP',         'SPEC_TRT_LOG',                  'LOGS');
        add_table('OPPAYMENTS', 'AUDIT_ARCHIVE',                 'LOGS');
        add_table('OPPAYMENTS', 'AUDIT_TRAIL',                   'LOGS');
        -- BANK_STATEMENTS module (2 tables, alphabetical)
        add_table('OPPAYMENTS', 'DIRECTORY_DISPATCHING',         'BANK_STATEMENTS');
        add_table('OPPAYMENTS', 'FILE_DISPATCHING',              'BANK_STATEMENTS');

        -- ------------------------------------------------------------------
        -- Header
        -- ------------------------------------------------------------------
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('====================================================================================');
        DBMS_OUTPUT.PUT_LINE('  SPACE USAGE COMPARISON (Before vs After)');
        DBMS_OUTPUT.PUT_LINE('  Run ID: ' || RAWTOHEX(p_run_id) || '   Depth: ' || NVL(UPPER(p_depth), C_DEPTH_ALL));
        DBMS_OUTPUT.PUT_LINE('====================================================================================');
        DBMS_OUTPUT.PUT_LINE(RPAD('Owner.Table', 44)
            || LPAD('Before(MB)', 13)
            || LPAD('After(MB)', 13)
            || LPAD('Freed(MB)', 13)
            || LPAD('Freed%', 8));

        -- ------------------------------------------------------------------
        -- Module blocks (only those covered by depth)
        -- ------------------------------------------------------------------
        IF l_show_payments THEN print_module_block('PAYMENTS');        END IF;
        IF l_show_logs     THEN print_module_block('LOGS');            END IF;
        IF l_show_bank     THEN print_module_block('BANK_STATEMENTS'); END IF;

        -- ------------------------------------------------------------------
        -- Tablespace-wide totals (every captured row, including OTHER and
        -- non-purged segments). Read straight from the snapshot.
        -- ------------------------------------------------------------------
        BEGIN
            SELECT NVL(SUM(size_mb), 0) INTO l_ts_before
            FROM oppayments.epf_purge_space_snapshot
            WHERE run_id = p_run_id AND snapshot_phase = 'BEFORE';
        EXCEPTION WHEN OTHERS THEN l_ts_before := 0; END;

        BEGIN
            SELECT NVL(SUM(size_mb), 0) INTO l_ts_after
            FROM oppayments.epf_purge_space_snapshot
            WHERE run_id = p_run_id AND snapshot_phase = 'AFTER';
        EXCEPTION WHEN OTHERS THEN l_ts_after := 0; END;

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE(RPAD('=', 89, '='));
        DBMS_OUTPUT.PUT_LINE('  ' ||
            fmt_row('PURGED TABLES TOTAL (shown modules)',
                    l_purged_before, l_purged_after));
        DBMS_OUTPUT.PUT_LINE('  ' ||
            fmt_row('TABLESPACE TOTAL (incl. non-purged)',
                    l_ts_before, l_ts_after));
        DBMS_OUTPUT.PUT_LINE(RPAD('=', 89, '='));
        DBMS_OUTPUT.PUT_LINE('NOTE: TABLESPACE TOTAL covers every segment captured in OPPAYMENTS''');
        DBMS_OUTPUT.PUT_LINE('default tablespace; it should match the reclaim script''s "Used space"');
        DBMS_OUTPUT.PUT_LINE('numbers when DBA views are accessible. Without DBA views the totals');
        DBMS_OUTPUT.PUT_LINE('only cover OPPAYMENTS-owned segments.');

    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('ERROR generating space comparison: ' || SQLERRM);
    END print_space_comparison;

    -- ========================================================================
    -- run_purge (master orchestrator)
    -- ========================================================================
    PROCEDURE run_purge(
        p_retention_days   IN NUMBER   DEFAULT C_DEFAULT_RETENTION_DAYS,
        p_purge_depth      IN VARCHAR2 DEFAULT C_DEPTH_ALL,
        p_batch_size       IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run          IN BOOLEAN  DEFAULT FALSE,
        p_purge_mode       IN VARCHAR2 DEFAULT 'FULL'
    )
    IS
        l_run_id       RAW(16);
        l_cutoff_date  DATE;
        l_start_ts     TIMESTAMP;
        l_depth        VARCHAR2(200);
        l_mode         VARCHAR2(30);

        -- Helper: check if a module is included in a (possibly comma-separated) depth string.
        -- Returns TRUE for 'ALL' or when the module name appears in the list.
        FUNCTION depth_includes(p_d VARCHAR2, p_module VARCHAR2) RETURN BOOLEAN IS
        BEGIN
            RETURN p_d = C_DEPTH_ALL
                OR INSTR(',' || p_d || ',', ',' || p_module || ',') > 0;
        END depth_includes;
    BEGIN
        l_start_ts := SYSTIMESTAMP;
        l_run_id := SYS_GUID();
        l_cutoff_date := TRUNC(SYSDATE - p_retention_days);
        l_depth := UPPER(REPLACE(NVL(p_purge_depth, C_DEPTH_ALL), ' ', ''));
        l_mode  := UPPER(NVL(p_purge_mode, C_MODE_FULL));

        -- Validate purge mode
        IF l_mode NOT IN (C_MODE_FULL, C_MODE_CLOB_ONLY, C_MODE_CLOB_N_LOGS) THEN
            RAISE_APPLICATION_ERROR(-20002,
                'Invalid purge mode: ' || l_mode
                || '. Valid values: FULL, CLOB_ONLY, CLOB_N_LOGS');
        END IF;

        -- CLOB_N_LOGS: auto-include LOGS in depth for the DELETE portion
        IF l_mode = C_MODE_CLOB_N_LOGS
           AND l_depth <> C_DEPTH_ALL
           AND NOT depth_includes(l_depth, C_DEPTH_LOGS)
        THEN
            l_depth := l_depth || ',' || C_DEPTH_LOGS;
        END IF;

        -- Ensure logging infrastructure exists
        ensure_log_table;

        -- Log run start
        log_entry(
            p_run_id         => l_run_id,
            p_module         => 'ORCHESTRATOR',
            p_operation      => 'RUN_START',
            p_retention_days => p_retention_days,
            p_status         => 'INFO',
            p_message        => 'EPF Purge started. depth=' || l_depth
                                || ', mode=' || l_mode
                                || ', retention=' || p_retention_days || ' days'
                                || ', cutoff=' || TO_CHAR(l_cutoff_date, 'YYYY-MM-DD')
                                || ', batch_size=' || p_batch_size
                                || ', dry_run=' || CASE WHEN p_dry_run THEN 'Y' ELSE 'N' END
        );

        -- The wrapper script already prints a Configuration Summary header
        -- before invoking run_purge, and the live monitor surfaces RUN_START
        -- with the same metadata. The duplicate banner here was redundant
        -- noise in the buffered DBMS_OUTPUT flood that arrived after the
        -- block ended. Run metadata lives in epf_purge_log instead.

        -- Capture space usage BEFORE purge
        capture_space_snapshot(l_run_id, 'BEFORE');

        -- Validate purge depth (supports single value or comma-separated list)
        IF l_depth <> C_DEPTH_ALL
           AND NOT depth_includes(l_depth, C_DEPTH_PAYMENTS)
           AND NOT depth_includes(l_depth, C_DEPTH_LOGS)
           AND NOT depth_includes(l_depth, C_DEPTH_BANK_STATEMENTS)
        THEN
            log_entry(
                p_run_id    => l_run_id,
                p_module    => 'ORCHESTRATOR',
                p_operation => 'VALIDATE',
                p_status    => 'ERROR',
                p_message   => 'Invalid purge depth: ' || l_depth
                               || '. Valid values: ALL, PAYMENTS, LOGS, BANK_STATEMENTS (comma-separated OK)'
            );
            RAISE_APPLICATION_ERROR(-20001,
                'Invalid purge depth: ' || l_depth
                || '. Valid values: ALL, PAYMENTS, LOGS, BANK_STATEMENTS (comma-separated OK)');
        END IF;

        -- Execute module procedures based on purge depth
        IF depth_includes(l_depth, C_DEPTH_PAYMENTS) THEN
            purge_bulk_payments(
                p_run_id      => l_run_id,
                p_cutoff_date => l_cutoff_date,
                p_batch_size  => p_batch_size,
                p_dry_run     => p_dry_run,
                p_purge_mode  => l_mode
            );

            purge_file_integrations(
                p_run_id      => l_run_id,
                p_cutoff_date => l_cutoff_date,
                p_batch_size  => p_batch_size,
                p_dry_run     => p_dry_run,
                p_purge_mode  => l_mode
            );
        END IF;

        IF depth_includes(l_depth, C_DEPTH_LOGS) THEN
            -- For CLOB_N_LOGS: LOGS module always gets FULL mode (delete rows)
            purge_audit_logs(
                p_run_id      => l_run_id,
                p_cutoff_date => l_cutoff_date,
                p_batch_size  => p_batch_size,
                p_dry_run     => p_dry_run,
                p_purge_mode  => CASE WHEN l_mode = C_MODE_CLOB_N_LOGS
                                      THEN C_MODE_FULL ELSE l_mode END
            );

            purge_tech_logs(
                p_run_id      => l_run_id,
                p_cutoff_date => l_cutoff_date,
                p_batch_size  => p_batch_size,
                p_dry_run     => p_dry_run,
                p_purge_mode  => CASE WHEN l_mode = C_MODE_CLOB_N_LOGS
                                      THEN C_MODE_FULL ELSE l_mode END
            );
        END IF;

        IF depth_includes(l_depth, C_DEPTH_BANK_STATEMENTS) THEN
            purge_bank_statements(
                p_run_id      => l_run_id,
                p_cutoff_date => l_cutoff_date,
                p_batch_size  => p_batch_size,
                p_dry_run     => p_dry_run,
                p_purge_mode  => l_mode
            );
        END IF;

        -- Capture space usage AFTER purge (rows deleted, but segments not yet compacted).
        -- NOTE: If --reclaim is used, the wrapper script will capture a second AFTER
        -- snapshot post-reclaim and print the comparison then, which is more meaningful
        -- since DELETE alone does not change segment sizes.
        capture_space_snapshot(l_run_id, 'AFTER');

        -- Log run end
        log_entry(
            p_run_id          => l_run_id,
            p_module          => 'ORCHESTRATOR',
            p_operation       => 'RUN_END',
            p_status          => 'SUCCESS',
            p_message         => 'EPF Purge completed',
            p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
        );

        -- Print summary
        print_run_summary(l_run_id);

    EXCEPTION
        WHEN OTHERS THEN
            log_entry(
                p_run_id        => l_run_id,
                p_module        => 'ORCHESTRATOR',
                p_operation     => 'RUN_END',
                p_status        => 'ERROR',
                p_error_code    => SQLCODE,
                p_error_message => SQLERRM,
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('FATAL ERROR in run_purge: ' || SQLERRM);
            RAISE;
    END run_purge;

END epf_purge_pkg;
/
