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
                    CONSTRAINT chk_snapshot_phase
                        CHECK (snapshot_phase IN (''BEFORE'', ''AFTER''))
                )';

            EXECUTE IMMEDIATE '
                CREATE INDEX oppayments.idx_epf_space_snap_run
                ON oppayments.epf_purge_space_snapshot (run_id, snapshot_phase)';

            DBMS_OUTPUT.PUT_LINE('EPF_PURGE_SPACE_SNAPSHOT table created.');
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
        p_dry_run     IN BOOLEAN  DEFAULT FALSE
    )
    IS
        l_batch_count  NUMBER := 0;
        l_total_count  NUMBER := 0;
        l_rows_deleted NUMBER;
        l_start_ts     TIMESTAMP;
        l_batch_start  TIMESTAMP;
    BEGIN
        l_start_ts := SYSTIMESTAMP;

        log_entry(
            p_run_id    => p_run_id,
            p_module    => 'FILE_INTEGRATION',
            p_operation => 'DELETE',
            p_status    => 'INFO',
            p_message   => 'Starting file integration purge. Cutoff date: '
                           || TO_CHAR(p_cutoff_date, 'YYYY-MM-DD')
        );

        IF p_dry_run THEN
            SELECT COUNT(*) INTO l_total_count
            FROM oppayments.file_integration
            WHERE integration_date < p_cutoff_date;

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

            DELETE FROM oppayments.file_integration
            WHERE integration_date < p_cutoff_date
              AND ROWNUM <= p_batch_size;

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
        p_dry_run     IN BOOLEAN  DEFAULT FALSE
    )
    IS
        l_batch_count  NUMBER := 0;
        l_total_count  NUMBER := 0;
        l_rows_deleted NUMBER;
        l_start_ts     TIMESTAMP;
        l_batch_start  TIMESTAMP;
    BEGIN
        l_start_ts := SYSTIMESTAMP;

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
        p_dry_run     IN BOOLEAN  DEFAULT FALSE
    )
    IS
        l_audit_ids    t_id_table;
        l_archive_ids  t_id_table;
        l_batch_count  NUMBER := 0;
        l_total_audit  NUMBER := 0;
        l_total_archive NUMBER := 0;
        l_rows_deleted NUMBER;
        l_start_ts     TIMESTAMP;
        l_batch_start  TIMESTAMP;

        CURSOR c_audit IS
            SELECT audit_id, audit_archive_id
            FROM oppayments.audit_trail
            WHERE audit_timestamp < p_cutoff_date
            ORDER BY audit_id;
    BEGIN
        l_start_ts := SYSTIMESTAMP;

        log_entry(
            p_run_id    => p_run_id,
            p_module    => 'AUDIT_LOGS',
            p_operation => 'DELETE',
            p_status    => 'INFO',
            p_message   => 'Starting audit logs purge. Cutoff date: '
                           || TO_CHAR(p_cutoff_date, 'YYYY-MM-DD')
        );

        IF p_dry_run THEN
            SELECT COUNT(*) INTO l_total_audit
            FROM oppayments.audit_trail
            WHERE audit_timestamp < p_cutoff_date;

            SELECT COUNT(*) INTO l_total_archive
            FROM oppayments.audit_archive aa
            WHERE EXISTS (
                SELECT 1 FROM oppayments.audit_trail at2
                WHERE at2.audit_archive_id = aa.audit_archive_id
                  AND at2.audit_timestamp < p_cutoff_date
            );

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

        OPEN c_audit;
        LOOP
            l_batch_start := SYSTIMESTAMP;
            l_batch_count := l_batch_count + 1;

            FETCH c_audit BULK COLLECT INTO l_audit_ids, l_archive_ids
                LIMIT p_batch_size;

            EXIT WHEN l_audit_ids.COUNT = 0;

            -- Delete audit_archive first (child table)
            -- Filter out NULL archive IDs (not every audit_trail row has an archive)
            FORALL i IN 1..l_archive_ids.COUNT
                DELETE FROM oppayments.audit_archive
                WHERE audit_archive_id = l_archive_ids(i)
                  AND l_archive_ids(i) IS NOT NULL;

            l_rows_deleted := SQL%ROWCOUNT;
            l_total_archive := l_total_archive + l_rows_deleted;

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
            FORALL i IN 1..l_audit_ids.COUNT
                DELETE FROM oppayments.audit_trail
                WHERE audit_id = l_audit_ids(i);

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
        CLOSE c_audit;

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
            IF c_audit%ISOPEN THEN CLOSE c_audit; END IF;
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
        p_dry_run     IN BOOLEAN  DEFAULT FALSE
    )
    IS
        l_fd_ids       t_id_table;
        l_dd_ids       t_id_table;
        l_batch_count  NUMBER := 0;
        l_total_fd     NUMBER := 0;
        l_total_dd     NUMBER := 0;
        l_rows_deleted NUMBER;
        l_start_ts     TIMESTAMP;
        l_batch_start  TIMESTAMP;

        CURSOR c_bank_stmts IS
            SELECT fd.file_dispatching_id, dd.directory_dispatching_id
            FROM oppayments.directory_dispatching dd
            INNER JOIN oppayments.file_dispatching fd
                ON fd.file_dispatching_id = dd.file_dispatching_id
            WHERE fd.date_reception < p_cutoff_date
            ORDER BY fd.file_dispatching_id;
    BEGIN
        l_start_ts := SYSTIMESTAMP;

        log_entry(
            p_run_id    => p_run_id,
            p_module    => 'BANK_STATEMENTS',
            p_operation => 'DELETE',
            p_status    => 'INFO',
            p_message   => 'Starting bank statements purge. Cutoff date: '
                           || TO_CHAR(p_cutoff_date, 'YYYY-MM-DD')
        );

        IF p_dry_run THEN
            SELECT COUNT(*) INTO l_total_dd
            FROM oppayments.directory_dispatching dd
            INNER JOIN oppayments.file_dispatching fd
                ON fd.file_dispatching_id = dd.file_dispatching_id
            WHERE fd.date_reception < p_cutoff_date;

            SELECT COUNT(*) INTO l_total_fd
            FROM oppayments.file_dispatching
            WHERE date_reception < p_cutoff_date;

            log_entry(
                p_run_id        => p_run_id,
                p_module        => 'BANK_STATEMENTS',
                p_operation     => 'DRY_RUN_COUNT',
                p_status        => 'SUCCESS',
                p_message       => 'Dry run: ' || l_total_dd || ' directory_dispatching + '
                                   || l_total_fd || ' file_dispatching rows would be deleted',
                p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
            );
            DBMS_OUTPUT.PUT_LINE('[DRY RUN] directory_dispatching: ' || l_total_dd
                || ', file_dispatching: ' || l_total_fd || ' rows would be deleted');
            RETURN;
        END IF;

        OPEN c_bank_stmts;
        LOOP
            l_batch_start := SYSTIMESTAMP;
            l_batch_count := l_batch_count + 1;

            FETCH c_bank_stmts BULK COLLECT INTO l_fd_ids, l_dd_ids
                LIMIT p_batch_size;

            EXIT WHEN l_dd_ids.COUNT = 0;

            -- Delete directory_dispatching first (child table)
            FORALL i IN 1..l_dd_ids.COUNT
                DELETE FROM oppayments.directory_dispatching
                WHERE directory_dispatching_id = l_dd_ids(i);

            l_rows_deleted := SQL%ROWCOUNT;
            l_total_dd := l_total_dd + l_rows_deleted;

            -- Delete file_dispatching (parent table)
            -- Use direct delete with deduplication to handle one-to-many relationship
            FORALL i IN 1..l_fd_ids.COUNT
                DELETE FROM oppayments.file_dispatching
                WHERE file_dispatching_id = l_fd_ids(i)
                  AND NOT EXISTS (
                      SELECT 1 FROM oppayments.directory_dispatching dd2
                      WHERE dd2.file_dispatching_id = l_fd_ids(i)
                  );

            l_total_fd := l_total_fd + SQL%ROWCOUNT;

            -- Single per-batch log entry
            log_entry(
                p_run_id          => p_run_id,
                p_module          => 'BANK_STATEMENTS',
                p_operation       => 'DELETE',
                p_rows_affected   => l_rows_deleted,
                p_batch_number    => l_batch_count,
                p_status          => 'SUCCESS',
                p_elapsed_seconds => get_elapsed_seconds(l_batch_start, SYSTIMESTAMP)
            );

            COMMIT;
        END LOOP;
        CLOSE c_bank_stmts;

        log_entry(
            p_run_id          => p_run_id,
            p_module          => 'BANK_STATEMENTS',
            p_operation       => 'DELETE',
            p_status          => 'SUCCESS',
            p_message         => 'Completed. directory_dispatching: ' || l_total_dd
                                 || ', file_dispatching: ' || l_total_fd || ' rows deleted',
            p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
        );

        DBMS_OUTPUT.PUT_LINE('directory_dispatching: ' || l_total_dd
            || ', file_dispatching: ' || l_total_fd || ' rows deleted');

    EXCEPTION
        WHEN OTHERS THEN
            IF c_bank_stmts%ISOPEN THEN CLOSE c_bank_stmts; END IF;
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
        p_dry_run     IN BOOLEAN  DEFAULT FALSE
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

        CURSOR c_bulk_payments IS
            SELECT bp.bulk_payment_id
            FROM oppayments.bulk_payment bp
            WHERE bp.value_date < p_cutoff_date
            ORDER BY bp.bulk_payment_id;
    BEGIN
        l_start_ts := SYSTIMESTAMP;

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
            SELECT COUNT(*) INTO l_tot_bp
            FROM oppayments.bulk_payment
            WHERE value_date < p_cutoff_date;

            SELECT COUNT(*) INTO l_tot_payment
            FROM oppayments.payment p
            WHERE EXISTS (
                SELECT 1 FROM oppayments.bulk_payment bp
                WHERE bp.bulk_payment_id = p.bulk_payment_id
                  AND bp.value_date < p_cutoff_date
            );

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

        OPEN c_bulk_payments;
        LOOP
            l_batch_start := SYSTIMESTAMP;
            l_batch_count := l_batch_count + 1;

            -- Collect a batch of bulk_payment_ids
            FETCH c_bulk_payments BULK COLLECT INTO l_bp_ids
                LIMIT p_batch_size;

            EXIT WHEN l_bp_ids.COUNT = 0;

            -- =============================================================
            -- OPTIMIZATION: Materialise payment_ids ONCE per batch.
            -- Avoids repeating the payment join 7 times per batch.
            -- Uses oppayments.epf_number_tab (schema-level nested table)
            -- so there is no VARRAY 32K limit.
            -- =============================================================
            SELECT payment_id BULK COLLECT INTO l_pay_ids
            FROM oppayments.payment
            WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(l_bp_ids));

            -- =============================================================
            -- DELETE in FK-dependency order (leaves first, root last)
            -- =============================================================

            -- 1. bulk_payment_additional_info (FK to bulk_payment)
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.bulk_payment_additional_info
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_bp_addl := l_tot_bp_addl + l_rows_deleted;

            -- 2. bulk_signature (FK to bulk_payment)
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.bulk_signature
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_bulk_sig := l_tot_bulk_sig + l_rows_deleted;

            -- 3. mandatory_signers (FK to bulk_payment)
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.mandatory_signers
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_mand_sign := l_tot_mand_sign + l_rows_deleted;

            -- 4. oidc_request_token (FK to bulk_payment)
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.oidc_request_token
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_oidc_req := l_tot_oidc_req + l_rows_deleted;

            -- 5. payment_audit (by bulk_payment_id)
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.payment_audit
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_pay_aud1 := l_tot_pay_aud1 + l_rows_deleted;

            -- 6. transmission_execution_audit (FK to transmission_execution
            --    AND possibly import_audit via IMPORT_AUDIT_ID_FK)
            --    Must be deleted BEFORE both import_audit and transmission_execution
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.transmission_execution_audit
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_tx_aud := l_tot_tx_aud + l_rows_deleted;

            -- 7. import_audit_messages (FK to import_audit via IMPORT_AUDIT_ID_FK)
            --    Must be deleted BEFORE import_audit
            DELETE FROM oppayments.import_audit_messages
            WHERE import_audit_id IN (
                SELECT import_audit_id FROM oppayments.import_audit
                WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(l_bp_ids))
            );
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_imp_msg := l_tot_imp_msg + l_rows_deleted;

            -- 7b. notification_execution (FK to import_audit AND transmission_execution)
            --    Must be deleted BEFORE both import_audit and transmission_execution
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.notification_execution
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_notif := l_tot_notif + l_rows_deleted;

            -- 8. import_audit (FK to bulk_payment)
            --    Now safe: import_audit_messages + notification_execution + tex_audit deleted
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.import_audit
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_imp_aud := l_tot_imp_aud + l_rows_deleted;

            -- 9. transmission_execution (FK to bulk_payment AND transmission_exception)
            --    Now safe: notification_execution + tex_audit already deleted
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.transmission_execution
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_tx_exec := l_tot_tx_exec + l_rows_deleted;

            -- 10. transmission_exception (FK to bulk_payment)
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.transmission_exception
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_tx_exc := l_tot_tx_exc + l_rows_deleted;

            -- 11. approbation_execution_opt (FK to workflow_execution_opt)
            DELETE FROM oppayments.approbation_execution_opt
            WHERE execution_id IN (
                SELECT execution_id FROM oppayments.workflow_execution_opt
                WHERE bulk_payment_id IN (SELECT COLUMN_VALUE FROM TABLE(l_bp_ids))
            );
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_approv_opt := l_tot_approv_opt + l_rows_deleted;

            -- 12. workflow_execution_opt (FK to bulk_payment)
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.workflow_execution_opt
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_wf_opt := l_tot_wf_opt + l_rows_deleted;

            -- ============================================================
            -- Payment-level deletes: use materialised l_pay_ids
            -- (eliminates 6 redundant joins to the payment table)
            -- ============================================================

            -- 13. approbation_execution (FK to workflow_execution)
            DELETE FROM oppayments.approbation_execution
            WHERE execution_id IN (
                SELECT we.execution_id
                FROM oppayments.workflow_execution we
                WHERE we.payment_id IN (SELECT COLUMN_VALUE FROM TABLE(l_pay_ids))
            );
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_approv := l_tot_approv + l_rows_deleted;

            -- 14. workflow_execution (via payment)
            DELETE FROM oppayments.workflow_execution
            WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(l_pay_ids));
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_wf_exec := l_tot_wf_exec + l_rows_deleted;

            -- 15. payment_audit (indirect: via payment_id)
            DELETE FROM oppayments.payment_audit
            WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(l_pay_ids));
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_pay_aud2 := l_tot_pay_aud2 + l_rows_deleted;

            -- 16. bulkpayment_exception (FK to payment)
            DELETE FROM oppayments.bulkpayment_exception
            WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(l_pay_ids));
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_bp_exc := l_tot_bp_exc + l_rows_deleted;

            -- 17. invoice_additional_info (FK to invoice)
            DELETE FROM oppayments.invoice_additional_info
            WHERE invoice_id IN (
                SELECT invoice_id FROM oppayments.invoice
                WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(l_pay_ids))
            );
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_inv_addl := l_tot_inv_addl + l_rows_deleted;

            -- 18. invoice (FK to payment)
            DELETE FROM oppayments.invoice
            WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(l_pay_ids));
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_invoice := l_tot_invoice + l_rows_deleted;

            -- 19. payment_additional_info (via payment)
            DELETE FROM oppayments.payment_additional_info
            WHERE payment_id IN (SELECT COLUMN_VALUE FROM TABLE(l_pay_ids));
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_pay_addl := l_tot_pay_addl + l_rows_deleted;

            -- 20. payment (mid-level)
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.payment
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_payment := l_tot_payment + l_rows_deleted;

            -- 21. bulk_payment (root)
            FORALL i IN 1..l_bp_ids.COUNT
                DELETE FROM oppayments.bulk_payment
                WHERE bulk_payment_id = l_bp_ids(i);
            l_rows_deleted := SQL%ROWCOUNT;
            l_tot_bp := l_tot_bp + l_rows_deleted;

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
                p_operation       => 'DELETE',
                p_table_name      => 'oppayments.bulk_payment (batch)',
                p_rows_affected   => l_bp_ids.COUNT,
                p_batch_number    => l_batch_count,
                p_status          => 'SUCCESS',
                p_message         => 'Batch ' || l_batch_count || ': '
                                     || l_tot_bp || ' bulk_payment, '
                                     || l_tot_payment || ' payment deleted so far',
                p_elapsed_seconds => get_elapsed_seconds(l_batch_start, SYSTIMESTAMP)
            );
        END LOOP;
        CLOSE c_bulk_payments;

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
            p_operation       => 'DELETE',
            p_status          => 'SUCCESS',
            p_message         => 'Completed. bulk_payment: ' || l_tot_bp
                                 || ', payment: ' || l_tot_payment
                                 || ' (+ all dependent records across 21 tables)',
            p_elapsed_seconds => get_elapsed_seconds(l_start_ts, SYSTIMESTAMP)
        );

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

    EXCEPTION
        WHEN OTHERS THEN
            IF c_bulk_payments%ISOPEN THEN CLOSE c_bulk_payments; END IF;
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
    BEGIN
        -- Determine the OPPAYMENTS default tablespace (user_users needs no DBA grants)
        BEGIN
            SELECT default_tablespace INTO v_tablespace FROM user_users;
        EXCEPTION
            WHEN OTHERS THEN v_tablespace := NULL;
        END;

        -- Build DBA query: capture ALL segments in the tablespace so totals
        -- match the reclaim script (which also measures by tablespace).
        v_dba_sql :=
            'INSERT INTO oppayments.epf_purge_space_snapshot
                (run_id, snapshot_phase, owner, segment_name, segment_type,
                 parent_table, size_bytes, size_mb)
            SELECT :run_id, :phase, sg.owner, sg.segment_name, sg.segment_type,
                   COALESCE(l.table_name, sg.segment_name) AS parent_table,
                   sg.total_bytes, ROUND(sg.total_bytes / 1048576, 2)
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
            ) l ON l.owner = sg.owner AND l.segment_name = sg.segment_name';

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
                -- Fall back to user_segments / user_lobs (no DBA privileges)
                INSERT INTO oppayments.epf_purge_space_snapshot
                    (run_id, snapshot_phase, owner, segment_name, segment_type,
                     parent_table, size_bytes, size_mb)
                SELECT p_run_id, p_snapshot_phase, USER, sg.segment_name, sg.segment_type,
                       COALESCE(l.table_name, sg.segment_name) AS parent_table,
                       sg.total_bytes, ROUND(sg.total_bytes / 1048576, 2)
                FROM (
                    SELECT segment_name, segment_type, SUM(bytes) AS total_bytes
                    FROM user_segments
                    GROUP BY segment_name, segment_type
                ) sg
                LEFT JOIN (
                    SELECT segment_name, MIN(table_name) AS table_name
                    FROM user_lobs
                    GROUP BY segment_name
                ) l ON l.segment_name = sg.segment_name;
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
    -- Prints a before/after comparison of segment sizes, grouped by
    -- owner + parent table (so LOB segments roll up into their parent
    -- table's total, and cross-schema segments are shown separately).
    PROCEDURE print_space_comparison(p_run_id IN RAW)
    IS
        l_total_before NUMBER := 0;
        l_total_after  NUMBER := 0;
    BEGIN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('  SPACE USAGE COMPARISON (Before vs After Purge)');
        DBMS_OUTPUT.PUT_LINE('  Run ID: ' || RAWTOHEX(p_run_id));
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE(
            RPAD('Owner.Segment / Table', 42)
            || LPAD('Before(MB)', 12)
            || LPAD('After(MB)', 12)
            || LPAD('Freed(MB)', 12)
            || LPAD('Freed%', 8)
        );
        DBMS_OUTPUT.PUT_LINE(RPAD('-', 86, '-'));

        FOR rec IN (
            SELECT NVL(b.owner, a.owner) AS owner,
                   NVL(b.parent_table, a.parent_table) AS parent_table,
                   NVL(b.total_mb, 0) AS before_mb,
                   NVL(a.total_mb, 0) AS after_mb,
                   NVL(b.total_mb, 0) - NVL(a.total_mb, 0) AS freed_mb,
                   CASE WHEN NVL(b.total_mb, 0) > 0
                        THEN ROUND((NVL(b.total_mb, 0) - NVL(a.total_mb, 0))
                                   / b.total_mb * 100, 1)
                        ELSE 0
                   END AS freed_pct
            FROM (
                SELECT owner, parent_table, SUM(size_mb) AS total_mb
                FROM oppayments.epf_purge_space_snapshot
                WHERE run_id = p_run_id AND snapshot_phase = 'BEFORE'
                GROUP BY owner, parent_table
            ) b
            FULL OUTER JOIN (
                SELECT owner, parent_table, SUM(size_mb) AS total_mb
                FROM oppayments.epf_purge_space_snapshot
                WHERE run_id = p_run_id AND snapshot_phase = 'AFTER'
                GROUP BY owner, parent_table
            ) a ON b.owner = a.owner AND b.parent_table = a.parent_table
            ORDER BY NVL(b.total_mb, 0) DESC
        ) LOOP
            -- Only show segments > 0.01 MB to avoid noise
            IF rec.before_mb >= 0.01 OR rec.after_mb >= 0.01 THEN
                DBMS_OUTPUT.PUT_LINE(
                    RPAD(rec.owner || '.' || rec.parent_table, 42)
                    || LPAD(TO_CHAR(rec.before_mb, '999,990.00'), 12)
                    || LPAD(TO_CHAR(rec.after_mb, '999,990.00'), 12)
                    || LPAD(TO_CHAR(rec.freed_mb, '999,990.00'), 12)
                    || LPAD(TO_CHAR(rec.freed_pct, '990.0') || '%', 8)
                );
                l_total_before := l_total_before + rec.before_mb;
                l_total_after := l_total_after + rec.after_mb;
            END IF;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE(RPAD('-', 86, '-'));
        DBMS_OUTPUT.PUT_LINE(
            RPAD('TOTAL (TABLESPACE)', 42)
            || LPAD(TO_CHAR(l_total_before, '999,990.00'), 12)
            || LPAD(TO_CHAR(l_total_after, '999,990.00'), 12)
            || LPAD(TO_CHAR(l_total_before - l_total_after, '999,990.00'), 12)
            || LPAD(CASE WHEN l_total_before > 0
                        THEN TO_CHAR(ROUND((l_total_before - l_total_after)
                                           / l_total_before * 100, 1), '990.0') || '%'
                        ELSE '  0.0%'
                   END, 8)
        );
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('NOTE: With DBA views, totals cover the full tablespace and');
        DBMS_OUTPUT.PUT_LINE('should match the reclaim report. Without DBA views, only');
        DBMS_OUTPUT.PUT_LINE('the current user''s segments are included (totals will be');
        DBMS_OUTPUT.PUT_LINE('lower than the reclaim report).');
        DBMS_OUTPUT.PUT_LINE('============================================================');

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
        p_dry_run          IN BOOLEAN  DEFAULT FALSE
    )
    IS
        l_run_id       RAW(16);
        l_cutoff_date  DATE;
        l_start_ts     TIMESTAMP;
        l_depth        VARCHAR2(30);
    BEGIN
        l_start_ts := SYSTIMESTAMP;
        l_run_id := SYS_GUID();
        l_cutoff_date := TRUNC(SYSDATE - p_retention_days);
        l_depth := UPPER(NVL(p_purge_depth, C_DEPTH_ALL));

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
                                || ', retention=' || p_retention_days || ' days'
                                || ', cutoff=' || TO_CHAR(l_cutoff_date, 'YYYY-MM-DD')
                                || ', batch_size=' || p_batch_size
                                || ', dry_run=' || CASE WHEN p_dry_run THEN 'Y' ELSE 'N' END
        );

        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('  EPF DATA PURGE');
        DBMS_OUTPUT.PUT_LINE('  Run ID:     ' || RAWTOHEX(l_run_id));
        DBMS_OUTPUT.PUT_LINE('  Depth:      ' || l_depth);
        DBMS_OUTPUT.PUT_LINE('  Retention:  ' || p_retention_days || ' days');
        DBMS_OUTPUT.PUT_LINE('  Cutoff:     ' || TO_CHAR(l_cutoff_date, 'YYYY-MM-DD'));
        DBMS_OUTPUT.PUT_LINE('  Batch size: ' || p_batch_size);
        DBMS_OUTPUT.PUT_LINE('  Dry run:    ' || CASE WHEN p_dry_run THEN 'YES' ELSE 'NO' END);
        DBMS_OUTPUT.PUT_LINE('============================================================');

        -- Capture space usage BEFORE purge
        capture_space_snapshot(l_run_id, 'BEFORE');

        -- Validate purge depth
        IF l_depth NOT IN (C_DEPTH_ALL, C_DEPTH_PAYMENTS, C_DEPTH_LOGS, C_DEPTH_BANK_STATEMENTS) THEN
            log_entry(
                p_run_id    => l_run_id,
                p_module    => 'ORCHESTRATOR',
                p_operation => 'VALIDATE',
                p_status    => 'ERROR',
                p_message   => 'Invalid purge depth: ' || l_depth
                               || '. Valid values: ALL, PAYMENTS, LOGS, BANK_STATEMENTS'
            );
            RAISE_APPLICATION_ERROR(-20001,
                'Invalid purge depth: ' || l_depth
                || '. Valid values: ALL, PAYMENTS, LOGS, BANK_STATEMENTS');
        END IF;

        -- Execute module procedures based on purge depth
        IF l_depth IN (C_DEPTH_ALL, C_DEPTH_PAYMENTS) THEN
            purge_bulk_payments(
                p_run_id      => l_run_id,
                p_cutoff_date => l_cutoff_date,
                p_batch_size  => p_batch_size,
                p_dry_run     => p_dry_run
            );

            purge_file_integrations(
                p_run_id      => l_run_id,
                p_cutoff_date => l_cutoff_date,
                p_batch_size  => p_batch_size,
                p_dry_run     => p_dry_run
            );
        END IF;

        IF l_depth IN (C_DEPTH_ALL, C_DEPTH_LOGS) THEN
            purge_audit_logs(
                p_run_id      => l_run_id,
                p_cutoff_date => l_cutoff_date,
                p_batch_size  => p_batch_size,
                p_dry_run     => p_dry_run
            );

            purge_tech_logs(
                p_run_id      => l_run_id,
                p_cutoff_date => l_cutoff_date,
                p_batch_size  => p_batch_size,
                p_dry_run     => p_dry_run
            );
        END IF;

        IF l_depth IN (C_DEPTH_ALL, C_DEPTH_BANK_STATEMENTS) THEN
            purge_bank_statements(
                p_run_id      => l_run_id,
                p_cutoff_date => l_cutoff_date,
                p_batch_size  => p_batch_size,
                p_dry_run     => p_dry_run
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
