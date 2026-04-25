-- ============================================================================
-- EPF Data Purge - Audit/Log Table DDL & Supporting Types
-- ============================================================================
-- Creates the epf_purge_log table in the oppayments schema.
-- This table records every purge operation for auditability and traceability.
-- Also creates the epf_number_tab nested-table type used for bulk-delete
-- optimizations (materializing payment_ids per batch).
-- The script is idempotent: it will not fail if objects already exist.
-- ============================================================================

-- Schema-level nested table type (no VARRAY 32K limit).
-- Used by purge_bulk_payments to materialize payment_ids once per batch
-- instead of repeating the join 7 times.
DECLARE
    l_type_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO l_type_exists
    FROM all_types
    WHERE owner = 'OPPAYMENTS' AND type_name = 'EPF_NUMBER_TAB';

    IF l_type_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE OR REPLACE TYPE oppayments.epf_number_tab AS TABLE OF NUMBER';
        DBMS_OUTPUT.PUT_LINE('EPF_NUMBER_TAB type created successfully.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('EPF_NUMBER_TAB type already exists. Skipping creation.');
    END IF;
END;
/

DECLARE
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

        DBMS_OUTPUT.PUT_LINE('EPF_PURGE_LOG table created successfully.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('EPF_PURGE_LOG table already exists. Skipping creation.');
    END IF;

    -- ========================================================================
    -- EPF_PURGE_SPACE_SNAPSHOT: captures segment sizes before/after purge
    -- ========================================================================
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

        DBMS_OUTPUT.PUT_LINE('EPF_PURGE_SPACE_SNAPSHOT table created successfully.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('EPF_PURGE_SPACE_SNAPSHOT table already exists. Skipping creation.');
    END IF;
END;
/

-- Migration for existing installs: add module column if it doesn't exist.
-- Tags each captured segment with its purge module so the comparison report
-- can group + filter by depth (PAYMENTS / LOGS / BANK_STATEMENTS / OTHER).
DECLARE
    l_col_exists NUMBER;
BEGIN
    SELECT COUNT(*) INTO l_col_exists
    FROM all_tab_columns
    WHERE owner = 'OPPAYMENTS'
      AND table_name = 'EPF_PURGE_SPACE_SNAPSHOT'
      AND column_name = 'MODULE';

    IF l_col_exists = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE oppayments.epf_purge_space_snapshot ADD (module VARCHAR2(20))';
        DBMS_OUTPUT.PUT_LINE('EPF_PURGE_SPACE_SNAPSHOT.module column added.');
    END IF;
END;
/
