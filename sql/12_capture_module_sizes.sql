-- ============================================================================
-- EPF Purge: Module Sizes (machine-readable)
-- ============================================================================
-- Emits a single pipe-delimited line that the wrapper script can parse:
--
--   EPF_SIZES|PAYMENTS_GB|LOGS_GB|BANK_STATEMENTS_GB|TOTAL_GB|DATAFILE_GB
--
-- Used by the wrapper to:
--   - Show GB next to each option in the "Purge Depth" prompt
--   - Recommend a sensible --max-iterations value based on tablespace size
--
-- Run as the OPPAYMENTS user. Uses DBA views (dba_segments / dba_lobs /
-- dba_data_files) when granted, falls back to user_segments otherwise.
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET HEADING OFF FEEDBACK OFF VERIFY OFF PAGESIZE 0 LINESIZE 200 ECHO OFF
SET DEFINE OFF

DECLARE
    v_tablespace VARCHAR2(128);
    v_pay        NUMBER := 0;
    v_log        NUMBER := 0;
    v_bst        NUMBER := 0;
    v_total      NUMBER := 0;
    v_df_bytes   NUMBER := 0;

    -- Module classification helper (same logic as capture_space_snapshot
    -- in epf_purge_pkg).
    FUNCTION classify(p_owner VARCHAR2, p_table VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        IF UPPER(p_table) IN (
            'BULK_PAYMENT', 'BULK_PAYMENT_ADDITIONAL_INFO', 'BULK_SIGNATURE',
            'MANDATORY_SIGNERS', 'OIDC_REQUEST_TOKEN', 'PAYMENT',
            'PAYMENT_ADDITIONAL_INFO', 'PAYMENT_AUDIT', 'IMPORT_AUDIT',
            'IMPORT_AUDIT_MESSAGES', 'TRANSMISSION_EXECUTION',
            'TRANSMISSION_EXECUTION_AUDIT', 'TRANSMISSION_EXCEPTION',
            'NOTIFICATION_EXECUTION', 'APPROBATION_EXECUTION',
            'APPROBATION_EXECUTION_OPT', 'WORKFLOW_EXECUTION',
            'WORKFLOW_EXECUTION_OPT', 'BULKPAYMENT_EXCEPTION',
            'INVOICE', 'INVOICE_ADDITIONAL_INFO', 'FILE_INTEGRATION'
        ) THEN
            RETURN 'PAYMENTS';
        ELSIF UPPER(p_table) IN ('AUDIT_TRAIL', 'AUDIT_ARCHIVE') THEN
            RETURN 'LOGS';
        ELSIF UPPER(p_owner) = 'OP' AND UPPER(p_table) = 'SPEC_TRT_LOG' THEN
            RETURN 'LOGS';
        ELSIF UPPER(p_table) IN ('DIRECTORY_DISPATCHING', 'FILE_DISPATCHING') THEN
            RETURN 'BANK_STATEMENTS';
        ELSE
            RETURN 'OTHER';
        END IF;
    END;
BEGIN
    BEGIN
        SELECT default_tablespace INTO v_tablespace FROM user_users;
    EXCEPTION WHEN OTHERS THEN v_tablespace := NULL; END;

    -- ------------------------------------------------------------------
    -- Try DBA path first: full coverage including op.spec_trt_log
    -- ------------------------------------------------------------------
    BEGIN
        FOR rec IN (
            SELECT sg.owner,
                   COALESCE(l.table_name, sg.segment_name) AS parent_table,
                   SUM(sg.bytes) AS bytes
            FROM dba_segments sg
            LEFT JOIN dba_lobs l
                ON l.owner = sg.owner AND l.segment_name = sg.segment_name
            WHERE sg.tablespace_name = v_tablespace
               OR (sg.owner = 'OP' AND sg.segment_name = 'SPEC_TRT_LOG')
            GROUP BY sg.owner, COALESCE(l.table_name, sg.segment_name)
        ) LOOP
            v_total := v_total + rec.bytes;
            CASE classify(rec.owner, rec.parent_table)
                WHEN 'PAYMENTS'        THEN v_pay := v_pay + rec.bytes;
                WHEN 'LOGS'            THEN v_log := v_log + rec.bytes;
                WHEN 'BANK_STATEMENTS' THEN v_bst := v_bst + rec.bytes;
                ELSE NULL;  -- OTHER counted in v_total only
            END CASE;
        END LOOP;

        -- Datafile size of OPPAYMENTS default tablespace (for max-iter hint)
        BEGIN
            SELECT NVL(SUM(bytes), 0) INTO v_df_bytes
            FROM dba_data_files
            WHERE tablespace_name = v_tablespace;
        EXCEPTION WHEN OTHERS THEN v_df_bytes := 0; END;
    EXCEPTION
        WHEN OTHERS THEN
            -- Fall back to user_segments. This misses op.spec_trt_log (in OP
            -- schema) and may miss any LOGS/BANK_STATEMENTS data outside this
            -- user's segments. Better than nothing.
            v_pay := 0; v_log := 0; v_bst := 0; v_total := 0;
            FOR rec IN (
                SELECT USER AS owner,
                       COALESCE(l.table_name, sg.segment_name) AS parent_table,
                       SUM(sg.bytes) AS bytes
                FROM user_segments sg
                LEFT JOIN user_lobs l ON l.segment_name = sg.segment_name
                GROUP BY COALESCE(l.table_name, sg.segment_name)
            ) LOOP
                v_total := v_total + rec.bytes;
                CASE classify(rec.owner, rec.parent_table)
                    WHEN 'PAYMENTS'        THEN v_pay := v_pay + rec.bytes;
                    WHEN 'LOGS'            THEN v_log := v_log + rec.bytes;
                    WHEN 'BANK_STATEMENTS' THEN v_bst := v_bst + rec.bytes;
                    ELSE NULL;
                END CASE;
            END LOOP;
            v_df_bytes := 0;  -- no DBA view => no datafile size
    END;

    -- ------------------------------------------------------------------
    -- Single pipe-delimited line. Wrapper greps for ^EPF_SIZES| prefix.
    -- ------------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE(
        'EPF_SIZES|'
        || TRIM(TO_CHAR(ROUND(v_pay   / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_log   / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_bst   / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_total / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_df_bytes / 1073741824, 2), '99990.00'))
    );
EXCEPTION
    WHEN OTHERS THEN
        -- Emit a diagnostic line the wrapper can capture instead of failing
        -- silently with no output at all.
        DBMS_OUTPUT.PUT_LINE('EPF_ERROR|' || SQLERRM);
END;
/

EXIT;
