-- ============================================================================
-- EPF Purge: Module Sizes + Purge Estimate (machine-readable)
-- ============================================================================
-- Accepts one positional parameter: retention days (e.g. 90).
-- Emits a single pipe-delimited line that the wrapper script can parse:
--
--   EPF_SIZES|PAY_GB|LOG_GB|BST_GB|TOTAL_GB|DATAFILE_GB
--            |EST_PAY|EST_LOG|EST_BST|EST_ALL|OTHER_GB|OTHER_PCT|COVERAGE_GB
--            |CLOB_PAY_GB|CLOB_LOG_GB|CLOB_BST_GB|CLOB_TOTAL_GB|CLOB_PCT
--
-- Fields 1-6:   segment sizes
-- Fields 7-12:  retention-based purge estimates + "other" (outside coverage)
-- Field  13:    total coverage GB (PAY+LOG+BST)
-- Fields 14-18: CLOB segment sizes per module + total + % of schema
--
-- Also emits a second line with per-table CLOB detail (legacy format):
--
--   EPF_CLOB_DETAIL|DIR_DISP|FILE_DISP|TX_AUD
--
-- Fields 2-4: CLOB GB for the 3 largest purge-covered tables with CLOBs.
--
-- Also emits one line per purge-covered table that has LOB columns:
--
--   EPF_CLOB_TABLE|MODULE:TABLE_NAME|GB
--
-- These lines are dynamically detected from user_lobs and sorted by size
-- descending. Used by the Purge Mode prompt to show all CLOB tables.
--
-- Used by the wrapper to:
--   - Show detailed purge coverage in the "Purge Depth" prompt
--   - Show retention-aware purge estimates per module
--   - Show CLOB composition in the "Purge Mode" prompt
--   - Recommend a sensible --max-iterations value based on tablespace size
--
-- Run as the OPPAYMENTS user. Uses DBA views (dba_segments / dba_lobs /
-- dba_data_files) when granted, falls back to user_segments otherwise.
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET HEADING OFF FEEDBACK OFF VERIFY OFF PAGESIZE 0 LINESIZE 300 ECHO OFF

DECLARE
    v_tablespace VARCHAR2(128);
    v_pay        NUMBER := 0;
    v_log        NUMBER := 0;
    v_bst        NUMBER := 0;
    v_total      NUMBER := 0;
    v_df_bytes   NUMBER := 0;

    -- Retention-based estimation
    v_retention  NUMBER := NVL(TO_NUMBER('&1'), 30);
    v_cutoff     DATE   := TRUNC(SYSDATE) - v_retention;
    v_bp_total   NUMBER := 0;
    v_bp_old     NUMBER := 0;
    v_at_total   NUMBER := 0;
    v_at_old     NUMBER := 0;
    v_fd_total   NUMBER := 0;
    v_fd_old     NUMBER := 0;
    v_pay_ratio  NUMBER := 0;
    v_log_ratio  NUMBER := 0;
    v_bst_ratio  NUMBER := 0;
    v_est_pay    NUMBER := 0;
    v_est_log    NUMBER := 0;
    v_est_bst    NUMBER := 0;
    v_est_all    NUMBER := 0;
    v_purge      NUMBER := 0;
    v_other      NUMBER := 0;
    v_other_pct  NUMBER := 0;

    -- CLOB (LOB segment) tracking per module
    v_lob_bytes  NUMBER := 0;
    v_clob_pay   NUMBER := 0;
    v_clob_log   NUMBER := 0;
    v_clob_bst   NUMBER := 0;
    v_clob_total NUMBER := 0;
    v_clob_pct   NUMBER := 0;

    -- Per-table CLOB sizes (3 purge-covered tables with significant CLOBs)
    v_clob_dir_disp  NUMBER := 0;
    v_clob_file_disp NUMBER := 0;
    v_clob_tx_aud    NUMBER := 0;

    -- Dynamic per-table CLOB detail (all purge-covered tables with LOBs)
    -- Using parallel scalar arrays (record types not allowed in anon blocks)
    TYPE t_vc128_tab IS TABLE OF VARCHAR2(128) INDEX BY PLS_INTEGER;
    TYPE t_vc4k_tab  IS TABLE OF VARCHAR2(4000) INDEX BY PLS_INTEGER;
    TYPE t_num_tab   IS TABLE OF NUMBER        INDEX BY PLS_INTEGER;
    v_cd_modules  t_vc128_tab;
    v_cd_tables   t_vc128_tab;
    v_cd_cols     t_vc4k_tab;
    v_cd_gbs      t_num_tab;
    v_cd_idx      PLS_INTEGER := 0;

    -- IMPORTANT: the DBA path uses dynamic SQL (OPEN ... FOR + EXECUTE
    -- IMMEDIATE) intentionally. Static references to dba_segments / dba_lobs /
    -- dba_data_files are resolved at COMPILE time, so when oppayments lacks
    -- SELECT on those views (e.g. fresh DB before grant_dba_views runs) the
    -- ENTIRE anonymous block fails to compile with ORA-00942, and the
    -- EXCEPTION handler never executes -- the operator just sees a raw
    -- "ERROR at line N" with no fallback. Dynamic SQL defers resolution to
    -- runtime, so the WHEN OTHERS handler catches the privilege error and
    -- falls back to user_segments cleanly.
    cur          SYS_REFCURSOR;
    v_owner      VARCHAR2(128);
    v_parent     VARCHAR2(128);
    v_bytes      NUMBER;
    v_lob_b      NUMBER;

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
    -- (dynamic SQL so missing dba_* grants surface as runtime exceptions
    -- caught by the WHEN OTHERS below, not as block-compile failures)
    -- ------------------------------------------------------------------
    BEGIN
        OPEN cur FOR
            'SELECT sg.owner,
                    COALESCE(l.table_name, sg.segment_name) AS parent_table,
                    SUM(sg.bytes) AS bytes,
                    SUM(CASE WHEN l.table_name IS NOT NULL THEN sg.bytes ELSE 0 END) AS lob_bytes
             FROM dba_segments sg
             LEFT JOIN dba_lobs l
                 ON l.owner = sg.owner AND l.segment_name = sg.segment_name
             WHERE sg.owner IN (''OPPAYMENTS'', ''OP'')
             GROUP BY sg.owner, COALESCE(l.table_name, sg.segment_name)';
        LOOP
            FETCH cur INTO v_owner, v_parent, v_bytes, v_lob_b;
            EXIT WHEN cur%NOTFOUND;
            v_total := v_total + v_bytes;
            v_clob_total := v_clob_total + v_lob_b;
            CASE classify(v_owner, v_parent)
                WHEN 'PAYMENTS'        THEN v_pay := v_pay + v_bytes; v_clob_pay := v_clob_pay + v_lob_b;
                WHEN 'LOGS'            THEN v_log := v_log + v_bytes; v_clob_log := v_clob_log + v_lob_b;
                WHEN 'BANK_STATEMENTS' THEN v_bst := v_bst + v_bytes; v_clob_bst := v_clob_bst + v_lob_b;
                ELSE NULL;  -- OTHER counted in v_total only
            END CASE;
        END LOOP;
        CLOSE cur;

        -- Datafile size: sum across all tablespaces holding OPPAYMENTS/OP data
        BEGIN
            EXECUTE IMMEDIATE
                'SELECT NVL(SUM(bytes), 0) FROM dba_data_files
                  WHERE tablespace_name IN (
                      SELECT DISTINCT tablespace_name FROM dba_segments
                       WHERE owner IN (''OPPAYMENTS'', ''OP''))'
                INTO v_df_bytes;
        EXCEPTION WHEN OTHERS THEN v_df_bytes := 0; END;
    EXCEPTION
        WHEN OTHERS THEN
            -- Fall back to user_segments. This misses op.spec_trt_log (in OP
            -- schema) and may miss any LOGS/BANK_STATEMENTS data outside this
            -- user's segments. Better than nothing.
            BEGIN IF cur%ISOPEN THEN CLOSE cur; END IF; EXCEPTION WHEN OTHERS THEN NULL; END;
            v_pay := 0; v_log := 0; v_bst := 0; v_total := 0;
            v_clob_pay := 0; v_clob_log := 0; v_clob_bst := 0; v_clob_total := 0;
            FOR rec IN (
                SELECT USER AS owner,
                       COALESCE(l.table_name, sg.segment_name) AS parent_table,
                       SUM(sg.bytes) AS bytes,
                       SUM(CASE WHEN l.table_name IS NOT NULL THEN sg.bytes ELSE 0 END) AS lob_bytes
                FROM user_segments sg
                LEFT JOIN user_lobs l ON l.segment_name = sg.segment_name
                GROUP BY COALESCE(l.table_name, sg.segment_name)
            ) LOOP
                v_total := v_total + rec.bytes;
                v_clob_total := v_clob_total + rec.lob_bytes;
                CASE classify(rec.owner, rec.parent_table)
                    WHEN 'PAYMENTS'        THEN v_pay := v_pay + rec.bytes; v_clob_pay := v_clob_pay + rec.lob_bytes;
                    WHEN 'LOGS'            THEN v_log := v_log + rec.bytes; v_clob_log := v_clob_log + rec.lob_bytes;
                    WHEN 'BANK_STATEMENTS' THEN v_bst := v_bst + rec.bytes; v_clob_bst := v_clob_bst + rec.lob_bytes;
                    ELSE NULL;
                END CASE;
            END LOOP;
            v_df_bytes := 0;  -- will retry dba_data_files below
    END;

    -- ------------------------------------------------------------------
    -- Retry dba_data_files independently if the DBA segments path failed
    -- (e.g. dba_segments not granted but dba_data_files is).
    -- ------------------------------------------------------------------
    IF v_df_bytes = 0 THEN
        BEGIN
            EXECUTE IMMEDIATE
                'SELECT NVL(SUM(bytes), 0) FROM dba_data_files
                  WHERE tablespace_name IN (
                      SELECT DISTINCT tablespace_name FROM dba_segments
                       WHERE owner IN (''OPPAYMENTS'', ''OP''))'
                INTO v_df_bytes;
        EXCEPTION WHEN OTHERS THEN v_df_bytes := 0; END;
    END IF;

    -- ------------------------------------------------------------------
    -- Retention-based purge estimate: ratio of old rows in root tables
    -- applied to each module's total segment size.
    -- Wrapped in its own block so table-not-found etc. does not kill the
    -- entire script — estimates simply stay at 0.
    -- ------------------------------------------------------------------
    BEGIN
        SELECT COUNT(*), COUNT(CASE WHEN value_date < v_cutoff THEN 1 END)
          INTO v_bp_total, v_bp_old FROM bulk_payment;
        SELECT COUNT(*), COUNT(CASE WHEN audit_timestamp < v_cutoff THEN 1 END)
          INTO v_at_total, v_at_old FROM audit_trail;
        SELECT COUNT(*), COUNT(CASE WHEN date_reception < v_cutoff THEN 1 END)
          INTO v_fd_total, v_fd_old FROM file_dispatching;

        IF v_bp_total > 0 THEN v_pay_ratio := v_bp_old / v_bp_total; END IF;
        IF v_at_total > 0 THEN v_log_ratio := v_at_old / v_at_total; END IF;
        IF v_fd_total > 0 THEN v_bst_ratio := v_fd_old / v_fd_total; END IF;
    EXCEPTION
        WHEN OTHERS THEN NULL;  -- estimates stay 0
    END;

    v_est_pay := ROUND(v_pay / 1073741824 * v_pay_ratio, 2);
    v_est_log := ROUND(v_log / 1073741824 * v_log_ratio, 2);
    v_est_bst := ROUND(v_bst / 1073741824 * v_bst_ratio, 2);
    v_est_all := v_est_pay + v_est_log + v_est_bst;
    v_purge   := v_pay + v_log + v_bst;
    v_other   := v_total - v_purge;
    IF v_total > 0 THEN
        v_other_pct := ROUND(v_other / v_total * 100, 1);
        v_clob_pct  := ROUND(v_clob_total / v_total * 100, 1);
    END IF;

    -- ------------------------------------------------------------------
    -- Per-table CLOB sizes: dynamically discover ALL purge-covered tables
    -- that have LOB columns. Uses user_lobs + user_segments.
    -- Also populates the legacy 3 variables for backward compatibility.
    -- NOTE: classify() is a PL/SQL function and cannot be called inside
    -- SQL in an anonymous block, so we iterate all tables and filter in PL/SQL.
    -- ------------------------------------------------------------------
    BEGIN
        FOR rec IN (
            SELECT l.table_name,
                   SUM(NVL(sg.bytes, 0)) AS lob_bytes,
                   LISTAGG(l.column_name, ', ') WITHIN GROUP (ORDER BY l.column_name) AS col_names
            FROM user_lobs l
            LEFT JOIN user_segments sg ON sg.segment_name = l.segment_name
            GROUP BY l.table_name
            HAVING SUM(NVL(sg.bytes, 0)) > 0
            ORDER BY SUM(NVL(sg.bytes, 0)) DESC
        ) LOOP
            -- Skip tables outside purge coverage
            IF classify(USER, rec.table_name) NOT IN ('PAYMENTS', 'LOGS', 'BANK_STATEMENTS') THEN
                CONTINUE;
            END IF;
            -- Legacy variables (for EPF_CLOB_DETAIL backward compat)
            CASE rec.table_name
                WHEN 'DIRECTORY_DISPATCHING'        THEN v_clob_dir_disp  := rec.lob_bytes;
                WHEN 'FILE_DISPATCHING'             THEN v_clob_file_disp := rec.lob_bytes;
                WHEN 'TRANSMISSION_EXECUTION_AUDIT' THEN v_clob_tx_aud    := rec.lob_bytes;
                ELSE NULL;
            END CASE;
            -- Dynamic collection
            v_cd_idx := v_cd_idx + 1;
            v_cd_modules(v_cd_idx) := classify(USER, rec.table_name);
            v_cd_tables(v_cd_idx)  := rec.table_name;
            v_cd_gbs(v_cd_idx)     := ROUND(rec.lob_bytes / 1073741824, 2);
            v_cd_cols(v_cd_idx)    := rec.col_names;
        END LOOP;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    -- ------------------------------------------------------------------
    -- Pipe-delimited line (18 fields). Wrapper greps for ^EPF_SIZES|.
    -- Fields 1-13:  PAY|LOG|BST|TOTAL|DATAFILE|EST_PAY|EST_LOG|EST_BST|EST_ALL|OTHER_GB|OTHER_PCT|COVERAGE_GB
    -- Fields 14-18: CLOB_PAY|CLOB_LOG|CLOB_BST|CLOB_TOTAL|CLOB_PCT
    -- ------------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE(
        'EPF_SIZES|'
        || TRIM(TO_CHAR(ROUND(v_pay      / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_log      / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_bst      / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_total    / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_df_bytes / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(v_est_pay,  '99990.00')) || '|'
        || TRIM(TO_CHAR(v_est_log,  '99990.00')) || '|'
        || TRIM(TO_CHAR(v_est_bst,  '99990.00')) || '|'
        || TRIM(TO_CHAR(v_est_all,  '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_other / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(v_other_pct, '990.0')) || '|'
        || TRIM(TO_CHAR(ROUND(v_purge / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_clob_pay   / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_clob_log   / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_clob_bst   / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_clob_total / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(v_clob_pct, '990.0'))
    );

    -- Per-table CLOB detail (legacy 3-field line for backward compat).
    -- Wrapper greps for ^EPF_CLOB_DETAIL|.
    DBMS_OUTPUT.PUT_LINE(
        'EPF_CLOB_DETAIL|'
        || TRIM(TO_CHAR(ROUND(v_clob_dir_disp  / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_clob_file_disp / 1073741824, 2), '99990.00')) || '|'
        || TRIM(TO_CHAR(ROUND(v_clob_tx_aud    / 1073741824, 2), '99990.00'))
    );

    -- Dynamic per-table CLOB lines: one line per purge-covered table with LOBs.
    -- Format: EPF_CLOB_TABLE|MODULE:TABLE_NAME|GB|COLUMN_NAMES
    -- Wrapper greps for ^EPF_CLOB_TABLE| to build the dynamic display.
    FOR i IN 1 .. v_cd_idx LOOP
        DBMS_OUTPUT.PUT_LINE(
            'EPF_CLOB_TABLE|'
            || v_cd_modules(i) || ':' || v_cd_tables(i) || '|'
            || TRIM(TO_CHAR(v_cd_gbs(i), '99990.00')) || '|'
            || v_cd_cols(i)
        );
    END LOOP;
EXCEPTION
    WHEN OTHERS THEN
        -- Emit a diagnostic line the wrapper can capture instead of failing
        -- silently with no output at all.
        DBMS_OUTPUT.PUT_LINE('EPF_ERROR|' || SQLERRM);
END;
/

EXIT;
