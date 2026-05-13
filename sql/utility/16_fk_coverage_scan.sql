-- ============================================================================
-- EPF Data Purge - FK Coverage Discovery
-- ============================================================================
-- Walks foreign key chains starting from the known purge-covered tables and
-- discovers ALL tables connected via FK relationships (parents + children,
-- recursively).  Tables not currently purged are flagged as gaps.
--
-- Output: one row per discovered table, showing:
--   - Schema, table name, segment size
--   - Whether it's currently purge-covered
--   - Which purge module it belongs to (PAYMENTS / LOGS / BANK_STATEMENTS)
--   - The FK path that connects it to a known table
--   - Whether it has date columns suitable for retention-based purge
--
-- Run as: SYS / SYSDBA (uses DBA_* views)
-- Usage:  sqlplus sys/<pwd>@<tns> AS SYSDBA @sql/16_fk_coverage_scan.sql
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 250
SET PAGESIZE 0
SET FEEDBACK OFF
SET VERIFY OFF
WHENEVER SQLERROR CONTINUE

DECLARE
    -- ---- Known purge-covered tables ----
    TYPE t_tab IS TABLE OF VARCHAR2(128);

    c_payments_tables CONSTANT t_tab := t_tab(
        'BULK_PAYMENT',
        'BULK_PAYMENT_ADDITIONAL_INFO',
        'BULK_SIGNATURE',
        'MANDATORY_SIGNERS',
        'OIDC_REQUEST_TOKEN',
        'PAYMENT',
        'PAYMENT_ADDITIONAL_INFO',
        'PAYMENT_AUDIT',
        'TRANSMISSION_EXECUTION_AUDIT',
        'IMPORT_AUDIT',
        'IMPORT_AUDIT_MESSAGES',
        'NOTIFICATION_EXECUTION',
        'TRANSMISSION_EXECUTION',
        'TRANSMISSION_EXCEPTION',
        'WORKFLOW_EXECUTION',
        'WORKFLOW_EXECUTION_OPT',
        'APPROBATION_EXECUTION',
        'APPROBATION_EXECUTION_OPT',
        'BULKPAYMENT_EXCEPTION',
        'INVOICE',
        'INVOICE_ADDITIONAL_INFO',
        'FILE_INTEGRATION'
    );

    c_logs_tables CONSTANT t_tab := t_tab(
        'AUDIT_TRAIL',
        'AUDIT_ARCHIVE',
        'SPEC_TRT_LOG'
    );

    c_bank_tables CONSTANT t_tab := t_tab(
        'FILE_DISPATCHING',
        'DIRECTORY_DISPATCHING'
    );

    -- ---- Discovery state ----
    TYPE t_table_info IS RECORD (
        owner       VARCHAR2(128),
        table_name  VARCHAR2(128),
        size_mb     NUMBER,
        covered     VARCHAR2(1),
        module      VARCHAR2(20),
        fk_path     VARCHAR2(4000),
        depth       NUMBER,
        has_date    VARCHAR2(200)
    );
    TYPE t_table_map IS TABLE OF t_table_info INDEX BY VARCHAR2(257); -- owner.table
    v_discovered t_table_map;

    TYPE t_queue_entry IS RECORD (
        owner       VARCHAR2(128),
        table_name  VARCHAR2(128),
        depth       NUMBER,
        path        VARCHAR2(4000)
    );
    TYPE t_queue IS TABLE OF t_queue_entry INDEX BY PLS_INTEGER;
    v_queue   t_queue;
    v_qi      PLS_INTEGER := 0;  -- queue write index
    v_qr      PLS_INTEGER := 1;  -- queue read index

    v_key     VARCHAR2(257);
    v_entry   t_table_info;
    v_cur     t_queue_entry;

    v_total_covered     NUMBER := 0;
    v_total_uncovered   NUMBER := 0;
    v_total_covered_mb  NUMBER := 0;
    v_total_uncovered_mb NUMBER := 0;
    v_total_standalone   NUMBER := 0;
    v_total_standalone_mb NUMBER := 0;

    -- ========================================================================
    FUNCTION get_module(p_table VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        FOR i IN 1 .. c_payments_tables.COUNT LOOP
            IF c_payments_tables(i) = p_table THEN RETURN 'PAYMENTS'; END IF;
        END LOOP;
        FOR i IN 1 .. c_logs_tables.COUNT LOOP
            IF c_logs_tables(i) = p_table THEN RETURN 'LOGS'; END IF;
        END LOOP;
        FOR i IN 1 .. c_bank_tables.COUNT LOOP
            IF c_bank_tables(i) = p_table THEN RETURN 'BANK_STATEMENTS'; END IF;
        END LOOP;
        RETURN NULL;
    END;

    FUNCTION is_covered(p_table VARCHAR2) RETURN BOOLEAN IS
    BEGIN
        RETURN get_module(p_table) IS NOT NULL;
    END;

    FUNCTION get_size_mb(p_owner VARCHAR2, p_table VARCHAR2) RETURN NUMBER IS
        v_mb NUMBER := 0;
    BEGIN
        SELECT NVL(SUM(bytes) / 1024 / 1024, 0) INTO v_mb
        FROM dba_segments
        WHERE owner = p_owner
          AND (segment_name = p_table
               OR segment_name IN (
                   SELECT segment_name FROM dba_lobs
                   WHERE owner = p_owner AND table_name = p_table
               )
               OR segment_name IN (
                   SELECT index_name FROM dba_lobs
                   WHERE owner = p_owner AND table_name = p_table
               ));
        RETURN ROUND(v_mb, 2);
    EXCEPTION WHEN OTHERS THEN RETURN 0;
    END;

    FUNCTION get_date_columns(p_owner VARCHAR2, p_table VARCHAR2) RETURN VARCHAR2 IS
        v_cols VARCHAR2(200) := '';
    BEGIN
        FOR c IN (
            SELECT column_name FROM dba_tab_columns
            WHERE owner = p_owner AND table_name = p_table
              AND data_type IN ('DATE', 'TIMESTAMP(6)', 'TIMESTAMP(6) WITH TIME ZONE')
            ORDER BY column_id
            FETCH FIRST 5 ROWS ONLY
        ) LOOP
            v_cols := v_cols || CASE WHEN v_cols IS NOT NULL THEN ', ' END || c.column_name;
        END LOOP;
        RETURN v_cols;
    EXCEPTION WHEN OTHERS THEN RETURN '(error)';
    END;

    PROCEDURE enqueue(p_owner VARCHAR2, p_table VARCHAR2, p_depth NUMBER, p_path VARCHAR2) IS
        l_key VARCHAR2(257) := p_owner || '.' || p_table;
    BEGIN
        IF v_discovered.EXISTS(l_key) THEN RETURN; END IF;
        IF p_depth > 10 THEN RETURN; END IF;  -- safety: max 10 levels deep

        v_qi := v_qi + 1;
        v_queue(v_qi).owner      := p_owner;
        v_queue(v_qi).table_name := p_table;
        v_queue(v_qi).depth      := p_depth;
        v_queue(v_qi).path       := p_path;

        -- Mark as discovered immediately to prevent duplicates
        v_entry.owner      := p_owner;
        v_entry.table_name := p_table;
        v_entry.size_mb    := get_size_mb(p_owner, p_table);
        v_entry.covered    := CASE WHEN is_covered(p_table) THEN 'Y' ELSE 'N' END;
        v_entry.module     := get_module(p_table);
        v_entry.fk_path    := p_path;
        v_entry.depth      := p_depth;
        v_entry.has_date   := get_date_columns(p_owner, p_table);
        v_discovered(l_key) := v_entry;
    END;

    -- ========================================================================
BEGIN
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  EPF PURGE COVERAGE - FK CHAIN DISCOVERY');
    DBMS_OUTPUT.PUT_LINE('  Database: ' || SYS_CONTEXT('USERENV', 'DB_NAME'));
    DBMS_OUTPUT.PUT_LINE('  Date:     ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('');

    -- Seed the queue with all known purge tables
    FOR i IN 1 .. c_payments_tables.COUNT LOOP
        FOR t IN (SELECT owner, table_name FROM dba_tables
                  WHERE table_name = c_payments_tables(i)
                    AND owner = 'OPPAYMENTS') LOOP
            enqueue(t.owner, t.table_name, 0, t.table_name);
        END LOOP;
    END LOOP;
    FOR i IN 1 .. c_logs_tables.COUNT LOOP
        FOR t IN (SELECT owner, table_name FROM dba_tables
                  WHERE table_name = c_logs_tables(i)
                    AND owner IN ('OPPAYMENTS', 'OP')) LOOP
            enqueue(t.owner, t.table_name, 0, t.table_name);
        END LOOP;
    END LOOP;
    FOR i IN 1 .. c_bank_tables.COUNT LOOP
        FOR t IN (SELECT owner, table_name FROM dba_tables
                  WHERE table_name = c_bank_tables(i)
                    AND owner = 'OPPAYMENTS') LOOP
            enqueue(t.owner, t.table_name, 0, t.table_name);
        END LOOP;
    END LOOP;

    -- BFS: walk FK chains downward (children only — tables whose FK references a known table)
    WHILE v_qr <= v_qi LOOP
        v_cur := v_queue(v_qr);
        v_qr := v_qr + 1;

        -- Children: tables whose FK references this table's PK/UK
        FOR fk IN (
            SELECT c.owner AS child_owner, c.table_name AS child_table,
                   c.constraint_name AS fk_name
            FROM dba_constraints c
            JOIN dba_constraints r
              ON c.r_owner = r.owner
             AND c.r_constraint_name = r.constraint_name
            WHERE r.owner = v_cur.owner
              AND r.table_name = v_cur.table_name
              AND c.constraint_type = 'R'
              AND c.owner IN ('OPPAYMENTS', 'OP')
        ) LOOP
            enqueue(fk.child_owner, fk.child_table, v_cur.depth + 1,
                    v_cur.path || ' -> ' || fk.child_table);
        END LOOP;
    END LOOP;

    -- ---- Report ----
    DBMS_OUTPUT.PUT_LINE('--- UNCOVERED TABLES (connected via FK but NOT purged) ---');
    DBMS_OUTPUT.PUT_LINE(RPAD('Owner.Table', 50) || RPAD('Size(MB)', 12)
        || RPAD('Depth', 7) || RPAD('Date Cols', 40) || 'FK Path');
    DBMS_OUTPUT.PUT_LINE(RPAD('-', 50, '-') || RPAD('-', 12, '-')
        || RPAD('-', 7, '-') || RPAD('-', 40, '-') || RPAD('-', 80, '-'));

    v_key := v_discovered.FIRST;
    WHILE v_key IS NOT NULL LOOP
        v_entry := v_discovered(v_key);
        IF v_entry.covered = 'N' THEN
            v_total_uncovered := v_total_uncovered + 1;
            v_total_uncovered_mb := v_total_uncovered_mb + v_entry.size_mb;
            DBMS_OUTPUT.PUT_LINE(
                RPAD(v_entry.owner || '.' || v_entry.table_name, 50)
                || RPAD(TO_CHAR(v_entry.size_mb, '99,999.99'), 12)
                || RPAD(v_entry.depth, 7)
                || RPAD(NVL(v_entry.has_date, '(none)'), 40)
                || v_entry.fk_path
            );
        END IF;
        v_key := v_discovered.NEXT(v_key);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('--- COVERED TABLES (already in purge) ---');
    DBMS_OUTPUT.PUT_LINE(RPAD('Owner.Table', 50) || RPAD('Size(MB)', 12) || 'Module');
    DBMS_OUTPUT.PUT_LINE(RPAD('-', 50, '-') || RPAD('-', 12, '-') || RPAD('-', 20, '-'));

    v_key := v_discovered.FIRST;
    WHILE v_key IS NOT NULL LOOP
        v_entry := v_discovered(v_key);
        IF v_entry.covered = 'Y' THEN
            v_total_covered := v_total_covered + 1;
            v_total_covered_mb := v_total_covered_mb + v_entry.size_mb;
            DBMS_OUTPUT.PUT_LINE(
                RPAD(v_entry.owner || '.' || v_entry.table_name, 50)
                || RPAD(TO_CHAR(v_entry.size_mb, '99,999.99'), 12)
                || v_entry.module
            );
        END IF;
        v_key := v_discovered.NEXT(v_key);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('--- STANDALONE TABLES (OPPAYMENTS/OP, no FK link to purge tables) ---');
    DBMS_OUTPUT.PUT_LINE(RPAD('Owner.Table', 50) || RPAD('Size(MB)', 12)
        || RPAD('Rows', 14) || 'Date Cols');
    DBMS_OUTPUT.PUT_LINE(RPAD('-', 50, '-') || RPAD('-', 12, '-')
        || RPAD('-', 14, '-') || RPAD('-', 60, '-'));

    FOR t IN (
        SELECT t.owner, t.table_name, t.num_rows
        FROM dba_tables t
        WHERE t.owner IN ('OPPAYMENTS', 'OP')
          AND t.table_name NOT LIKE 'EPF_%'
          AND t.table_name NOT LIKE 'BIN$%'
          AND t.table_name NOT LIKE 'SYS_%'
        ORDER BY t.owner, t.table_name
    ) LOOP
        IF NOT v_discovered.EXISTS(t.owner || '.' || t.table_name) THEN
            DECLARE
                l_mb   NUMBER := get_size_mb(t.owner, t.table_name);
                l_date VARCHAR2(200) := get_date_columns(t.owner, t.table_name);
                l_rows VARCHAR2(14);
            BEGIN
                IF t.num_rows IS NOT NULL THEN
                    l_rows := TRIM(TO_CHAR(t.num_rows, '999,999,999'));
                ELSE
                    l_rows := '(no stats)';
                END IF;
                v_total_standalone := v_total_standalone + 1;
                v_total_standalone_mb := v_total_standalone_mb + l_mb;
                DBMS_OUTPUT.PUT_LINE(
                    RPAD(t.owner || '.' || t.table_name, 50)
                    || RPAD(TO_CHAR(l_mb, '99,999.99'), 12)
                    || RPAD(l_rows, 14)
                    || NVL(l_date, '(none)')
                );
            END;
        END IF;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  SUMMARY');
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  Covered tables:    ' || v_total_covered
        || '  (' || ROUND(v_total_covered_mb, 1) || ' MB)');
    DBMS_OUTPUT.PUT_LINE('  Uncovered (FK):    ' || v_total_uncovered
        || '  (' || ROUND(v_total_uncovered_mb, 1) || ' MB)');
    DBMS_OUTPUT.PUT_LINE('  Standalone:        ' || v_total_standalone
        || '  (' || ROUND(v_total_standalone_mb, 1) || ' MB)');
    DBMS_OUTPUT.PUT_LINE('  Total discovered:  ' || (v_total_covered + v_total_uncovered));
    DBMS_OUTPUT.PUT_LINE('  Max FK depth:      ' || v_qi);
    DBMS_OUTPUT.PUT_LINE('============================================================');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('*** FATAL ERROR ***');
        DBMS_OUTPUT.PUT_LINE('  SQLCODE: ' || SQLCODE);
        DBMS_OUTPUT.PUT_LINE('  SQLERRM: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('  At queue position ' || v_qr || ' of ' || v_qi);
        IF v_qr > 1 AND v_queue.EXISTS(v_qr - 1) THEN
            DBMS_OUTPUT.PUT_LINE('  Last table: ' || v_queue(v_qr - 1).owner || '.' || v_queue(v_qr - 1).table_name);
        END IF;
END;
/
EXIT;
