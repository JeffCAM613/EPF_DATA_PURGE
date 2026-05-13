-- ============================================================================
-- EPF Data Purge - Recover Indexes / Constraints from DDL Backup
-- ============================================================================
-- Use this script when the reclaim session (05_reclaim_tablespace.sql) was
-- interrupted after dropping indexes/constraints but before recreating them.
-- It reads the DDL persisted in OPPAYMENTS.EPF_DDL_BACKUP and recreates
-- any objects that are currently missing from the database.
--
-- The script is SAFE to run at any time:
--   - Objects that already exist are silently skipped.
--   - Only the most recent backup set (latest run_id) is used.
--   - Objects are recreated in the correct order: indexes first, then
--     PK/UK constraints (which create their backing indexes), then FK
--     constraints (which reference the PKs).
--   - Successfully recreated objects are marked recreated='Y' in the
--     backup table.
--
-- Prerequisites:
--   * Run as SYS / SYSDBA (same as the reclaim script).
--   * The EPF_DDL_BACKUP table must exist (created by 01_create_purge_log_table.sql).
--
-- Parameters: none.
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET FEEDBACK ON

DECLARE
    v_run_id          RAW(16);
    v_backup_ts       TIMESTAMP;
    v_total           NUMBER := 0;
    v_recreated       NUMBER := 0;
    v_skipped         NUMBER := 0;
    v_errors          NUMBER := 0;
    v_error_list      VARCHAR2(4000) := NULL;

    -- Check if an index exists in the database.
    FUNCTION index_exists(p_owner VARCHAR2, p_name VARCHAR2) RETURN BOOLEAN IS
        l_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO l_cnt
          FROM dba_indexes
         WHERE owner = p_owner AND index_name = p_name;
        RETURN l_cnt > 0;
    END;

    -- Check if a constraint exists in the database.
    FUNCTION constraint_exists(p_owner VARCHAR2, p_name VARCHAR2) RETURN BOOLEAN IS
        l_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO l_cnt
          FROM dba_constraints
         WHERE owner = p_owner AND constraint_name = p_name;
        RETURN l_cnt > 0;
    END;

    -- Mark a backup row as recreated.
    PROCEDURE mark_recreated(p_backup_id NUMBER) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        UPDATE oppayments.epf_ddl_backup
           SET recreated = 'Y'
         WHERE backup_id = p_backup_id;
        COMMIT;
    EXCEPTION WHEN OTHERS THEN ROLLBACK;
    END;

BEGIN
    -- ========================================================================
    -- Find the most recent backup set
    -- ========================================================================
    BEGIN
        SELECT run_id, backup_timestamp
          INTO v_run_id, v_backup_ts
          FROM (
              SELECT run_id, MAX(backup_timestamp) AS backup_timestamp
                FROM oppayments.epf_ddl_backup
               GROUP BY run_id
               ORDER BY backup_timestamp DESC
          )
         WHERE ROWNUM = 1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: No DDL backup found in OPPAYMENTS.EPF_DDL_BACKUP.');
            DBMS_OUTPUT.PUT_LINE('       The reclaim script may not have persisted DDL yet,');
            DBMS_OUTPUT.PUT_LINE('       or the table has not been created (run 01_create_purge_log_table.sql).');
            RETURN;
    END;

    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  EPF INDEX / CONSTRAINT RECOVERY');
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  Using backup from: ' || TO_CHAR(v_backup_ts, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('  Run ID: ' || v_run_id);
    DBMS_OUTPUT.PUT_LINE('============================================================');

    -- Count objects in this backup set.
    SELECT COUNT(*) INTO v_total
      FROM oppayments.epf_ddl_backup
     WHERE run_id = v_run_id;

    DBMS_OUTPUT.PUT_LINE('  Objects in backup: ' || v_total);
    DBMS_OUTPUT.PUT_LINE('');

    -- Early exit if backup set is empty.
    IF v_total = 0 THEN
        DBMS_OUTPUT.PUT_LINE('  WARNING: Backup set is empty (0 objects). Nothing to recover.');
        DBMS_OUTPUT.PUT_LINE('  This run_id was likely written by a reclaim that found no');
        DBMS_OUTPUT.PUT_LINE('  indexes/constraints to capture.  Look for an older run_id:');
        DBMS_OUTPUT.PUT_LINE('    SELECT run_id, MIN(backup_timestamp), COUNT(*)');
        DBMS_OUTPUT.PUT_LINE('      FROM oppayments.epf_ddl_backup GROUP BY run_id');
        DBMS_OUTPUT.PUT_LINE('      ORDER BY MIN(backup_timestamp);');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        RETURN;
    END IF;

    -- Sanity-check: verify at least one DDL text is readable (catches
    -- corrupted CLOBs after CREATE TABLE AS SELECT recreations).
    DECLARE
        v_sample_len NUMBER := 0;
    BEGIN
        SELECT DBMS_LOB.GETLENGTH(ddl_text) INTO v_sample_len
          FROM (SELECT ddl_text FROM oppayments.epf_ddl_backup
                 WHERE run_id = v_run_id AND ddl_text IS NOT NULL
                 ORDER BY seq_num)
         WHERE ROWNUM = 1;
        IF v_sample_len = 0 THEN
            DBMS_OUTPUT.PUT_LINE('  WARNING: First DDL text is empty (0 bytes)!');
        ELSE
            DBMS_OUTPUT.PUT_LINE('  DDL sample length: ' || v_sample_len || ' chars (OK)');
        END IF;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('  WARNING: No readable DDL text found! Table may be corrupt.');
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('  WARNING: Cannot read DDL text: ' || SQLERRM);
    END;
    DBMS_OUTPUT.PUT_LINE('');

    -- ========================================================================
    -- Pass 1: Recreate non-constraint indexes
    -- ========================================================================
    DBMS_OUTPUT.PUT_LINE('--- Pass 1: Non-constraint indexes ---');
    FOR rec IN (
        SELECT backup_id, object_owner, object_name, ddl_text
          FROM oppayments.epf_ddl_backup
         WHERE run_id = v_run_id
           AND object_type = 'INDEX'
         ORDER BY seq_num
    ) LOOP
        BEGIN
            IF index_exists(rec.object_owner, rec.object_name) THEN
                v_skipped := v_skipped + 1;
            ELSE
                EXECUTE IMMEDIATE rec.ddl_text;
                v_recreated := v_recreated + 1;
                mark_recreated(rec.backup_id);
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                v_errors := v_errors + 1;
                v_error_list := SUBSTR(
                    NVL(v_error_list, '') || rec.object_owner || '.'
                    || rec.object_name || '(' || SQLCODE || '); ', 1, 4000);
                DBMS_OUTPUT.PUT_LINE('  ERROR: ' || rec.object_owner || '.'
                    || rec.object_name || ': ' || SUBSTR(SQLERRM, 1, 200));
        END;
        IF MOD(v_recreated + v_skipped + v_errors, 50) = 0 THEN
            DBMS_OUTPUT.PUT_LINE('  ... processed ' || (v_recreated + v_skipped + v_errors)
                || ' indexes (' || v_recreated || ' created)');
        END IF;
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('  Indexes: recreated=' || v_recreated
        || ', already_exist=' || v_skipped || ', errors=' || v_errors);

    -- ========================================================================
    -- Pass 2: Recreate PK / UK constraints
    -- ========================================================================
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('--- Pass 2: PK / UK constraints ---');
    DECLARE
        v_pk_recreated NUMBER := 0;
        v_pk_skipped   NUMBER := 0;
        v_pk_errors    NUMBER := 0;
    BEGIN
        FOR rec IN (
            SELECT backup_id, object_owner, object_name, table_name, ddl_text
              FROM oppayments.epf_ddl_backup
             WHERE run_id = v_run_id
               AND object_type = 'PK'
             ORDER BY seq_num
        ) LOOP
            BEGIN
                IF constraint_exists(rec.object_owner, rec.object_name) THEN
                    v_pk_skipped := v_pk_skipped + 1;
                    v_skipped := v_skipped + 1;
                ELSE
                    EXECUTE IMMEDIATE rec.ddl_text;
                    v_pk_recreated := v_pk_recreated + 1;
                    v_recreated := v_recreated + 1;
                    mark_recreated(rec.backup_id);
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    v_pk_errors := v_pk_errors + 1;
                    v_errors := v_errors + 1;
                    v_error_list := SUBSTR(
                        NVL(v_error_list, '') || rec.object_owner || '.'
                        || rec.object_name || '(' || SQLCODE || '); ', 1, 4000);
                    DBMS_OUTPUT.PUT_LINE('  ERROR: ' || rec.object_owner || '.'
                        || rec.object_name || ' on ' || rec.table_name
                        || ': ' || SUBSTR(SQLERRM, 1, 200));
            END;
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('  PK/UK: recreated=' || v_pk_recreated
            || ', already_exist=' || v_pk_skipped || ', errors=' || v_pk_errors);
    END;

    -- ========================================================================
    -- Pass 3: Recreate FK constraints
    -- ========================================================================
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('--- Pass 3: FK constraints ---');
    DECLARE
        v_fk_recreated NUMBER := 0;
        v_fk_skipped   NUMBER := 0;
        v_fk_errors    NUMBER := 0;
    BEGIN
        FOR rec IN (
            SELECT backup_id, object_owner, object_name, table_name, ddl_text
              FROM oppayments.epf_ddl_backup
             WHERE run_id = v_run_id
               AND object_type = 'FK'
             ORDER BY seq_num
        ) LOOP
            BEGIN
                IF constraint_exists(rec.object_owner, rec.object_name) THEN
                    v_fk_skipped := v_fk_skipped + 1;
                    v_skipped := v_skipped + 1;
                ELSE
                    EXECUTE IMMEDIATE rec.ddl_text;
                    v_fk_recreated := v_fk_recreated + 1;
                    v_recreated := v_recreated + 1;
                    mark_recreated(rec.backup_id);
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    v_fk_errors := v_fk_errors + 1;
                    v_errors := v_errors + 1;
                    v_error_list := SUBSTR(
                        NVL(v_error_list, '') || rec.object_owner || '.'
                        || rec.object_name || '(' || SQLCODE || '); ', 1, 4000);
                    DBMS_OUTPUT.PUT_LINE('  ERROR: ' || rec.object_owner || '.'
                        || rec.object_name || ' on ' || rec.table_name
                        || ': ' || SUBSTR(SQLERRM, 1, 200));
            END;
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('  FK: recreated=' || v_fk_recreated
            || ', already_exist=' || v_fk_skipped || ', errors=' || v_fk_errors);
    END;

    -- ========================================================================
    -- Summary
    -- ========================================================================
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  RECOVERY COMPLETE');
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  Total in backup:  ' || v_total);
    DBMS_OUTPUT.PUT_LINE('  Recreated:        ' || v_recreated);
    DBMS_OUTPUT.PUT_LINE('  Already existed:  ' || v_skipped);
    DBMS_OUTPUT.PUT_LINE('  Errors:           ' || v_errors);
    IF v_errors > 0 THEN
        DBMS_OUTPUT.PUT_LINE('  FAILED: ' || v_error_list);
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  To see all backed-up DDL for manual recovery:');
        DBMS_OUTPUT.PUT_LINE('    SELECT object_type, object_owner, object_name, ddl_text');
        DBMS_OUTPUT.PUT_LINE('      FROM oppayments.epf_ddl_backup');
        DBMS_OUTPUT.PUT_LINE('     WHERE run_id = ''' || v_run_id || '''');
        DBMS_OUTPUT.PUT_LINE('       AND recreated = ''N''');
        DBMS_OUTPUT.PUT_LINE('     ORDER BY object_type, seq_num;');
    END IF;
    IF v_recreated = 0 AND v_errors = 0 THEN
        DBMS_OUTPUT.PUT_LINE('  All objects already present. Nothing to recover.');
    END IF;

    -- Post-recovery: verify current database state.
    DECLARE
        v_idx_now NUMBER;
        v_pk_now  NUMBER;
        v_fk_now  NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_idx_now FROM dba_indexes
         WHERE owner IN ('OPPAYMENTS','OP')
           AND index_type IN ('NORMAL','UNIQUE','FUNCTION-BASED NORMAL',
                              'BITMAP','FUNCTION-BASED BITMAP');
        SELECT COUNT(*) INTO v_pk_now FROM dba_constraints
         WHERE owner IN ('OPPAYMENTS','OP') AND constraint_type IN ('P','U');
        SELECT COUNT(*) INTO v_fk_now FROM dba_constraints
         WHERE constraint_type = 'R'
           AND (owner IN ('OPPAYMENTS','OP') OR r_owner IN ('OPPAYMENTS','OP'));
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Verification (current DB state):');
        DBMS_OUTPUT.PUT_LINE('    Indexes:  ' || v_idx_now);
        DBMS_OUTPUT.PUT_LINE('    PK/UK:    ' || v_pk_now);
        DBMS_OUTPUT.PUT_LINE('    FK:       ' || v_fk_now);
    END;

    DBMS_OUTPUT.PUT_LINE('============================================================');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
        DBMS_OUTPUT.PUT_LINE('  UNHANDLED ERROR — recovery aborted!');
        DBMS_OUTPUT.PUT_LINE('  ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('  ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        DBMS_OUTPUT.PUT_LINE('  Progress so far: recreated=' || v_recreated
            || ', skipped=' || v_skipped || ', errors=' || v_errors);
        DBMS_OUTPUT.PUT_LINE('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
        RAISE;
END;
/
