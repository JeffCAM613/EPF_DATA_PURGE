-- ============================================================================
-- EPF Data Purge - Reclaim Recovery
-- ============================================================================
-- Comprehensive recovery script for when 05_reclaim_tablespace.sql fails
-- mid-run. Diagnoses the current state, moves any stranded tables back
-- from EPF_SCRATCH to the original DATA tablespace, drops EPF_SCRATCH,
-- and recreates all missing indexes/constraints from EPF_DDL_BACKUP.
--
-- This script is SAFE to run at any time — even on a healthy database:
--   - If no recovery is needed, it detects that and exits cleanly.
--   - Table moves skip tables already in the correct tablespace.
--   - Index/constraint recreation skips objects that already exist.
--   - EPF_SCRATCH is only dropped when empty.
--
-- Failure scenarios handled:
--
--   Scenario A: Crash during DRAIN (DATA -> SCRATCH)
--     Tables are split: some in DATA, some in SCRATCH.
--     Indexes/constraints are dropped.
--     Recovery: move all SCRATCH tables back to DATA, recreate objects.
--
--   Scenario B: Crash during REFILL (SCRATCH -> DATA)
--     Tables are split: some still in SCRATCH (refill incomplete).
--     Indexes/constraints are dropped.
--     Recovery: move remaining SCRATCH tables back to DATA, recreate objects.
--
--   Scenario C: Crash during RECREATE (indexes/constraints)
--     All tables are back in DATA.
--     Some indexes/constraints partially recreated.
--     Recovery: recreate remaining objects from backup.
--
--   Scenario D: SCRATCH left behind (drop failed or segments remain)
--     All tables may or may not be in DATA.
--     Recovery: move stranded tables, drop SCRATCH.
--
--   Scenario E: Nothing wrong (healthy state)
--     Script detects no issues and exits.
--
-- Prerequisites:
--   * Run as SYS / SYSDBA
--   * The EPF_DDL_BACKUP table must exist and contain DDL from the
--     failed reclaim run (created automatically by 05_reclaim_tablespace.sql).
--   * If EPF_DDL_BACKUP is missing or empty, index/constraint recovery
--     is skipped — manual DDL execution required.
--
-- Parameters: none.
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET FEEDBACK ON

DECLARE
    -- ---- Schemas in scope ----
    c_owner_oppay       CONSTANT VARCHAR2(30) := 'OPPAYMENTS';
    c_owner_op          CONSTANT VARCHAR2(30) := 'OP';

    -- ---- Discovered state ----
    v_data_ts           VARCHAR2(128);
    v_block_size        NUMBER;
    v_scratch_exists    BOOLEAN := FALSE;
    v_scratch_seg_count NUMBER := 0;
    v_tables_in_scratch NUMBER := 0;
    v_missing_indexes   NUMBER := 0;
    v_missing_pk        NUMBER := 0;
    v_missing_fk        NUMBER := 0;
    v_backup_exists     BOOLEAN := FALSE;
    v_backup_count      NUMBER := 0;
    v_needs_recovery    BOOLEAN := FALSE;

    -- ---- Counters ----
    v_moved_back        NUMBER := 0;
    v_move_errors       NUMBER := 0;
    v_recreated_idx     NUMBER := 0;
    v_recreated_pk      NUMBER := 0;
    v_recreated_fk      NUMBER := 0;
    v_skipped           NUMBER := 0;
    v_recreate_errors   NUMBER := 0;
    v_error_list        VARCHAR2(4000) := NULL;

    -- ---- DDL backup ----
    v_run_id            RAW(16);

    -- ---- Helpers ----
    v_data_file         VARCHAR2(512);

    -- Build a MOVE SQL statement including LOB STORE AS clauses.
    FUNCTION build_move_sql(
        p_owner     VARCHAR2,
        p_table     VARCHAR2,
        p_target_ts VARCHAR2
    ) RETURN VARCHAR2 IS
        v_sql VARCHAR2(4000);
    BEGIN
        v_sql := 'ALTER TABLE "' || p_owner || '"."' || p_table
              || '" MOVE TABLESPACE ' || p_target_ts;
        FOR lob IN (
            SELECT column_name FROM dba_lobs
             WHERE owner = p_owner AND table_name = p_table
             ORDER BY column_name
        ) LOOP
            v_sql := v_sql || ' LOB ("' || lob.column_name
                  || '") STORE AS (TABLESPACE ' || p_target_ts || ')';
        END LOOP;
        RETURN v_sql;
    END;

    -- Check if an index exists.
    FUNCTION index_exists(p_owner VARCHAR2, p_name VARCHAR2) RETURN BOOLEAN IS
        l_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO l_cnt FROM dba_indexes
         WHERE owner = p_owner AND index_name = p_name;
        RETURN l_cnt > 0;
    END;

    -- Check if a constraint exists.
    FUNCTION constraint_exists(p_owner VARCHAR2, p_name VARCHAR2) RETURN BOOLEAN IS
        l_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO l_cnt FROM dba_constraints
         WHERE owner = p_owner AND constraint_name = p_name;
        RETURN l_cnt > 0;
    END;

    -- Resize a single datafile to HWM + margin.
    PROCEDURE resize_datafile(
        p_datafile  VARCHAR2,
        p_ts        VARCHAR2,
        p_margin_mb NUMBER DEFAULT 50
    ) IS
        v_hwm_bytes NUMBER;
        v_cur_bytes NUMBER;
        v_new_bytes NUMBER;
    BEGIN
        SELECT NVL(MAX(block_id + blocks), 0) * v_block_size
          INTO v_hwm_bytes FROM dba_extents WHERE tablespace_name = p_ts;

        SELECT bytes INTO v_cur_bytes FROM dba_data_files
         WHERE file_name = p_datafile AND ROWNUM = 1;

        v_new_bytes := v_hwm_bytes + p_margin_mb * 1024 * 1024;
        v_new_bytes := GREATEST(v_new_bytes, 10 * 1024 * 1024);

        IF v_new_bytes >= v_cur_bytes THEN RETURN; END IF;

        BEGIN
            EXECUTE IMMEDIATE 'ALTER DATABASE DATAFILE '''
                || REPLACE(p_datafile, '''', '''''')
                || ''' RESIZE ' || v_new_bytes;
            DBMS_OUTPUT.PUT_LINE('    Resized: '
                || ROUND(v_cur_bytes/1024/1024/1024, 2) || ' GB -> '
                || ROUND(v_new_bytes/1024/1024/1024, 2) || ' GB');
        EXCEPTION WHEN OTHERS THEN
            IF SQLCODE = -3297 THEN
                FOR add_gb IN 1..10 LOOP
                    v_new_bytes := v_hwm_bytes + p_margin_mb * 1024 * 1024
                                 + add_gb * 1024 * 1024 * 1024;
                    IF v_new_bytes >= v_cur_bytes THEN RETURN; END IF;
                    BEGIN
                        EXECUTE IMMEDIATE 'ALTER DATABASE DATAFILE '''
                            || REPLACE(p_datafile, '''', '''''')
                            || ''' RESIZE ' || v_new_bytes;
                        DBMS_OUTPUT.PUT_LINE('    Resized (with +' || add_gb
                            || 'GB headroom): '
                            || ROUND(v_cur_bytes/1024/1024/1024, 2) || ' GB -> '
                            || ROUND(v_new_bytes/1024/1024/1024, 2) || ' GB');
                        RETURN;
                    EXCEPTION WHEN OTHERS THEN
                        IF SQLCODE != -3297 THEN RAISE; END IF;
                    END;
                END LOOP;
                DBMS_OUTPUT.PUT_LINE('    WARNING: Could not resize '
                    || p_ts || ' datafile (ORA-03297 after 10 retries).');
            ELSE
                DBMS_OUTPUT.PUT_LINE('    WARNING: Resize failed: '
                    || SUBSTR(SQLERRM, 1, 200));
            END IF;
        END;
    END resize_datafile;

BEGIN
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  EPF RECLAIM RECOVERY');
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('');

    -- ========================================================================
    -- PHASE 1: DIAGNOSE — Detect the current state
    -- ========================================================================
    DBMS_OUTPUT.PUT_LINE('--- Phase 1: Diagnosing current state ---');
    DBMS_OUTPUT.PUT_LINE('');

    -- 1a. Get block size.
    EXECUTE IMMEDIATE 'SELECT value FROM v$parameter WHERE name = ''db_block_size'''
        INTO v_block_size;

    -- 1b. Discover DATA tablespace.
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT default_tablespace FROM dba_users WHERE username = :1'
            INTO v_data_ts USING c_owner_oppay;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('  ERROR: ' || c_owner_oppay || ' user not found.');
            DBMS_OUTPUT.PUT_LINE('         Cannot determine DATA tablespace.');
            DBMS_OUTPUT.PUT_LINE('============================================================');
            RETURN;
    END;
    DBMS_OUTPUT.PUT_LINE('  DATA tablespace: ' || v_data_ts);

    -- Look up primary data file.
    SELECT file_name INTO v_data_file FROM dba_data_files
     WHERE tablespace_name = v_data_ts AND ROWNUM = 1;

    -- 1c. Check if EPF_SCRATCH exists.
    DECLARE
        v_cnt NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_cnt FROM dba_tablespaces
         WHERE tablespace_name = 'EPF_SCRATCH';
        v_scratch_exists := (v_cnt > 0);
    END;

    IF v_scratch_exists THEN
        -- Count segments in SCRATCH.
        SELECT COUNT(*) INTO v_scratch_seg_count
          FROM dba_segments WHERE tablespace_name = 'EPF_SCRATCH';

        -- Count OPPAYMENTS/OP tables in SCRATCH.
        SELECT COUNT(*) INTO v_tables_in_scratch
          FROM dba_tables
         WHERE owner IN (c_owner_oppay, c_owner_op)
           AND tablespace_name = 'EPF_SCRATCH';

        DBMS_OUTPUT.PUT_LINE('  EPF_SCRATCH:     EXISTS (' || v_scratch_seg_count
            || ' segments, ' || v_tables_in_scratch || ' tables)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('  EPF_SCRATCH:     does not exist');
    END IF;

    -- 1d. Count missing indexes/constraints compared to DDL backup.
    DECLARE
        v_tbl_exists NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_tbl_exists FROM all_tables
         WHERE owner = c_owner_oppay AND table_name = 'EPF_DDL_BACKUP';
        v_backup_exists := (v_tbl_exists > 0);
    END;

    IF v_backup_exists THEN
        -- Find the most recent backup set.
        BEGIN
            SELECT run_id INTO v_run_id
              FROM (
                  SELECT run_id FROM oppayments.epf_ddl_backup
                   GROUP BY run_id ORDER BY MAX(backup_timestamp) DESC
              ) WHERE ROWNUM = 1;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                v_backup_exists := FALSE;
                v_run_id := NULL;
        END;
    END IF;

    IF v_backup_exists AND v_run_id IS NOT NULL THEN
        SELECT COUNT(*) INTO v_backup_count
          FROM oppayments.epf_ddl_backup WHERE run_id = v_run_id;

        -- Count backed-up indexes that are currently missing.
        SELECT COUNT(*) INTO v_missing_indexes
          FROM oppayments.epf_ddl_backup b
         WHERE b.run_id = v_run_id
           AND b.object_type = 'INDEX'
           AND NOT EXISTS (
               SELECT 1 FROM dba_indexes i
                WHERE i.owner = b.object_owner AND i.index_name = b.object_name
           );

        -- Count backed-up PK/UK that are currently missing.
        SELECT COUNT(*) INTO v_missing_pk
          FROM oppayments.epf_ddl_backup b
         WHERE b.run_id = v_run_id
           AND b.object_type IN ('PK', 'UK')
           AND NOT EXISTS (
               SELECT 1 FROM dba_constraints c
                WHERE c.owner = b.object_owner AND c.constraint_name = b.object_name
           );

        -- Count backed-up FK that are currently missing.
        SELECT COUNT(*) INTO v_missing_fk
          FROM oppayments.epf_ddl_backup b
         WHERE b.run_id = v_run_id
           AND b.object_type = 'FK'
           AND NOT EXISTS (
               SELECT 1 FROM dba_constraints c
                WHERE c.owner = b.object_owner AND c.constraint_name = b.object_name
           );

        DBMS_OUTPUT.PUT_LINE('  DDL backup:      ' || v_backup_count || ' objects'
            || ' (run_id=' || RAWTOHEX(v_run_id) || ')');
        DBMS_OUTPUT.PUT_LINE('  Missing indexes: ' || v_missing_indexes);
        DBMS_OUTPUT.PUT_LINE('  Missing PK/UK:   ' || v_missing_pk);
        DBMS_OUTPUT.PUT_LINE('  Missing FK:      ' || v_missing_fk);
    ELSE
        DBMS_OUTPUT.PUT_LINE('  DDL backup:      NOT AVAILABLE');
        DBMS_OUTPUT.PUT_LINE('                   (EPF_DDL_BACKUP is empty or missing)');

        -- Fall back: count actual indexes/constraints to show current state.
        DECLARE
            v_idx_now NUMBER;
            v_pk_now  NUMBER;
            v_fk_now  NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_idx_now FROM dba_indexes
             WHERE owner IN (c_owner_oppay, c_owner_op)
               AND index_type IN ('NORMAL','UNIQUE','FUNCTION-BASED NORMAL',
                                  'BITMAP','FUNCTION-BASED BITMAP');
            SELECT COUNT(*) INTO v_pk_now FROM dba_constraints
             WHERE owner IN (c_owner_oppay, c_owner_op)
               AND constraint_type IN ('P','U');
            SELECT COUNT(*) INTO v_fk_now FROM dba_constraints
             WHERE constraint_type = 'R'
               AND (owner IN (c_owner_oppay, c_owner_op)
                    OR r_owner IN (c_owner_oppay, c_owner_op));
            DBMS_OUTPUT.PUT_LINE('  Current DB has:  '
                || v_idx_now || ' indexes, '
                || v_pk_now || ' PK/UK, '
                || v_fk_now || ' FK');
            IF v_idx_now = 0 AND v_pk_now = 0 AND v_fk_now = 0 THEN
                DBMS_OUTPUT.PUT_LINE('  WARNING: Zero indexes and constraints!');
                DBMS_OUTPUT.PUT_LINE('           Recovery requires DDL backup or a metadata export.');
            END IF;
        END;
    END IF;

    -- 1e. Also list any tables with LOB segments stranded in SCRATCH.
    IF v_scratch_exists AND v_tables_in_scratch > 0 THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Tables currently in EPF_SCRATCH:');
        FOR rec IN (
            SELECT owner, table_name,
                   ROUND(SUM(bytes) / 1024 / 1024, 1) AS size_mb
              FROM (
                  -- Table segments in SCRATCH
                  SELECT s.owner, s.segment_name AS table_name, s.bytes
                    FROM dba_segments s
                   WHERE s.tablespace_name = 'EPF_SCRATCH'
                     AND s.owner IN (c_owner_oppay, c_owner_op)
                     AND s.segment_type = 'TABLE'
                  UNION ALL
                  -- LOB segments in SCRATCH resolved to parent table
                  SELECT l.owner, l.table_name, s.bytes
                    FROM dba_lobs l
                    JOIN dba_segments s ON s.owner = l.owner
                         AND s.segment_name = l.segment_name
                   WHERE s.tablespace_name = 'EPF_SCRATCH'
                     AND s.segment_type = 'LOBSEGMENT'
                     AND l.owner IN (c_owner_oppay, c_owner_op)
              )
             GROUP BY owner, table_name
             ORDER BY SUM(bytes) DESC
        ) LOOP
            DBMS_OUTPUT.PUT_LINE('    ' || rec.owner || '.' || rec.table_name
                || ' (' || rec.size_mb || ' MB)');
        END LOOP;
    END IF;

    -- 1f. Determine if recovery is needed.
    v_needs_recovery := v_tables_in_scratch > 0
                     OR v_missing_indexes > 0
                     OR v_missing_pk > 0
                     OR v_missing_fk > 0
                     OR (v_scratch_exists AND v_scratch_seg_count > 0);

    IF NOT v_needs_recovery THEN
        -- Check if SCRATCH exists but is empty (just needs cleanup).
        IF v_scratch_exists AND v_scratch_seg_count = 0 THEN
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('  EPF_SCRATCH is empty — dropping it.');
            EXECUTE IMMEDIATE 'DROP TABLESPACE EPF_SCRATCH INCLUDING CONTENTS AND DATAFILES';
            DBMS_OUTPUT.PUT_LINE('  EPF_SCRATCH dropped.');
        END IF;

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('  NO RECOVERY NEEDED');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('  All tables are in the correct tablespace.');
        DBMS_OUTPUT.PUT_LINE('  All indexes and constraints are present.');
        DBMS_OUTPUT.PUT_LINE('  EPF_SCRATCH does not exist (or was just cleaned up).');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        RETURN;
    END IF;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('  ** RECOVERY IS NEEDED **');
    DBMS_OUTPUT.PUT_LINE('');

    -- ========================================================================
    -- PHASE 2: REPATRIATE — Move stranded tables from SCRATCH back to DATA
    -- ========================================================================
    IF v_tables_in_scratch > 0 THEN
        DBMS_OUTPUT.PUT_LINE('--- Phase 2: Moving ' || v_tables_in_scratch
            || ' tables from EPF_SCRATCH back to ' || v_data_ts || ' ---');
        DBMS_OUTPUT.PUT_LINE('');

        -- Ensure AUTOEXTEND ON so DATA can grow to accept returning tables.
        BEGIN
            EXECUTE IMMEDIATE 'ALTER DATABASE DATAFILE '''
                || REPLACE(v_data_file, '''', '''''')
                || ''' AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED';
        EXCEPTION WHEN OTHERS THEN NULL;
        END;

        -- Move each table back, handling LOBs.
        -- Process tables with LONG columns last (they cannot be moved).
        FOR rec IN (
            SELECT t.owner, t.table_name,
                   ROUND(NVL(s.bytes, 0) / 1024 / 1024, 1) AS size_mb,
                   CASE WHEN EXISTS (
                       SELECT 1 FROM dba_tab_columns c
                        WHERE c.owner = t.owner AND c.table_name = t.table_name
                          AND c.data_type IN ('LONG', 'LONG RAW')
                   ) THEN 'Y' ELSE 'N' END AS has_long
              FROM dba_tables t
              LEFT JOIN (
                  SELECT owner, segment_name, SUM(bytes) AS bytes
                    FROM dba_segments
                   WHERE tablespace_name = 'EPF_SCRATCH' AND segment_type = 'TABLE'
                   GROUP BY owner, segment_name
              ) s ON s.owner = t.owner AND s.segment_name = t.table_name
             WHERE t.owner IN (c_owner_oppay, c_owner_op)
               AND t.tablespace_name = 'EPF_SCRATCH'
             ORDER BY NVL(s.bytes, 0) DESC
        ) LOOP
            IF rec.has_long = 'Y' THEN
                v_move_errors := v_move_errors + 1;
                v_error_list := SUBSTR(
                    NVL(v_error_list, '') || 'LONG:' || rec.owner || '.'
                    || rec.table_name || '; ', 1, 4000);
                DBMS_OUTPUT.PUT_LINE('  SKIP (LONG column): ' || rec.owner || '.'
                    || rec.table_name || ' — cannot MOVE tables with LONG columns.');
                DBMS_OUTPUT.PUT_LINE('         This table must stay in EPF_SCRATCH or be');
                DBMS_OUTPUT.PUT_LINE('         rebuilt manually (export/import).');
            ELSE
                BEGIN
                    EXECUTE IMMEDIATE build_move_sql(rec.owner, rec.table_name, v_data_ts);
                    v_moved_back := v_moved_back + 1;
                    IF rec.size_mb >= 50 OR MOD(v_moved_back, 25) = 0 THEN
                        DBMS_OUTPUT.PUT_LINE('  Moved [' || v_moved_back || '] '
                            || rec.owner || '.' || rec.table_name
                            || ' (' || rec.size_mb || ' MB)');
                    END IF;
                EXCEPTION WHEN OTHERS THEN
                    v_move_errors := v_move_errors + 1;
                    v_error_list := SUBSTR(
                        NVL(v_error_list, '') || 'MOVE:' || rec.owner || '.'
                        || rec.table_name || '(' || SQLCODE || '); ', 1, 4000);
                    DBMS_OUTPUT.PUT_LINE('  ERROR moving ' || rec.owner || '.'
                        || rec.table_name || ': ' || SUBSTR(SQLERRM, 1, 200));
                END;
            END IF;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Repatriation: moved=' || v_moved_back
            || ', errors=' || v_move_errors);

        -- Also move any LOB segments that might be orphaned in SCRATCH
        -- (LOB segment's table is already in DATA but LOB stayed behind).
        DECLARE
            v_orphan_lobs NUMBER := 0;
        BEGIN
            FOR lob IN (
                SELECT l.owner, l.table_name, l.column_name
                  FROM dba_lobs l
                  JOIN dba_segments s ON s.owner = l.owner
                       AND s.segment_name = l.segment_name
                 WHERE s.tablespace_name = 'EPF_SCRATCH'
                   AND l.owner IN (c_owner_oppay, c_owner_op)
                   AND EXISTS (
                       SELECT 1 FROM dba_tables t
                        WHERE t.owner = l.owner
                          AND t.table_name = l.table_name
                          AND t.tablespace_name = v_data_ts
                   )
            ) LOOP
                BEGIN
                    EXECUTE IMMEDIATE 'ALTER TABLE "' || lob.owner || '"."'
                        || lob.table_name || '" MOVE LOB ("' || lob.column_name
                        || '") STORE AS (TABLESPACE ' || v_data_ts || ')';
                    v_orphan_lobs := v_orphan_lobs + 1;
                EXCEPTION WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('  WARNING: Orphan LOB move failed: '
                        || lob.owner || '.' || lob.table_name || '.' || lob.column_name
                        || ': ' || SUBSTR(SQLERRM, 1, 200));
                END;
            END LOOP;
            IF v_orphan_lobs > 0 THEN
                DBMS_OUTPUT.PUT_LINE('  Orphan LOBs moved back: ' || v_orphan_lobs);
            END IF;
        END;
    ELSE
        DBMS_OUTPUT.PUT_LINE('--- Phase 2: No tables in EPF_SCRATCH — skipping repatriation ---');
    END IF;

    DBMS_OUTPUT.PUT_LINE('');

    -- ========================================================================
    -- PHASE 3: DROP SCRATCH — Remove EPF_SCRATCH if empty
    -- ========================================================================
    IF v_scratch_exists THEN
        DBMS_OUTPUT.PUT_LINE('--- Phase 3: EPF_SCRATCH cleanup ---');

        DECLARE
            v_remaining NUMBER;
        BEGIN
            SELECT COUNT(*) INTO v_remaining FROM dba_segments
             WHERE tablespace_name = 'EPF_SCRATCH';

            IF v_remaining = 0 THEN
                EXECUTE IMMEDIATE 'DROP TABLESPACE EPF_SCRATCH INCLUDING CONTENTS AND DATAFILES';
                DBMS_OUTPUT.PUT_LINE('  EPF_SCRATCH dropped (was empty).');
            ELSE
                DBMS_OUTPUT.PUT_LINE('  WARNING: ' || v_remaining
                    || ' segments still in EPF_SCRATCH. Cannot drop.');
                DBMS_OUTPUT.PUT_LINE('  Remaining segments:');
                FOR rec IN (
                    SELECT owner, segment_name, segment_type,
                           ROUND(bytes / 1024 / 1024, 1) AS size_mb
                      FROM dba_segments
                     WHERE tablespace_name = 'EPF_SCRATCH'
                     ORDER BY bytes DESC
                     FETCH FIRST 20 ROWS ONLY
                ) LOOP
                    DBMS_OUTPUT.PUT_LINE('    ' || rec.owner || '.' || rec.segment_name
                        || ' [' || rec.segment_type || '] ' || rec.size_mb || ' MB');
                END LOOP;
            END IF;
        END;

        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('--- Phase 3: EPF_SCRATCH does not exist — nothing to drop ---');
        DBMS_OUTPUT.PUT_LINE('');
    END IF;

    -- ========================================================================
    -- PHASE 4: RECREATE — Restore missing indexes and constraints from backup
    -- ========================================================================
    IF v_backup_exists AND v_run_id IS NOT NULL
       AND (v_missing_indexes > 0 OR v_missing_pk > 0 OR v_missing_fk > 0)
    THEN
        DBMS_OUTPUT.PUT_LINE('--- Phase 4: Recreating missing indexes/constraints ---');
        DBMS_OUTPUT.PUT_LINE('  Using DDL backup run_id=' || RAWTOHEX(v_run_id));
        DBMS_OUTPUT.PUT_LINE('');

        -- 4a. Recreate non-constraint indexes.
        DBMS_OUTPUT.PUT_LINE('  Pass 1: Non-constraint indexes (' || v_missing_indexes || ' missing)');
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
                    v_recreated_idx := v_recreated_idx + 1;
                    -- Mark as recreated in backup table.
                    BEGIN
                        UPDATE oppayments.epf_ddl_backup
                           SET recreated = 'Y'
                         WHERE backup_id = rec.backup_id;
                        COMMIT;
                    EXCEPTION WHEN OTHERS THEN NULL;
                    END;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    v_recreate_errors := v_recreate_errors + 1;
                    v_error_list := SUBSTR(
                        NVL(v_error_list, '') || 'IDX:' || rec.object_owner || '.'
                        || rec.object_name || '(' || SQLCODE || '); ', 1, 4000);
                    DBMS_OUTPUT.PUT_LINE('    ERROR: ' || rec.object_owner || '.'
                        || rec.object_name || ': ' || SUBSTR(SQLERRM, 1, 200));
            END;
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('    Indexes: recreated=' || v_recreated_idx
            || ', skipped=' || v_skipped || ', errors=' || v_recreate_errors);

        -- 4b. Recreate PK/UK constraints.
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Pass 2: PK/UK constraints (' || v_missing_pk || ' missing)');
        DECLARE
            v_pk_skip NUMBER := 0;
            v_pk_err  NUMBER := 0;
        BEGIN
            FOR rec IN (
                SELECT backup_id, object_owner, object_name, table_name, ddl_text
                  FROM oppayments.epf_ddl_backup
                 WHERE run_id = v_run_id
                   AND object_type IN ('PK', 'UK')
                 ORDER BY seq_num
            ) LOOP
                BEGIN
                    IF constraint_exists(rec.object_owner, rec.object_name) THEN
                        v_pk_skip := v_pk_skip + 1;
                        v_skipped := v_skipped + 1;
                    ELSE
                        EXECUTE IMMEDIATE rec.ddl_text;
                        v_recreated_pk := v_recreated_pk + 1;
                        BEGIN
                            UPDATE oppayments.epf_ddl_backup
                               SET recreated = 'Y'
                             WHERE backup_id = rec.backup_id;
                            COMMIT;
                        EXCEPTION WHEN OTHERS THEN NULL;
                        END;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        v_pk_err := v_pk_err + 1;
                        v_recreate_errors := v_recreate_errors + 1;
                        v_error_list := SUBSTR(
                            NVL(v_error_list, '') || 'PK:' || rec.object_owner || '.'
                            || rec.object_name || '(' || SQLCODE || '); ', 1, 4000);
                        DBMS_OUTPUT.PUT_LINE('    ERROR: ' || rec.object_owner || '.'
                            || rec.object_name || ' on ' || rec.table_name
                            || ': ' || SUBSTR(SQLERRM, 1, 200));
                END;
            END LOOP;
            DBMS_OUTPUT.PUT_LINE('    PK/UK: recreated=' || v_recreated_pk
                || ', skipped=' || v_pk_skip || ', errors=' || v_pk_err);
        END;

        -- 4c. Recreate FK constraints.
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Pass 3: FK constraints (' || v_missing_fk || ' missing)');
        DECLARE
            v_fk_skip NUMBER := 0;
            v_fk_err  NUMBER := 0;
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
                        v_fk_skip := v_fk_skip + 1;
                        v_skipped := v_skipped + 1;
                    ELSE
                        EXECUTE IMMEDIATE rec.ddl_text;
                        v_recreated_fk := v_recreated_fk + 1;
                        BEGIN
                            UPDATE oppayments.epf_ddl_backup
                               SET recreated = 'Y'
                             WHERE backup_id = rec.backup_id;
                            COMMIT;
                        EXCEPTION WHEN OTHERS THEN NULL;
                        END;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        v_fk_err := v_fk_err + 1;
                        v_recreate_errors := v_recreate_errors + 1;
                        v_error_list := SUBSTR(
                            NVL(v_error_list, '') || 'FK:' || rec.object_owner || '.'
                            || rec.object_name || '(' || SQLCODE || '); ', 1, 4000);
                        DBMS_OUTPUT.PUT_LINE('    ERROR: ' || rec.object_owner || '.'
                            || rec.object_name || ' on ' || rec.table_name
                            || ': ' || SUBSTR(SQLERRM, 1, 200));
                END;
            END LOOP;
            DBMS_OUTPUT.PUT_LINE('    FK: recreated=' || v_recreated_fk
                || ', skipped=' || v_fk_skip || ', errors=' || v_fk_err);
        END;

        DBMS_OUTPUT.PUT_LINE('');
    ELSIF (v_missing_indexes > 0 OR v_missing_pk > 0 OR v_missing_fk > 0)
          AND NOT v_backup_exists
    THEN
        DBMS_OUTPUT.PUT_LINE('--- Phase 4: CANNOT RECREATE — No DDL backup available ---');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Indexes and constraints are missing but EPF_DDL_BACKUP');
        DBMS_OUTPUT.PUT_LINE('  is empty or does not exist. You must restore them manually:');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Option 1: Restore from a metadata export:');
        DBMS_OUTPUT.PUT_LINE('    impdp system/<pwd>@<TNS> dumpfile=epf_meta.dmp \\');
        DBMS_OUTPUT.PUT_LINE('      schemas=OPPAYMENTS,OP content=METADATA_ONLY \\');
        DBMS_OUTPUT.PUT_LINE('      include=INDEX,CONSTRAINT table_exists_action=SKIP');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Option 2: Restore from RMAN backup.');
        DBMS_OUTPUT.PUT_LINE('');
    ELSE
        DBMS_OUTPUT.PUT_LINE('--- Phase 4: All indexes/constraints present — skipping ---');
        DBMS_OUTPUT.PUT_LINE('');
    END IF;

    -- ========================================================================
    -- PHASE 5: RESIZE — Shrink datafiles after recovery
    -- ========================================================================
    IF v_moved_back > 0 OR v_recreated_idx > 0 OR v_recreated_pk > 0 THEN
        DBMS_OUTPUT.PUT_LINE('--- Phase 5: Resize datafiles ---');
        resize_datafile(v_data_file, v_data_ts, 100);
        DBMS_OUTPUT.PUT_LINE('');
    END IF;

    -- ========================================================================
    -- PHASE 6: VERIFY — Final state check
    -- ========================================================================
    DBMS_OUTPUT.PUT_LINE('--- Phase 6: Final verification ---');
    DBMS_OUTPUT.PUT_LINE('');

    -- Tablespace check.
    DECLARE
        v_data_hwm  NUMBER;
        v_data_used NUMBER;
        v_data_file_gb NUMBER;
    BEGIN
        SELECT ROUND(NVL(MAX(block_id + blocks), 0) * v_block_size / 1024/1024/1024, 4)
          INTO v_data_hwm FROM dba_extents WHERE tablespace_name = v_data_ts;
        SELECT ROUND(NVL(SUM(bytes), 0) / 1024/1024/1024, 4)
          INTO v_data_used FROM dba_segments WHERE tablespace_name = v_data_ts;
        SELECT ROUND(SUM(bytes) / 1024/1024/1024, 4) INTO v_data_file_gb
          FROM dba_data_files WHERE tablespace_name = v_data_ts;
        DBMS_OUTPUT.PUT_LINE('  ' || v_data_ts || ':');
        DBMS_OUTPUT.PUT_LINE('    HWM:      ' || v_data_hwm || ' GB');
        DBMS_OUTPUT.PUT_LINE('    Used:     ' || v_data_used || ' GB');
        DBMS_OUTPUT.PUT_LINE('    Datafile: ' || v_data_file_gb || ' GB');
    END;

    -- Scratch check.
    DECLARE
        v_scratch_remains NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_scratch_remains FROM dba_tablespaces
         WHERE tablespace_name = 'EPF_SCRATCH';
        IF v_scratch_remains > 0 THEN
            DECLARE
                v_seg NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_seg FROM dba_segments
                 WHERE tablespace_name = 'EPF_SCRATCH';
                DBMS_OUTPUT.PUT_LINE('');
                DBMS_OUTPUT.PUT_LINE('  EPF_SCRATCH: STILL EXISTS (' || v_seg || ' segments)');
                IF v_seg > 0 THEN
                    DBMS_OUTPUT.PUT_LINE('  WARNING: Manual intervention required to clear SCRATCH.');
                END IF;
            END;
        ELSE
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('  EPF_SCRATCH: does not exist (clean)');
        END IF;
    END;

    -- Object count check.
    DECLARE
        v_idx_now NUMBER;
        v_pk_now  NUMBER;
        v_fk_now  NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_idx_now FROM dba_indexes
         WHERE owner IN (c_owner_oppay, c_owner_op)
           AND index_type IN ('NORMAL','UNIQUE','FUNCTION-BASED NORMAL',
                              'BITMAP','FUNCTION-BASED BITMAP');
        SELECT COUNT(*) INTO v_pk_now FROM dba_constraints
         WHERE owner IN (c_owner_oppay, c_owner_op)
           AND constraint_type IN ('P','U');
        SELECT COUNT(*) INTO v_fk_now FROM dba_constraints
         WHERE constraint_type = 'R'
           AND (owner IN (c_owner_oppay, c_owner_op)
                OR r_owner IN (c_owner_oppay, c_owner_op));
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Current DB objects:');
        DBMS_OUTPUT.PUT_LINE('    Indexes:     ' || v_idx_now);
        DBMS_OUTPUT.PUT_LINE('    PK/UK:       ' || v_pk_now);
        DBMS_OUTPUT.PUT_LINE('    FK:          ' || v_fk_now);
    END;

    -- ========================================================================
    -- SUMMARY
    -- ========================================================================
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('============================================================');
    IF v_move_errors = 0 AND v_recreate_errors = 0 THEN
        DBMS_OUTPUT.PUT_LINE('  RECOVERY COMPLETE — ALL CLEAR');
    ELSE
        DBMS_OUTPUT.PUT_LINE('  RECOVERY COMPLETE — WITH ERRORS');
    END IF;
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  Tables moved from SCRATCH: ' || v_moved_back);
    DBMS_OUTPUT.PUT_LINE('  Table move errors:         ' || v_move_errors);
    DBMS_OUTPUT.PUT_LINE('  Indexes recreated:         ' || v_recreated_idx);
    DBMS_OUTPUT.PUT_LINE('  PK/UK recreated:           ' || v_recreated_pk);
    DBMS_OUTPUT.PUT_LINE('  FK recreated:              ' || v_recreated_fk);
    DBMS_OUTPUT.PUT_LINE('  Already existed (skipped):  ' || v_skipped);
    DBMS_OUTPUT.PUT_LINE('  Recreate errors:           ' || v_recreate_errors);
    IF v_error_list IS NOT NULL THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  FAILED OBJECTS:');
        DBMS_OUTPUT.PUT_LINE('    ' || v_error_list);
    END IF;

    IF v_move_errors > 0 THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Some tables could not be moved back. Possible causes:');
        DBMS_OUTPUT.PUT_LINE('    - Table has LONG/LONG RAW column (cannot be MOVEd)');
        DBMS_OUTPUT.PUT_LINE('    - Insufficient space in ' || v_data_ts);
        DBMS_OUTPUT.PUT_LINE('    - Table is locked or in use by another session');
        DBMS_OUTPUT.PUT_LINE('  Fix these manually, then re-run this script.');
    END IF;

    IF v_recreate_errors > 0 THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Some objects could not be recreated. To see their DDL:');
        DBMS_OUTPUT.PUT_LINE('    SELECT object_type, object_owner, object_name, ddl_text');
        DBMS_OUTPUT.PUT_LINE('      FROM oppayments.epf_ddl_backup');
        DBMS_OUTPUT.PUT_LINE('     WHERE run_id = HEXTORAW(''' || RAWTOHEX(v_run_id) || ''')');
        DBMS_OUTPUT.PUT_LINE('       AND recreated = ''N''');
        DBMS_OUTPUT.PUT_LINE('     ORDER BY object_type, seq_num;');
    END IF;

    IF v_move_errors = 0 AND v_recreate_errors = 0 THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  The database schema is fully restored.');
        DBMS_OUTPUT.PUT_LINE('  The application can be brought back online.');
    END IF;

    DBMS_OUTPUT.PUT_LINE('============================================================');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
        DBMS_OUTPUT.PUT_LINE('  UNHANDLED ERROR — recovery aborted!');
        DBMS_OUTPUT.PUT_LINE('  ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('  ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Progress at failure:');
        DBMS_OUTPUT.PUT_LINE('    Tables moved from SCRATCH: ' || v_moved_back);
        DBMS_OUTPUT.PUT_LINE('    Move errors: ' || v_move_errors);
        DBMS_OUTPUT.PUT_LINE('    Indexes recreated: ' || v_recreated_idx);
        DBMS_OUTPUT.PUT_LINE('    PK/UK recreated: ' || v_recreated_pk);
        DBMS_OUTPUT.PUT_LINE('    FK recreated: ' || v_recreated_fk);
        DBMS_OUTPUT.PUT_LINE('    Recreate errors: ' || v_recreate_errors);
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  This script is safe to re-run. Fix the underlying issue');
        DBMS_OUTPUT.PUT_LINE('  (usually insufficient disk space) and try again.');
        DBMS_OUTPUT.PUT_LINE('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!');
        RAISE;
END;
/
