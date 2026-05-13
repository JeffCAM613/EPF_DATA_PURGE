-- ============================================================================
-- EPF Data Purge - Tablespace Reclaim (Iterative Drain + Refill)
-- ============================================================================
-- Deterministic tablespace defragmentation using iterative drain through a
-- scratch tablespace, with per-move datafile resize to keep disk usage flat.
--
-- Previous approaches:
--   - Squeeze loop (sql/legacy/05_reclaim_tablespace_squeeze.sql): non-
--     deterministic, locality bias prevented convergence.
--   - In-place MOVE (sql/legacy/05_reclaim_tablespace_inplace_move.sql):
--     dual-copy coexistence + autoallocate bitmap scattering caused HWM
--     inflation during sweeps.
--
-- This script:
--   1.  Drop all indexes/constraints (capture DDL for rebuild).
--   2.  SHRINK SPACE in-place compaction.
--   3.  Create EPF_SCRATCH tablespace (1 GB + AUTOEXTEND).
--   4.  Iterative DRAIN: move tables one-by-one from DATA to SCRATCH,
--       HWM-anchor first.  build_move_sql() includes LOB STORE AS
--       clauses so each table's LOBs travel with it.  Resize DATA
--       datafile after every ~1 GB drained so disk freed from DATA
--       offsets SCRATCH growth.
--   5.  Resize DATA to minimum. Set AUTOEXTEND ON.
--   6.  Iterative REFILL: move tables one-by-one from SCRATCH to DATA,
--       SCRATCH HWM-anchor first. Resize SCRATCH after every ~1 GB
--       refilled. AUTOEXTEND grows DATA sequentially = tight packing.
--   7.  Drop SCRATCH. Resize DATA. Recreate all indexes/constraints.
--
-- Disk profile:
--   Combined DATA + SCRATCH stays roughly constant throughout.
--   Peak temporary overshoot = largest single segment (~5-10 GB typical).
--   No 2x disk requirement.
--
-- Trade-off:
--   * Indexes are DROPPED for the duration. Application must be quiesced.
--   * Tables with LONG columns cannot be moved (stay as HWM anchor).
--
-- Parameters (positional, for wrapper backward compatibility):
--   &1 = target_pct_free   - safety margin above HWM for resize (default 10)
--   &2 = max_iterations    - IGNORED (legacy). Logged once.
--   &3 = skip_stall_checks - IGNORED (legacy). Logged once.
--   &4 = allow_offline_idx - IGNORED (legacy). Logged once.
--
-- Prerequisites:
--   * Oracle 12.2+
--   * Run as SYS / SYSDBA
--   * Application quiesced (indexes unavailable during reclaim)
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET FEEDBACK OFF
SET VERIFY OFF

DEFINE target_pct_free      = &1
DEFINE max_iterations       = &2
DEFINE skip_stall_checks    = &3
DEFINE allow_offline_idx    = &4

DECLARE
    c_target_pct_free   CONSTANT NUMBER := &target_pct_free;
    c_safety_margin_mb  CONSTANT NUMBER := 50;

    -- ---- Schemas in scope ----
    c_owner_oppay       CONSTANT VARCHAR2(30) := 'OPPAYMENTS';
    c_owner_op          CONSTANT VARCHAR2(30) := 'OP';

    -- ---- Discovered tablespaces / files ----
    v_data_ts           VARCHAR2(128);
    v_index_ts          VARCHAR2(128);            -- equals v_data_ts when shared
    v_separate_index    BOOLEAN := FALSE;
    v_block_size        NUMBER;

    -- Initial sizing snapshot (for the final summary banner; never
    -- overwritten -- this is the bug fix from the old script where the
    -- summary printed stale "13.29 -> 8.94" while the warning printed the
    -- pre-reclaim values from the same vars).
    v_initial_data_hwm_gb   NUMBER;
    v_initial_index_hwm_gb  NUMBER;
    v_initial_data_df_gb    NUMBER;
    v_initial_index_df_gb   NUMBER;
    v_initial_used_gb       NUMBER;

    -- ---- Captured DDL ----
    TYPE t_ddl_arr   IS TABLE OF CLOB           INDEX BY PLS_INTEGER;
    TYPE t_name_arr  IS TABLE OF VARCHAR2(128)  INDEX BY PLS_INTEGER;
    TYPE t_num_arr   IS TABLE OF NUMBER         INDEX BY PLS_INTEGER;

    v_idx_count          PLS_INTEGER := 0;
    v_idx_owner          t_name_arr;
    v_idx_name           t_name_arr;
    v_idx_ddl            t_ddl_arr;

    v_pk_count           PLS_INTEGER := 0;
    v_pk_owner           t_name_arr;
    v_pk_table           t_name_arr;
    v_pk_name            t_name_arr;
    v_pk_ddl             t_ddl_arr;

    v_fk_count           PLS_INTEGER := 0;
    v_fk_owner           t_name_arr;
    v_fk_table           t_name_arr;
    v_fk_name            t_name_arr;
    v_fk_ddl             t_ddl_arr;

    -- ---- Counters ----
    v_dropped_idx        NUMBER := 0;
    v_dropped_pk         NUMBER := 0;
    v_dropped_fk         NUMBER := 0;
    v_recreated_idx      NUMBER := 0;
    v_recreated_pk       NUMBER := 0;
    v_recreated_fk       NUMBER := 0;
    v_shrink_count       NUMBER := 0;
    v_shrink_errors      NUMBER := 0;
    v_shrink_total       NUMBER := 0;
    v_shrink_seen        NUMBER := 0;
    v_recreate_errors    NUMBER := 0;
    v_failed_recreate_list VARCHAR2(4000) := NULL;

    -- ---- Parallel index rebuild ----
    c_parallel_degree       CONSTANT NUMBER := 4;

    -- ---- Cadence ----
    c_log_every_n           CONSTANT NUMBER := 25;
    c_log_max_secs          CONSTANT NUMBER := 30;
    v_last_log_iter         NUMBER := 0;
    v_last_log_ts           TIMESTAMP;

    v_started               TIMESTAMP := SYSTIMESTAMP;
    v_run_id                RAW(16) := NULL;
    v_log_paused            BOOLEAN := FALSE;

    -- ---- Per-phase duration tracking ----
    v_phase_count           PLS_INTEGER := 0;
    v_phase_label           t_name_arr;
    v_phase_secs            t_num_arr;
    v_phase_current         VARCHAR2(64) := NULL;
    v_phase_started         TIMESTAMP;

    -- ---- Misc ----
    v_resize_bytes          NUMBER;

    -- ---- Iterative drain/refill ----
    v_drained               NUMBER := 0;
    v_refilled              NUMBER := 0;
    v_drain_failed          NUMBER := 0;
    v_refill_failed         NUMBER := 0;
    v_skipped_long          NUMBER := 0;
    v_moved_bytes           NUMBER := 0;
    c_resize_threshold      CONSTANT NUMBER := 1024 * 1024 * 1024; -- 1 GB
    v_data_file             VARCHAR2(512);
    v_scratch_file          VARCHAR2(512);

    TYPE t_table_rec IS RECORD (
        owner      VARCHAR2(128),
        table_name VARCHAR2(128),
        max_block  NUMBER,
        seg_mb     NUMBER
    );
    TYPE t_table_list IS TABLE OF t_table_rec;
    v_drain_list            t_table_list;
    v_refill_list           t_table_list;

    -- Additional index tablespaces (beyond v_data_ts and v_index_ts)
    v_other_idx_ts          t_name_arr;
    v_other_idx_ts_count    PLS_INTEGER := 0;
    v_other_idx_init_hwm    t_num_arr;
    v_other_idx_init_df     t_num_arr;

    -- ------------------------------------------------------------------------
    -- Cursor: tables in a tablespace ordered by highest extent block DESC.
    -- Groups TABLE + LOBSEGMENT + LOBINDEX extents by parent table.
    -- Excludes LONG-column tables and purge log tables.
    -- ------------------------------------------------------------------------
    CURSOR c_tables(p_ts VARCHAR2) IS
        SELECT owner, table_name, max_block,
               ROUND(seg_bytes / 1024 / 1024, 1) AS seg_mb
        FROM (
            SELECT t.owner, t.table_name,
                   MAX(t.max_blk) AS max_block,
                   SUM(t.seg_bytes) AS seg_bytes
            FROM (
                -- Table segments
                SELECT e.owner, e.segment_name AS table_name,
                       MAX(e.block_id + e.blocks) AS max_blk,
                       SUM(e.bytes) AS seg_bytes
                  FROM dba_extents e
                 WHERE e.tablespace_name = p_ts
                   AND e.owner IN (c_owner_oppay, c_owner_op)
                   AND e.segment_type = 'TABLE'
                   AND e.segment_name NOT IN ('EPF_PURGE_LOG','EPF_PURGE_SPACE_SNAPSHOT')
                 GROUP BY e.owner, e.segment_name
                UNION ALL
                -- LOB segments -> parent table
                SELECT l.owner, l.table_name,
                       MAX(e.block_id + e.blocks),
                       SUM(e.bytes)
                  FROM dba_lobs l
                  JOIN dba_extents e ON e.owner = l.owner
                       AND e.segment_name = l.segment_name
                 WHERE e.tablespace_name = p_ts
                   AND e.segment_type = 'LOBSEGMENT'
                   AND l.owner IN (c_owner_oppay, c_owner_op)
                   AND l.table_name NOT IN ('EPF_PURGE_LOG','EPF_PURGE_SPACE_SNAPSHOT')
                 GROUP BY l.owner, l.table_name
                UNION ALL
                -- LOB indexes -> parent table
                SELECT l.owner, l.table_name,
                       MAX(e.block_id + e.blocks),
                       SUM(e.bytes)
                  FROM dba_lobs l
                  JOIN dba_extents e ON e.owner = l.owner
                       AND e.segment_name = l.index_name
                 WHERE e.tablespace_name = p_ts
                   AND e.segment_type = 'LOBINDEX'
                   AND l.owner IN (c_owner_oppay, c_owner_op)
                   AND l.table_name NOT IN ('EPF_PURGE_LOG','EPF_PURGE_SPACE_SNAPSHOT')
                 GROUP BY l.owner, l.table_name
            ) t
            WHERE NOT EXISTS (
                SELECT 1 FROM dba_tab_columns c
                 WHERE c.owner = t.owner AND c.table_name = t.table_name
                   AND c.data_type IN ('LONG', 'LONG RAW')
            )
            GROUP BY t.owner, t.table_name
        )
        ORDER BY max_block DESC;

    -- ------------------------------------------------------------------------
    FUNCTION get_hwm_gb(p_ts VARCHAR2) RETURN NUMBER IS
        l_hwm NUMBER;
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT ROUND(MAX(block_id + blocks) * :1 / 1024/1024/1024, 6)
             FROM dba_extents WHERE tablespace_name = :2'
            INTO l_hwm USING v_block_size, p_ts;
        RETURN NVL(l_hwm, 0);
    END;

    FUNCTION get_used_gb(p_ts VARCHAR2) RETURN NUMBER IS
        l_used NUMBER;
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT ROUND(SUM(bytes) / 1024/1024/1024, 4)
             FROM dba_segments WHERE tablespace_name = :1'
            INTO l_used USING p_ts;
        RETURN NVL(l_used, 0);
    END;

    FUNCTION get_file_gb(p_ts VARCHAR2) RETURN NUMBER IS
        l_size NUMBER;
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT ROUND(SUM(bytes) / 1024/1024/1024, 4)
             FROM dba_data_files WHERE tablespace_name = :1'
            INTO l_size USING p_ts;
        RETURN NVL(l_size, 0);
    END;

    PROCEDURE reclaim_log(
        p_operation IN VARCHAR2,
        p_status    IN VARCHAR2,
        p_message   IN VARCHAR2
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        l_elapsed NUMBER;
    BEGIN
        IF v_run_id IS NULL OR v_log_paused THEN
            COMMIT;
            RETURN;
        END IF;
        l_elapsed := ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400);
        INSERT INTO oppayments.epf_purge_log (
            run_id, log_timestamp, module, operation, table_name,
            rows_affected, batch_number, retention_days, status,
            message, error_code, error_message, elapsed_seconds
        ) VALUES (
            v_run_id, SYSTIMESTAMP, 'RECLAIM', p_operation, NULL,
            0, NULL, NULL, p_status,
            SUBSTR(p_message, 1, 4000), NULL, NULL, l_elapsed
        );
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN ROLLBACK;
    END reclaim_log;

    -- ------------------------------------------------------------------------
    -- Per-phase duration helpers.
    PROCEDURE phase_end IS
        l_secs NUMBER;
    BEGIN
        IF v_phase_current IS NULL THEN RETURN; END IF;
        l_secs := ROUND((CAST(SYSTIMESTAMP AS DATE)
                       - CAST(v_phase_started AS DATE)) * 86400, 1);
        v_phase_count := v_phase_count + 1;
        v_phase_label(v_phase_count) := v_phase_current;
        v_phase_secs(v_phase_count)  := l_secs;
        reclaim_log('PHASE_END', 'INFO',
            v_phase_current || ' completed in ' || l_secs || ' s');
        v_phase_current := NULL;
    END phase_end;

    PROCEDURE phase_start(p_label IN VARCHAR2) IS
    BEGIN
        IF v_phase_current IS NOT NULL THEN
            phase_end;
        END IF;
        v_phase_current := p_label;
        v_phase_started := SYSTIMESTAMP;
        reclaim_log('PHASE_START', 'INFO', p_label);
    END phase_start;

    PROCEDURE print_phase_summary IS
        l_total NUMBER := 0;
    BEGIN
        IF v_phase_count = 0 THEN RETURN; END IF;
        DBMS_OUTPUT.PUT_LINE('  Per-phase duration:');
        FOR i IN 1 .. v_phase_count LOOP
            DBMS_OUTPUT.PUT_LINE('    '
                || RPAD(v_phase_label(i), 34) || ' '
                || LPAD(TO_CHAR(v_phase_secs(i), 'FM999990.0'), 9) || ' s');
            l_total := l_total + NVL(v_phase_secs(i), 0);
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('    '
            || RPAD('TOTAL (sum of phases)', 34) || ' '
            || LPAD(TO_CHAR(l_total, 'FM999990.0'), 9) || ' s');
    END print_phase_summary;

    -- Datafile resize for every datafile in a tablespace. Target = HWM +
    -- a small fixed safety margin (c_safety_margin_mb). ORA-03297 (data
    -- above target) means another tenant is sitting higher than our HWM
    -- target; we walk the target up in 1 GB steps until either the resize
    -- succeeds or we hit the original size.
    PROCEDURE resize_tablespace_to_hwm(p_ts VARCHAR2, p_label VARCHAR2) IS
        v_hwm_gb     NUMBER;
        v_target_gb  NUMBER;
        v_target_b   NUMBER;
        v_freed_gb   NUMBER := 0;
    BEGIN
        v_hwm_gb := get_hwm_gb(p_ts);
        v_target_gb := v_hwm_gb + c_safety_margin_mb / 1024;

        FOR f IN (
            SELECT file_name, ROUND(bytes/1024/1024/1024, 4) AS size_gb, bytes
              FROM dba_data_files
             WHERE tablespace_name = p_ts
             ORDER BY file_id
        ) LOOP
            v_target_b := CEIL(v_target_gb * 1024) * 1024 * 1024;
            IF v_target_b >= f.bytes THEN
                reclaim_log('RESIZE_DF', 'INFO',
                    p_label || ' datafile ' || f.file_name
                    || ' already <= target (' || ROUND(v_target_gb, 2)
                    || ' GB). No resize.');
            ELSE
                BEGIN
                    EXECUTE IMMEDIATE
                        'ALTER DATABASE DATAFILE ''' || f.file_name
                        || ''' RESIZE ' || v_target_b;
                    v_freed_gb := v_freed_gb + (f.size_gb - v_target_b/1024/1024/1024);
                    reclaim_log('RESIZE_DF', 'SUCCESS',
                        p_label || ' datafile resized: '
                        || f.file_name || ' '
                        || f.size_gb || ' GB -> '
                        || ROUND(v_target_b/1024/1024/1024, 2) || ' GB'
                        || ' (HWM=' || ROUND(v_hwm_gb, 4) || ' GB)');
                EXCEPTION
                    WHEN OTHERS THEN
                        IF SQLCODE = -3297 THEN
                            DECLARE
                                v_try_b NUMBER;
                                v_ok    BOOLEAN := FALSE;
                            BEGIN
                                FOR add_gb IN 1..10 LOOP
                                    v_try_b := CEIL((v_target_gb + add_gb) * 1024) * 1024 * 1024;
                                    IF v_try_b >= f.bytes THEN EXIT; END IF;
                                    BEGIN
                                        EXECUTE IMMEDIATE
                                            'ALTER DATABASE DATAFILE ''' || f.file_name
                                            || ''' RESIZE ' || v_try_b;
                                        v_freed_gb := v_freed_gb + (f.size_gb - v_try_b/1024/1024/1024);
                                        reclaim_log('RESIZE_DF', 'SUCCESS',
                                            p_label || ' datafile resized (with +'
                                            || add_gb || ' GB headroom for foreign tenant): '
                                            || f.file_name || ' '
                                            || f.size_gb || ' GB -> '
                                            || ROUND(v_try_b/1024/1024/1024, 2) || ' GB');
                                        v_ok := TRUE;
                                        EXIT;
                                    EXCEPTION
                                        WHEN OTHERS THEN
                                            IF SQLCODE != -3297 THEN RAISE; END IF;
                                    END;
                                END LOOP;
                                IF NOT v_ok THEN
                                    reclaim_log('RESIZE_DF', 'WARNING',
                                        p_label || ' datafile ' || f.file_name
                                        || ' could not be resized (ORA-03297): a foreign'
                                        || ' tenant in this tablespace sits above our HWM.'
                                        || ' Inspect dba_extents WHERE tablespace_name='''
                                        || p_ts || ''' ORDER BY block_id+blocks DESC.');
                                END IF;
                            END;
                        ELSE
                            reclaim_log('RESIZE_DF', 'WARNING',
                                p_label || ' datafile resize failed for '
                                || f.file_name || ': ' || SUBSTR(SQLERRM, 1, 500));
                        END IF;
                END;
            END IF;
        END LOOP;

        IF v_freed_gb > 0 THEN
            reclaim_log('RESIZE_DF', 'SUCCESS',
                p_label || ' tablespace ' || p_ts || ': freed '
                || ROUND(v_freed_gb, 2) || ' GB on disk.');
        END IF;
    END resize_tablespace_to_hwm;

    -- ------------------------------------------------------------------------
    -- Build a MOVE SQL statement that includes LOB STORE AS clauses for
    -- every LOB column in the table so LOBs travel with the table.
    -- ------------------------------------------------------------------------
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

    -- ------------------------------------------------------------------------
    -- Resize a single datafile to HWM + margin. Step-back on ORA-03297.
    -- Logs to epf_purge_log via reclaim_log().
    -- ------------------------------------------------------------------------
    PROCEDURE do_resize_datafile(
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

        IF v_new_bytes >= v_cur_bytes THEN
            RETURN;
        END IF;

        BEGIN
            EXECUTE IMMEDIATE 'ALTER DATABASE DATAFILE '''
                || REPLACE(p_datafile, '''', '''''')
                || ''' RESIZE ' || v_new_bytes;
            reclaim_log('RESIZE_DF', 'SUCCESS',
                p_ts || ' datafile resized: '
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
                        reclaim_log('RESIZE_DF', 'SUCCESS',
                            p_ts || ' datafile resized (with +' || add_gb
                            || 'GB headroom): '
                            || ROUND(v_cur_bytes/1024/1024/1024, 2) || ' GB -> '
                            || ROUND(v_new_bytes/1024/1024/1024, 2) || ' GB');
                        RETURN;
                    EXCEPTION WHEN OTHERS THEN
                        IF SQLCODE != -3297 THEN RAISE; END IF;
                    END;
                END LOOP;
                reclaim_log('RESIZE_DF', 'WARNING',
                    p_ts || ' datafile resize skipped (ORA-03297 after 10 retries).');
            ELSE
                reclaim_log('RESIZE_DF', 'WARNING',
                    p_ts || ' datafile resize error: ' || SUBSTR(SQLERRM, 1, 200));
            END IF;
        END;
    END do_resize_datafile;

BEGIN
    -- ========================================================================
    -- Step 0: Acknowledge ignored legacy params
    -- ========================================================================
    IF UPPER('&max_iterations') NOT IN ('', '0') AND '&max_iterations' != '2000' THEN
        DBMS_OUTPUT.PUT_LINE('NOTE: --max-iterations is ignored by the iterative drain reclaim.');
    END IF;
    IF UPPER('&skip_stall_checks') = 'Y' THEN
        DBMS_OUTPUT.PUT_LINE('NOTE: --no-stall-check is ignored by the iterative drain reclaim.');
    END IF;
    IF UPPER('&allow_offline_idx') = 'Y' THEN
        DBMS_OUTPUT.PUT_LINE('NOTE: --allow-offline-index-rebuild is implied (every index is'
            || ' dropped and recreated by design).');
    END IF;

    -- ========================================================================
    -- Step 1: Discover tablespaces
    -- ========================================================================
    EXECUTE IMMEDIATE 'SELECT value FROM v$parameter WHERE name = ''db_block_size'''
        INTO v_block_size;

    -- Data tablespace = OPPAYMENTS user's default tablespace.
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT default_tablespace FROM dba_users WHERE username = :1'
            INTO v_data_ts USING c_owner_oppay;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: ' || c_owner_oppay || ' user not found.');
            RETURN;
    END;

    -- Index tablespace = the most common tablespace housing OPPAYMENTS+OP
    -- non-LOB indexes.
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT tablespace_name FROM (
                 SELECT tablespace_name, COUNT(*) AS cnt
                   FROM dba_indexes
                  WHERE owner IN (:1, :2)
                    AND tablespace_name IS NOT NULL
                    AND index_type IN (''NORMAL'', ''UNIQUE'', ''FUNCTION-BASED NORMAL'',
                                       ''BITMAP'', ''FUNCTION-BASED BITMAP'')
                  GROUP BY tablespace_name
                  ORDER BY cnt DESC
             ) WHERE ROWNUM = 1'
            INTO v_index_ts USING c_owner_oppay, c_owner_op;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_index_ts := v_data_ts;
    END;
    v_separate_index := (v_index_ts != v_data_ts);

    -- Look up primary data file for iterative resize operations.
    SELECT file_name INTO v_data_file FROM dba_data_files
     WHERE tablespace_name = v_data_ts AND ROWNUM = 1;

    v_initial_data_hwm_gb  := get_hwm_gb(v_data_ts);
    v_initial_used_gb      := get_used_gb(v_data_ts);
    v_initial_index_hwm_gb := CASE WHEN v_separate_index THEN get_hwm_gb(v_index_ts) ELSE NULL END;

    BEGIN
        EXECUTE IMMEDIATE
            'SELECT ROUND(SUM(bytes)/1024/1024/1024, 4) FROM dba_data_files
              WHERE tablespace_name = :1'
            INTO v_initial_data_df_gb USING v_data_ts;
    EXCEPTION WHEN OTHERS THEN v_initial_data_df_gb := NULL;
    END;
    IF v_separate_index THEN
        BEGIN
            EXECUTE IMMEDIATE
                'SELECT ROUND(SUM(bytes)/1024/1024/1024, 4) FROM dba_data_files
                  WHERE tablespace_name = :1'
                INTO v_initial_index_df_gb USING v_index_ts;
        EXCEPTION WHEN OTHERS THEN v_initial_index_df_gb := NULL;
        END;
    END IF;

    -- Attach to the most recent purge run so logs show under the same run_id.
    -- If that run already has a RECLAIM_END (i.e. reclaim already ran for it),
    -- generate a fresh run_id so the new reclaim gets its own log entries.
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT run_id FROM (
                 SELECT run_id FROM oppayments.epf_purge_log
                  WHERE operation = ''RUN_END''
                  ORDER BY log_timestamp DESC
             ) WHERE ROWNUM = 1'
            INTO v_run_id;
        DECLARE
            l_reclaim_count NUMBER;
        BEGIN
            EXECUTE IMMEDIATE
                'SELECT COUNT(*) FROM oppayments.epf_purge_log
                  WHERE run_id = :1 AND operation = ''RECLAIM_END'''
                INTO l_reclaim_count USING v_run_id;
            IF l_reclaim_count > 0 THEN
                v_run_id := SYS_GUID();
            END IF;
        END;
    EXCEPTION WHEN OTHERS THEN v_run_id := SYS_GUID();
    END;

    reclaim_log('RECLAIM_START', 'INFO',
        'Iterative drain reclaim started.'
        || ' Data TS=' || v_data_ts
        || ' (datafile=' || NVL(TO_CHAR(v_initial_data_df_gb), '?') || 'GB,'
        || ' HWM=' || ROUND(v_initial_data_hwm_gb, 4) || 'GB,'
        || ' used=' || v_initial_used_gb || 'GB).'
        || ' Index TS=' || v_index_ts
        || CASE WHEN v_separate_index
                THEN ' (SEPARATE; datafile=' || NVL(TO_CHAR(v_initial_index_df_gb), '?')
                     || 'GB, HWM=' || ROUND(v_initial_index_hwm_gb, 4) || 'GB).'
                ELSE ' (SHARED with data TS).' END);

    -- Capture BEFORE space snapshot for the wrapper's post-reclaim comparison.
    BEGIN
        EXECUTE IMMEDIATE
            'BEGIN oppayments.epf_purge_pkg.capture_space_snapshot(:1, ''BEFORE''); END;'
            USING v_run_id;
        reclaim_log('INFO', 'INFO', 'BEFORE space snapshot captured for run_id ' || RAWTOHEX(v_run_id));
    EXCEPTION WHEN OTHERS THEN
        reclaim_log('INFO', 'INFO', 'BEFORE snapshot skipped: ' || SQLERRM);
    END;

    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  EPF TABLESPACE RECLAIM (iterative drain + refill)');
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  Data tablespace:  ' || v_data_ts
        || '  HWM=' || ROUND(v_initial_data_hwm_gb, 4) || ' GB'
        || '  used=' || v_initial_used_gb || ' GB');
    IF v_separate_index THEN
        DBMS_OUTPUT.PUT_LINE('  Index tablespace: ' || v_index_ts
            || '  HWM=' || ROUND(v_initial_index_hwm_gb, 4) || ' GB  (SEPARATE)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('  Index tablespace: ' || v_index_ts || '  (shared with data TS)');
    END IF;
    DBMS_OUTPUT.PUT_LINE('============================================================');

    -- ========================================================================
    -- Step 2: Relocate purge log/snapshot tables OUT of the data TS
    -- ========================================================================
    phase_start('Relocate log tables');
    DECLARE
        v_target_ts VARCHAR2(128) := NULL;
        v_count     NUMBER := 0;
        v_moved     NUMBER := 0;
    BEGIN
        SELECT COUNT(*) INTO v_count FROM dba_tablespaces
         WHERE tablespace_name = 'USERS' AND status = 'ONLINE'
           AND contents = 'PERMANENT';
        IF v_count > 0 THEN
            v_target_ts := 'USERS';
        ELSE
            SELECT COUNT(*) INTO v_count FROM dba_tablespaces
             WHERE tablespace_name = 'SYSAUX' AND status = 'ONLINE';
            IF v_count > 0 THEN v_target_ts := 'SYSAUX'; END IF;
        END IF;

        IF v_target_ts IS NULL THEN
            reclaim_log('LOG_RELOCATE', 'WARNING',
                'No USERS or SYSAUX tablespace available; log/snapshot tables'
                || ' stay in ' || v_data_ts || '. Final HWM may bottom out at'
                || ' their position.');
        ELSE
            v_log_paused := TRUE;

            FOR tbl IN (
                SELECT table_name FROM dba_tables
                 WHERE owner = c_owner_oppay
                   AND table_name IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT')
                   AND tablespace_name = v_data_ts
            ) LOOP
                BEGIN
                    EXECUTE IMMEDIATE
                        'ALTER TABLE ' || c_owner_oppay || '.' || tbl.table_name
                        || ' MOVE ONLINE TABLESPACE ' || v_target_ts;
                    v_moved := v_moved + 1;
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
            END LOOP;

            FOR idx IN (
                SELECT owner, index_name FROM dba_indexes
                 WHERE table_owner = c_owner_oppay
                   AND table_name IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT')
                   AND tablespace_name = v_data_ts
            ) LOOP
                BEGIN
                    EXECUTE IMMEDIATE
                        'ALTER INDEX ' || idx.owner || '.' || idx.index_name
                        || ' REBUILD ONLINE TABLESPACE ' || v_target_ts;
                    v_moved := v_moved + 1;
                EXCEPTION WHEN OTHERS THEN NULL;
                END;
            END LOOP;

            v_log_paused := FALSE;
            reclaim_log('LOG_RELOCATE', 'SUCCESS',
                'Phase 1: relocated ' || v_moved || ' log/snapshot segments to '
                || v_target_ts || '.');
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            v_log_paused := FALSE;
            reclaim_log('LOG_RELOCATE', 'WARNING',
                'Log-table relocation failed: ' || SUBSTR(SQLERRM, 1, 500));
    END;

    -- ========================================================================
    -- Step 3: Capture DDL (indexes, then PK/UK, then FK)
    -- ========================================================================
    phase_start('Capture DDL');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 2: capturing DDL ===');

    DBMS_METADATA.SET_TRANSFORM_PARAM(
        DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(
        DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', FALSE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(
        DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', TRUE);
    DBMS_METADATA.SET_TRANSFORM_PARAM(
        DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);

    -- 3a) Non-constraint indexes (all tablespaces -- DDL preserves original TS).
    FOR rec IN (
        SELECT i.owner, i.index_name
          FROM dba_indexes i
         WHERE i.owner IN (c_owner_oppay, c_owner_op)
           AND i.tablespace_name IS NOT NULL
           AND i.index_type IN ('NORMAL', 'UNIQUE',
                                'FUNCTION-BASED NORMAL',
                                'BITMAP', 'FUNCTION-BASED BITMAP')
           AND i.table_name NOT IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT')
           AND NOT EXISTS (
               SELECT 1 FROM dba_constraints c
                WHERE c.owner = i.owner
                  AND c.index_name = i.index_name
                  AND c.constraint_type IN ('P', 'U'))
         ORDER BY i.owner, i.index_name
    ) LOOP
        BEGIN
            v_idx_count := v_idx_count + 1;
            v_idx_owner(v_idx_count) := rec.owner;
            v_idx_name(v_idx_count)  := rec.index_name;
            v_idx_ddl(v_idx_count)   :=
                RTRIM(DBMS_METADATA.GET_DDL('INDEX', rec.index_name, rec.owner),
                      CHR(10) || CHR(13) || CHR(9) || ' ');
        EXCEPTION
            WHEN OTHERS THEN
                v_idx_count := v_idx_count - 1;
                reclaim_log('CAPTURE_DDL', 'WARNING',
                    'Skipping index ' || rec.owner || '.' || rec.index_name
                    || ' (DDL capture failed: ' || SUBSTR(SQLERRM, 1, 200) || ')');
        END;
    END LOOP;

    -- 3b) PK / UK constraints.
    FOR rec IN (
        SELECT c.owner, c.table_name, c.constraint_name, c.constraint_type
          FROM dba_constraints c
         WHERE c.owner IN (c_owner_oppay, c_owner_op)
           AND c.constraint_type IN ('P', 'U')
           AND c.table_name NOT IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT')
           AND NOT EXISTS (
               SELECT 1 FROM dba_tables t
                WHERE t.owner = c.owner
                  AND t.table_name = c.table_name
                  AND t.iot_type IS NOT NULL)
         ORDER BY c.owner, c.table_name, c.constraint_type DESC
    ) LOOP
        BEGIN
            v_pk_count := v_pk_count + 1;
            v_pk_owner(v_pk_count) := rec.owner;
            v_pk_table(v_pk_count) := rec.table_name;
            v_pk_name(v_pk_count)  := rec.constraint_name;
            v_pk_ddl(v_pk_count)   :=
                RTRIM(DBMS_METADATA.GET_DDL('CONSTRAINT', rec.constraint_name, rec.owner),
                      CHR(10) || CHR(13) || CHR(9) || ' ');
        EXCEPTION
            WHEN OTHERS THEN
                v_pk_count := v_pk_count - 1;
                reclaim_log('CAPTURE_DDL', 'WARNING',
                    'Skipping PK/UK ' || rec.owner || '.' || rec.constraint_name
                    || ' (DDL capture failed: ' || SUBSTR(SQLERRM, 1, 200) || ')');
        END;
    END LOOP;

    -- 3c) FK constraints.
    FOR rec IN (
        SELECT c.owner, c.table_name, c.constraint_name
          FROM dba_constraints c
         WHERE c.constraint_type = 'R'
           AND (c.owner IN (c_owner_oppay, c_owner_op)
                OR c.r_owner IN (c_owner_oppay, c_owner_op))
           AND c.table_name NOT IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT')
         ORDER BY c.owner, c.table_name, c.constraint_name
    ) LOOP
        BEGIN
            v_fk_count := v_fk_count + 1;
            v_fk_owner(v_fk_count) := rec.owner;
            v_fk_table(v_fk_count) := rec.table_name;
            v_fk_name(v_fk_count)  := rec.constraint_name;
            v_fk_ddl(v_fk_count)   :=
                RTRIM(DBMS_METADATA.GET_DDL('REF_CONSTRAINT', rec.constraint_name, rec.owner),
                      CHR(10) || CHR(13) || CHR(9) || ' ');
        EXCEPTION
            WHEN OTHERS THEN
                v_fk_count := v_fk_count - 1;
                reclaim_log('CAPTURE_DDL', 'WARNING',
                    'Skipping FK ' || rec.owner || '.' || rec.constraint_name
                    || ' (DDL capture failed: ' || SUBSTR(SQLERRM, 1, 200) || ')');
        END;
    END LOOP;

    reclaim_log('CAPTURE_DDL', 'SUCCESS',
        'DDL captured: indexes=' || v_idx_count
        || ', PK/UK=' || v_pk_count
        || ', FK=' || v_fk_count || '.');
    DBMS_OUTPUT.PUT_LINE('  Captured: indexes=' || v_idx_count
        || ', PK/UK=' || v_pk_count || ', FK=' || v_fk_count);

    -- Discover additional index tablespaces beyond v_data_ts / v_index_ts.
    FOR ts_rec IN (
        SELECT DISTINCT i.tablespace_name
          FROM dba_indexes i
         WHERE i.owner IN (c_owner_oppay, c_owner_op)
           AND i.tablespace_name IS NOT NULL
           AND i.tablespace_name NOT IN (v_data_ts, v_index_ts)
           AND i.index_type IN ('NORMAL', 'UNIQUE', 'FUNCTION-BASED NORMAL',
                                'BITMAP', 'FUNCTION-BASED BITMAP')
           AND i.table_name NOT IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT')
         ORDER BY i.tablespace_name
    ) LOOP
        v_other_idx_ts_count := v_other_idx_ts_count + 1;
        v_other_idx_ts(v_other_idx_ts_count) := ts_rec.tablespace_name;
        v_other_idx_init_hwm(v_other_idx_ts_count) := get_hwm_gb(ts_rec.tablespace_name);
        BEGIN
            EXECUTE IMMEDIATE
                'SELECT ROUND(SUM(bytes)/1024/1024/1024, 4) FROM dba_data_files
                  WHERE tablespace_name = :1'
                INTO v_other_idx_init_df(v_other_idx_ts_count)
                USING ts_rec.tablespace_name;
        EXCEPTION WHEN OTHERS THEN
            v_other_idx_init_df(v_other_idx_ts_count) := NULL;
        END;
    END LOOP;
    IF v_other_idx_ts_count > 0 THEN
        DBMS_OUTPUT.PUT_LINE('  Additional index TS: ' || v_other_idx_ts_count);
        FOR i IN 1 .. v_other_idx_ts_count LOOP
            DBMS_OUTPUT.PUT_LINE('    ' || v_other_idx_ts(i)
                || ' (HWM=' || ROUND(v_other_idx_init_hwm(i), 4) || ' GB)');
            reclaim_log('CAPTURE_DDL', 'INFO',
                'Additional index tablespace: ' || v_other_idx_ts(i)
                || ' HWM=' || ROUND(v_other_idx_init_hwm(i), 4) || 'GB'
                || ' datafile=' || NVL(TO_CHAR(v_other_idx_init_df(i)), '?') || 'GB');
        END LOOP;
    END IF;

    -- ========================================================================
    -- Step 3d: Persist captured DDL to EPF_DDL_BACKUP table
    -- ========================================================================
    phase_start('Persist DDL backup');
    DECLARE
        PROCEDURE persist_ddl(
            p_run_id      IN RAW,
            p_object_type IN VARCHAR2,
            p_owner       IN VARCHAR2,
            p_name        IN VARCHAR2,
            p_table_name  IN VARCHAR2,
            p_ddl         IN CLOB,
            p_seq         IN NUMBER
        ) IS
            PRAGMA AUTONOMOUS_TRANSACTION;
        BEGIN
            INSERT INTO oppayments.epf_ddl_backup (
                run_id, object_type, object_owner, object_name,
                table_name, ddl_text, recreated, seq_num
            ) VALUES (
                p_run_id, p_object_type, p_owner, p_name,
                p_table_name, p_ddl, 'N', p_seq
            );
            COMMIT;
        EXCEPTION
            WHEN OTHERS THEN ROLLBACK;
        END persist_ddl;

        PROCEDURE clear_old_backup(p_run_id IN RAW) IS
            PRAGMA AUTONOMOUS_TRANSACTION;
        BEGIN
            DELETE FROM oppayments.epf_ddl_backup WHERE run_id = p_run_id;
            COMMIT;
        EXCEPTION WHEN OTHERS THEN ROLLBACK;
        END clear_old_backup;
    BEGIN
        clear_old_backup(v_run_id);

        FOR i IN 1 .. v_idx_count LOOP
            persist_ddl(v_run_id, 'INDEX', v_idx_owner(i), v_idx_name(i),
                        NULL, v_idx_ddl(i), i);
        END LOOP;

        FOR i IN 1 .. v_pk_count LOOP
            persist_ddl(v_run_id, 'PK', v_pk_owner(i), v_pk_name(i),
                        v_pk_table(i), v_pk_ddl(i), i);
        END LOOP;

        FOR i IN 1 .. v_fk_count LOOP
            persist_ddl(v_run_id, 'FK', v_fk_owner(i), v_fk_name(i),
                        v_fk_table(i), v_fk_ddl(i), i);
        END LOOP;

        reclaim_log('DDL_BACKUP', 'SUCCESS',
            'DDL persisted to EPF_DDL_BACKUP: indexes=' || v_idx_count
            || ', PK/UK=' || v_pk_count || ', FK=' || v_fk_count
            || '. Recovery script: sql/14_recover_indexes.sql');
        DBMS_OUTPUT.PUT_LINE('  DDL backed up to EPF_DDL_BACKUP ('
            || (v_idx_count + v_pk_count + v_fk_count) || ' objects).');
    END;

    -- ========================================================================
    -- Guard: abort if no indexes/constraints exist to drop
    -- ========================================================================
    IF v_idx_count = 0 AND v_pk_count = 0 AND v_fk_count = 0 THEN
        reclaim_log('GUARD', 'ERROR',
            'ABORTING: No indexes or constraints found to drop. '
            || 'This usually means a previous reclaim dropped them but failed '
            || 'before recreating. Run sql/14_recover_indexes.sql first, then re-run reclaim.');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('  ERROR: NO INDEXES OR CONSTRAINTS FOUND');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('  The capture phase found 0 indexes, 0 PK/UK, 0 FK.');
        DBMS_OUTPUT.PUT_LINE('  This means a previous reclaim dropped them but never');
        DBMS_OUTPUT.PUT_LINE('  recovered them.');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  FIX: Run sql/14_recover_indexes.sql first to restore');
        DBMS_OUTPUT.PUT_LINE('       all indexes and constraints from EPF_DDL_BACKUP,');
        DBMS_OUTPUT.PUT_LINE('       then re-run the reclaim.');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        phase_end;

        reclaim_log('RECLAIM_END', 'ERROR',
            'Reclaim aborted: no indexes/constraints to drop (see GUARD error).');

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('  RECLAIM ABORTED');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        print_phase_summary;
        DBMS_OUTPUT.PUT_LINE('============================================================');
        RETURN;
    END IF;

    -- ========================================================================
    -- Step 4: Drop FK constraints
    -- ========================================================================
    phase_start('Drop FK constraints');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 3: dropping ' || v_fk_count || ' FK + '
        || v_pk_count || ' PK/UK + ' || v_idx_count || ' index(es) ===');

    FOR i IN 1 .. v_fk_count LOOP
        BEGIN
            EXECUTE IMMEDIATE
                'ALTER TABLE "' || v_fk_owner(i) || '"."' || v_fk_table(i)
                || '" DROP CONSTRAINT "' || v_fk_name(i) || '"';
            v_dropped_fk := v_dropped_fk + 1;
        EXCEPTION
            WHEN OTHERS THEN
                reclaim_log('DROP_FK', 'WARNING',
                    'Drop FK failed: ' || v_fk_owner(i) || '.' || v_fk_name(i)
                    || ' on ' || v_fk_table(i)
                    || ': ' || SUBSTR(SQLERRM, 1, 300));
        END;
    END LOOP;
    reclaim_log('DROP_FK', 'SUCCESS',
        'Dropped ' || v_dropped_fk || '/' || v_fk_count || ' FK constraints.');

    -- ========================================================================
    -- Step 5: Drop PK/UK constraints (also drops backing index)
    -- ========================================================================
    phase_start('Drop PK/UK constraints');
    FOR i IN 1 .. v_pk_count LOOP
        BEGIN
            EXECUTE IMMEDIATE
                'ALTER TABLE "' || v_pk_owner(i) || '"."' || v_pk_table(i)
                || '" DROP CONSTRAINT "' || v_pk_name(i) || '" DROP INDEX';
            v_dropped_pk := v_dropped_pk + 1;
        EXCEPTION
            WHEN OTHERS THEN
                reclaim_log('DROP_PK', 'WARNING',
                    'Drop PK/UK failed: ' || v_pk_owner(i) || '.' || v_pk_name(i)
                    || ' on ' || v_pk_table(i)
                    || ': ' || SUBSTR(SQLERRM, 1, 300));
        END;
    END LOOP;
    reclaim_log('DROP_PK', 'SUCCESS',
        'Dropped ' || v_dropped_pk || '/' || v_pk_count || ' PK/UK constraints.');

    -- ========================================================================
    -- Step 6: Drop remaining (non-constraint) indexes
    -- ========================================================================
    phase_start('Drop secondary indexes');
    FOR i IN 1 .. v_idx_count LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP INDEX "' || v_idx_owner(i) || '"."' || v_idx_name(i) || '"';
            v_dropped_idx := v_dropped_idx + 1;
        EXCEPTION
            WHEN OTHERS THEN
                IF SQLCODE != -1418 THEN
                    reclaim_log('DROP_IDX', 'WARNING',
                        'Drop index failed: ' || v_idx_owner(i) || '.' || v_idx_name(i)
                        || ': ' || SUBSTR(SQLERRM, 1, 300));
                END IF;
        END;
    END LOOP;
    reclaim_log('DROP_IDX', 'SUCCESS',
        'Dropped ' || v_dropped_idx || '/' || v_idx_count || ' non-constraint indexes.');
    DBMS_OUTPUT.PUT_LINE('  Drop summary: FK=' || v_dropped_fk
        || '/' || v_fk_count || ', PK/UK=' || v_dropped_pk
        || '/' || v_pk_count || ', Idx=' || v_dropped_idx
        || '/' || v_idx_count);

    reclaim_log('DROP_IDX', 'INFO',
        'Post-drop ' || v_data_ts || ' HWM=' || ROUND(get_hwm_gb(v_data_ts), 4)
        || 'GB, used=' || get_used_gb(v_data_ts) || 'GB.'
        || CASE WHEN v_separate_index THEN
                ' Index TS HWM=' || ROUND(get_hwm_gb(v_index_ts), 4)
                || 'GB, used=' || get_used_gb(v_index_ts) || 'GB.'
                ELSE '' END);

    -- ========================================================================
    -- Step 7a: SHRINK SPACE — SKIPPED
    -- ========================================================================
    -- SHRINK SPACE is redundant when the full drain path (ALTER TABLE MOVE)
    -- follows. MOVE rewrites every block from scratch, so any in-place
    -- compaction done by SHRINK is thrown away. Worse, SHRINK SPACE on
    -- LOB-heavy tables (AUDIT_TRAIL, SPEC_TRT_LOG, etc.) can hang for
    -- hours or days — blocking the entire reclaim.
    phase_start('SHRINK SPACE tables (skipped)');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 4a: SHRINK SPACE — skipped (drain path handles compaction) ===');
    reclaim_log('SHRINK_DONE', 'SUCCESS',
        'Phase 4a skipped: SHRINK SPACE is redundant when drain (ALTER TABLE MOVE) follows.');

    -- ========================================================================
    -- Step 7b: Create EPF_SCRATCH tablespace
    -- ========================================================================
    phase_start('Create EPF_SCRATCH');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 4b: create EPF_SCRATCH tablespace ===');
    DECLARE
        v_scratch_dir VARCHAR2(512);
        v_last_sep    NUMBER;
        v_exists      NUMBER;
    BEGIN
        -- Derive directory from DATA datafile
        v_last_sep := GREATEST(
            NVL(INSTR(v_data_file, '/', -1), 0),
            NVL(INSTR(v_data_file, '\', -1), 0)
        );
        v_scratch_dir := SUBSTR(v_data_file, 1, v_last_sep);
        v_scratch_file := v_scratch_dir || 'epf_scratch01.dbf';

        -- Drop if leftover from a previous failed run
        SELECT COUNT(*) INTO v_exists FROM dba_tablespaces
         WHERE tablespace_name = 'EPF_SCRATCH';
        IF v_exists > 0 THEN
            EXECUTE IMMEDIATE 'DROP TABLESPACE EPF_SCRATCH INCLUDING CONTENTS AND DATAFILES';
            reclaim_log('SCRATCH_TS', 'INFO', 'Dropped leftover EPF_SCRATCH from previous run.');
            DBMS_OUTPUT.PUT_LINE('  Dropped leftover EPF_SCRATCH.');
        END IF;

        EXECUTE IMMEDIATE
            'CREATE TABLESPACE EPF_SCRATCH DATAFILE '''
            || v_scratch_file || ''' SIZE 1G'
            || ' AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED';

        -- Grant quota so OPPAYMENTS and OP can write to it
        BEGIN EXECUTE IMMEDIATE 'ALTER USER OPPAYMENTS QUOTA UNLIMITED ON EPF_SCRATCH'; EXCEPTION WHEN OTHERS THEN NULL; END;
        BEGIN EXECUTE IMMEDIATE 'ALTER USER OP QUOTA UNLIMITED ON EPF_SCRATCH'; EXCEPTION WHEN OTHERS THEN NULL; END;

        -- Look up the scratch file path (may differ from our constructed name)
        SELECT file_name INTO v_scratch_file FROM dba_data_files
         WHERE tablespace_name = 'EPF_SCRATCH' AND ROWNUM = 1;

        reclaim_log('SCRATCH_TS', 'SUCCESS',
            'Created EPF_SCRATCH (1 GB + AUTOEXTEND). File: ' || v_scratch_file);
        DBMS_OUTPUT.PUT_LINE('  EPF_SCRATCH created: ' || v_scratch_file);
    END;

    -- ========================================================================
    -- Step 7c: Iterative drain (DATA -> SCRATCH), HWM-anchor first
    -- ========================================================================
    -- Move tables one-by-one from DATA to SCRATCH, starting with the table
    -- whose extents sit highest (HWM anchor). After each ~1 GB batch of
    -- moves, resize the DATA datafile down. Combined disk stays constant.
    phase_start('Iterative drain');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 4c: iterative drain DATA -> SCRATCH ===');

    OPEN c_tables(v_data_ts);
    FETCH c_tables BULK COLLECT INTO v_drain_list;
    CLOSE c_tables;

    -- Count LONG tables (excluded from cursor) for display
    SELECT COUNT(DISTINCT t.owner || '.' || t.table_name) INTO v_skipped_long
      FROM dba_tab_columns t
      JOIN dba_segments s ON s.owner = t.owner AND s.segment_name = t.table_name
     WHERE s.tablespace_name = v_data_ts
       AND s.segment_type = 'TABLE'
       AND t.data_type IN ('LONG', 'LONG RAW')
       AND t.owner IN (c_owner_oppay, c_owner_op);

    DBMS_OUTPUT.PUT_LINE('  Tables to drain: ' || v_drain_list.COUNT);
    IF v_skipped_long > 0 THEN
        DBMS_OUTPUT.PUT_LINE('  Skipped (LONG):  ' || v_skipped_long
            || ' (anchor HWM floor)');
    END IF;

    reclaim_log('DRAIN_START', 'INFO',
        'Drain starting: ' || v_drain_list.COUNT || ' tables to move,'
        || ' ' || v_skipped_long || ' LONG-column tables skipped.'
        || ' DATA HWM=' || ROUND(get_hwm_gb(v_data_ts), 4) || ' GB,'
        || ' DATA file=' || get_file_gb(v_data_ts) || ' GB.');

    v_moved_bytes := 0;
    v_last_log_ts := SYSTIMESTAMP - INTERVAL '1' HOUR;
    v_last_log_iter := 0;

    FOR i IN 1..v_drain_list.COUNT LOOP
        BEGIN
            EXECUTE IMMEDIATE build_move_sql(
                v_drain_list(i).owner,
                v_drain_list(i).table_name,
                'EPF_SCRATCH'
            );
            v_drained := v_drained + 1;
            v_moved_bytes := v_moved_bytes + v_drain_list(i).seg_mb * 1024 * 1024;

            -- Cadence logging
            IF v_drain_list(i).seg_mb >= 50
               OR v_drained - v_last_log_iter >= c_log_every_n
               OR (CAST(SYSTIMESTAMP AS DATE) - CAST(v_last_log_ts AS DATE)) * 86400
                    >= c_log_max_secs
            THEN
                reclaim_log('DRAIN_PROGRESS', 'INFO',
                    'DRAIN [' || v_drained || '/' || v_drain_list.COUNT || '] '
                    || v_drain_list(i).owner || '.' || v_drain_list(i).table_name
                    || ' (' || v_drain_list(i).seg_mb || ' MB).'
                    || ' DATA=' || get_file_gb(v_data_ts) || 'GB,'
                    || ' SCRATCH=' || get_file_gb('EPF_SCRATCH') || 'GB.');
                v_last_log_iter := v_drained;
                v_last_log_ts := SYSTIMESTAMP;
                DBMS_OUTPUT.PUT_LINE('  DRAIN [' || v_drained || '] '
                    || v_drain_list(i).owner || '.' || v_drain_list(i).table_name
                    || ' (' || v_drain_list(i).seg_mb || ' MB)');
            END IF;

            -- Resize DATA after threshold reached or last table
            IF v_moved_bytes >= c_resize_threshold OR i = v_drain_list.COUNT THEN
                do_resize_datafile(v_data_file, v_data_ts, 50);
                v_moved_bytes := 0;
                IF v_drain_list(i).seg_mb >= 50 OR MOD(v_drained, 50) = 0
                   OR i = v_drain_list.COUNT THEN
                    DBMS_OUTPUT.PUT_LINE('    -> DATA ' || get_file_gb(v_data_ts)
                        || ' GB | SCRATCH ' || get_file_gb('EPF_SCRATCH') || ' GB'
                        || ' | combined ' || ROUND(get_file_gb(v_data_ts)
                            + get_file_gb('EPF_SCRATCH'), 2) || ' GB');
                END IF;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- ORA-01652: unable to extend temp segment in tablespace
            -- ORA-01659: unable to allocate MINEXTENTS
            -- Both mean SCRATCH disk is full. Stop draining immediately --
            -- continuing would just pile up failures for every remaining table.
            IF SQLCODE IN (-1652, -1659) THEN
                v_drain_failed := v_drain_failed + 1;
                reclaim_log('DRAIN_PROGRESS', 'WARNING',
                    'DRAIN FAILED (disk full): ' || v_drain_list(i).owner || '.'
                    || v_drain_list(i).table_name
                    || ' (' || v_drain_list(i).seg_mb || ' MB): '
                    || SUBSTR(SQLERRM, 1, 200));
                DBMS_OUTPUT.PUT_LINE('  DRAIN FAILED (disk full): '
                    || v_drain_list(i).owner || '.' || v_drain_list(i).table_name
                    || ': ' || SUBSTR(SQLERRM, 1, 120));
                -- Try one last resize to free DATA space before giving up
                do_resize_datafile(v_data_file, v_data_ts, 50);
                reclaim_log('DRAIN_PROGRESS', 'WARNING',
                    'Disk full -- stopping drain early at ' || v_drained
                    || '/' || v_drain_list.COUNT || ' tables.'
                    || ' DATA=' || get_file_gb(v_data_ts) || 'GB,'
                    || ' SCRATCH=' || get_file_gb('EPF_SCRATCH') || 'GB.'
                    || ' Remaining ' || (v_drain_list.COUNT - i) || ' tables'
                    || ' stay in DATA (will be refilled in-place).');
                DBMS_OUTPUT.PUT_LINE('  ** Disk full -- stopping drain. '
                    || (v_drain_list.COUNT - i) || ' tables remain in DATA.');
                EXIT;
            END IF;
            v_drain_failed := v_drain_failed + 1;
            reclaim_log('DRAIN_PROGRESS', 'WARNING',
                'DRAIN FAILED: ' || v_drain_list(i).owner || '.'
                || v_drain_list(i).table_name
                || ' (' || v_drain_list(i).seg_mb || ' MB): '
                || SUBSTR(SQLERRM, 1, 200));
            DBMS_OUTPUT.PUT_LINE('  DRAIN FAILED: ' || v_drain_list(i).owner || '.'
                || v_drain_list(i).table_name || ': ' || SUBSTR(SQLERRM, 1, 120));
        END;
    END LOOP;

    reclaim_log('DRAIN_DONE', 'SUCCESS',
        'Drain complete: drained=' || v_drained || '/' || v_drain_list.COUNT
        || ', failed=' || v_drain_failed
        || '. DATA HWM=' || ROUND(get_hwm_gb(v_data_ts), 4) || 'GB,'
        || ' DATA file=' || get_file_gb(v_data_ts) || 'GB,'
        || ' SCRATCH=' || get_file_gb('EPF_SCRATCH') || 'GB.');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('  Drain complete: ' || v_drained || ' / ' || v_drain_list.COUNT
        || ' (failed: ' || v_drain_failed || ')');
    DBMS_OUTPUT.PUT_LINE('  DATA HWM:  ' || ROUND(get_hwm_gb(v_data_ts), 4) || ' GB');
    DBMS_OUTPUT.PUT_LINE('  DATA file: ' || get_file_gb(v_data_ts) || ' GB');
    DBMS_OUTPUT.PUT_LINE('  SCRATCH:   ' || get_file_gb('EPF_SCRATCH') || ' GB');

    -- ========================================================================
    -- Step 7d: Intermediate - shrink DATA to minimum
    -- ========================================================================
    -- If purge log/snapshot tables are still in DATA (relocation failed),
    -- truncate them so they don't anchor the HWM. If relocation succeeded
    -- (tables now in USERS/SYSAUX), skip truncation to preserve log history
    -- visible to the live monitor.
    phase_start('Intermediate resize');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 4d: intermediate - shrink DATA to minimum ===');

    FOR lt IN (
        SELECT table_name FROM dba_tables
         WHERE owner = c_owner_oppay
           AND table_name IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT')
           AND tablespace_name = v_data_ts
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE ' || c_owner_oppay || '.'
                || lt.table_name || ' DROP ALL STORAGE';
            DBMS_OUTPUT.PUT_LINE('  Truncated ' || lt.table_name
                || ' (still in ' || v_data_ts || ')');
        EXCEPTION WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('  ' || lt.table_name || ': '
                || SUBSTR(SQLERRM, 1, 120));
        END;
    END LOOP;

    do_resize_datafile(v_data_file, v_data_ts, 200);

    EXECUTE IMMEDIATE 'ALTER DATABASE DATAFILE '''
        || REPLACE(v_data_file, '''', '''''')
        || ''' AUTOEXTEND ON NEXT 1G MAXSIZE UNLIMITED';

    reclaim_log('INTERMEDIATE', 'SUCCESS',
        'DATA shrunk to minimum and AUTOEXTEND ON.'
        || ' DATA HWM=' || ROUND(get_hwm_gb(v_data_ts), 4) || 'GB,'
        || ' DATA file=' || get_file_gb(v_data_ts) || 'GB,'
        || ' SCRATCH=' || get_file_gb('EPF_SCRATCH') || 'GB.');
    DBMS_OUTPUT.PUT_LINE('  DATA HWM:  ' || ROUND(get_hwm_gb(v_data_ts), 4) || ' GB');
    DBMS_OUTPUT.PUT_LINE('  DATA file: ' || get_file_gb(v_data_ts) || ' GB  (AUTOEXTEND ON)');
    DBMS_OUTPUT.PUT_LINE('  SCRATCH:   ' || get_file_gb('EPF_SCRATCH') || ' GB');

    -- ========================================================================
    -- Step 7e: Iterative refill (SCRATCH -> DATA), SCRATCH HWM-anchor first
    -- ========================================================================
    -- Move tables back from SCRATCH to DATA. DATA is near-empty with
    -- AUTOEXTEND, so segments pack sequentially (tight). Removing from
    -- top of SCRATCH lets us resize SCRATCH down after each batch.
    phase_start('Iterative refill');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 4e: iterative refill SCRATCH -> DATA ===');

    OPEN c_tables('EPF_SCRATCH');
    FETCH c_tables BULK COLLECT INTO v_refill_list;
    CLOSE c_tables;

    DBMS_OUTPUT.PUT_LINE('  Tables to refill: ' || v_refill_list.COUNT);

    reclaim_log('REFILL_START', 'INFO',
        'Refill starting: ' || v_refill_list.COUNT || ' tables to move back.'
        || ' DATA file=' || get_file_gb(v_data_ts) || 'GB,'
        || ' SCRATCH=' || get_file_gb('EPF_SCRATCH') || 'GB.');

    v_moved_bytes := 0;
    v_last_log_ts := SYSTIMESTAMP - INTERVAL '1' HOUR;
    v_last_log_iter := 0;

    FOR i IN 1..v_refill_list.COUNT LOOP
        BEGIN
            EXECUTE IMMEDIATE build_move_sql(
                v_refill_list(i).owner,
                v_refill_list(i).table_name,
                v_data_ts
            );
            v_refilled := v_refilled + 1;
            v_moved_bytes := v_moved_bytes + v_refill_list(i).seg_mb * 1024 * 1024;

            -- Cadence logging
            IF v_refill_list(i).seg_mb >= 50
               OR v_refilled - v_last_log_iter >= c_log_every_n
               OR (CAST(SYSTIMESTAMP AS DATE) - CAST(v_last_log_ts AS DATE)) * 86400
                    >= c_log_max_secs
            THEN
                reclaim_log('REFILL_PROGRESS', 'INFO',
                    'REFILL [' || v_refilled || '/' || v_refill_list.COUNT || '] '
                    || v_refill_list(i).owner || '.' || v_refill_list(i).table_name
                    || ' (' || v_refill_list(i).seg_mb || ' MB).'
                    || ' DATA HWM=' || ROUND(get_hwm_gb(v_data_ts), 4) || 'GB,'
                    || ' DATA=' || get_file_gb(v_data_ts) || 'GB,'
                    || ' SCRATCH=' || get_file_gb('EPF_SCRATCH') || 'GB.');
                v_last_log_iter := v_refilled;
                v_last_log_ts := SYSTIMESTAMP;
                DBMS_OUTPUT.PUT_LINE('  REFILL [' || v_refilled || '] '
                    || v_refill_list(i).owner || '.' || v_refill_list(i).table_name
                    || ' (' || v_refill_list(i).seg_mb || ' MB)'
                    || ' -> DATA HWM ' || ROUND(get_hwm_gb(v_data_ts), 4) || ' GB');
            END IF;

            -- Resize SCRATCH after threshold reached or last table
            IF v_moved_bytes >= c_resize_threshold OR i = v_refill_list.COUNT THEN
                do_resize_datafile(v_scratch_file, 'EPF_SCRATCH', 50);
                v_moved_bytes := 0;
                IF v_refill_list(i).seg_mb >= 50 OR MOD(v_refilled, 50) = 0
                   OR i = v_refill_list.COUNT THEN
                    DBMS_OUTPUT.PUT_LINE('    -> DATA ' || get_file_gb(v_data_ts)
                        || ' GB | SCRATCH ' || get_file_gb('EPF_SCRATCH') || ' GB'
                        || ' | combined ' || ROUND(get_file_gb(v_data_ts)
                            + get_file_gb('EPF_SCRATCH'), 2) || ' GB');
                END IF;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            -- ORA-01652/01659: DATA disk is full -- can't grow to accept more tables.
            -- Stop refill; remaining tables stay in SCRATCH (safety check prevents drop).
            IF SQLCODE IN (-1652, -1659) THEN
                v_refill_failed := v_refill_failed + 1;
                reclaim_log('REFILL_PROGRESS', 'WARNING',
                    'REFILL FAILED (disk full): '
                    || v_refill_list(i).owner || '.' || v_refill_list(i).table_name
                    || ' (' || v_refill_list(i).seg_mb || ' MB): '
                    || SUBSTR(SQLERRM, 1, 200));
                DBMS_OUTPUT.PUT_LINE('  REFILL FAILED (disk full): '
                    || v_refill_list(i).owner || '.' || v_refill_list(i).table_name
                    || ': ' || SUBSTR(SQLERRM, 1, 120));
                do_resize_datafile(v_scratch_file, 'EPF_SCRATCH', 50);
                reclaim_log('REFILL_PROGRESS', 'WARNING',
                    'Disk full -- stopping refill early at ' || v_refilled
                    || '/' || v_refill_list.COUNT || ' tables.'
                    || ' ' || (v_refill_list.COUNT - i) || ' tables remain in SCRATCH.'
                    || ' SCRATCH will NOT be dropped.');
                DBMS_OUTPUT.PUT_LINE('  ** Disk full -- stopping refill. '
                    || (v_refill_list.COUNT - i) || ' tables remain in SCRATCH.');
                EXIT;
            END IF;
            v_refill_failed := v_refill_failed + 1;
            reclaim_log('REFILL_PROGRESS', 'WARNING',
                'REFILL FAILED (stuck in scratch!): '
                || v_refill_list(i).owner || '.' || v_refill_list(i).table_name
                || ' (' || v_refill_list(i).seg_mb || ' MB): '
                || SUBSTR(SQLERRM, 1, 200));
            DBMS_OUTPUT.PUT_LINE('  REFILL FAILED: ' || v_refill_list(i).owner || '.'
                || v_refill_list(i).table_name || ': ' || SUBSTR(SQLERRM, 1, 120));
        END;
    END LOOP;

    reclaim_log('REFILL_DONE', 'SUCCESS',
        'Refill complete: refilled=' || v_refilled || '/' || v_refill_list.COUNT
        || ', failed=' || v_refill_failed
        || '. DATA HWM=' || ROUND(get_hwm_gb(v_data_ts), 4) || 'GB,'
        || ' DATA file=' || get_file_gb(v_data_ts) || 'GB,'
        || ' SCRATCH=' || get_file_gb('EPF_SCRATCH') || 'GB.');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('  Refill complete: ' || v_refilled || ' / ' || v_refill_list.COUNT
        || ' (failed: ' || v_refill_failed || ')');
    DBMS_OUTPUT.PUT_LINE('  DATA HWM:  ' || ROUND(get_hwm_gb(v_data_ts), 4) || ' GB');
    DBMS_OUTPUT.PUT_LINE('  DATA file: ' || get_file_gb(v_data_ts) || ' GB');
    DBMS_OUTPUT.PUT_LINE('  SCRATCH:   ' || get_file_gb('EPF_SCRATCH') || ' GB');

    -- ========================================================================
    -- Step 7f: Drop EPF_SCRATCH tablespace
    -- ========================================================================
    phase_start('Drop EPF_SCRATCH');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 4f: drop EPF_SCRATCH ===');
    DECLARE
        v_remaining NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_remaining FROM dba_segments
         WHERE tablespace_name = 'EPF_SCRATCH';
        IF v_remaining > 0 THEN
            reclaim_log('SCRATCH_TS', 'WARNING',
                v_remaining || ' segments still in EPF_SCRATCH (refill failures). '
                || 'NOT dropping. Inspect and resolve manually.');
            DBMS_OUTPUT.PUT_LINE('  WARNING: ' || v_remaining
                || ' segments still in EPF_SCRATCH. NOT dropping.');
        ELSE
            EXECUTE IMMEDIATE 'DROP TABLESPACE EPF_SCRATCH INCLUDING CONTENTS AND DATAFILES';
            reclaim_log('SCRATCH_TS', 'SUCCESS', 'EPF_SCRATCH dropped.');
            DBMS_OUTPUT.PUT_LINE('  EPF_SCRATCH dropped.');
        END IF;
    END;

    -- ========================================================================
    -- Step 7g: Diagnostic -- log the top-10 highest-positioned segments.
    -- ========================================================================
    DECLARE
        v_top_msg VARCHAR2(4000);
        v_block_size_mb NUMBER;
    BEGIN
        SELECT block_size / 1024 / 1024 INTO v_block_size_mb
          FROM dba_tablespaces WHERE tablespace_name = v_data_ts;
        v_top_msg := 'Top 10 segments by position (HWM anchors): ';
        FOR rec IN (
            SELECT * FROM (
                SELECT owner, segment_name, segment_type,
                       MAX(block_id + blocks - 1) AS top_block,
                       SUM(blocks) AS total_blocks
                  FROM dba_extents
                 WHERE tablespace_name = v_data_ts
                 GROUP BY owner, segment_name, segment_type
                 ORDER BY MAX(block_id + blocks - 1) DESC
            ) WHERE ROWNUM <= 10
        ) LOOP
            v_top_msg := v_top_msg || CHR(10) || '  '
                || RPAD(rec.owner || '.' || rec.segment_name, 60)
                || ' [' || RPAD(rec.segment_type, 11) || ']'
                || ' top_block=' || rec.top_block
                || ' (' || ROUND(rec.top_block * v_block_size_mb / 1024, 3) || ' GB)'
                || ' size=' || ROUND(rec.total_blocks * v_block_size_mb, 1) || ' MB';
        END LOOP;
        reclaim_log('DRAIN_DONE', 'INFO', v_top_msg);
    END;

    -- ========================================================================
    -- Step 8: Resize data tablespace datafile(s)
    -- When indexes share the same tablespace, skip this intermediate resize
    -- because index recreation (step 10-12) will immediately autoextend the
    -- file back up. We only do the final resize (step 13) after rebuild.
    -- When indexes have a SEPARATE tablespace, resizing DATA here is useful
    -- because index rebuild won't grow the DATA file back.
    -- ========================================================================
    IF v_separate_index THEN
        phase_start('Resize data tablespace');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('=== Phase 5: resize data tablespace ===');
        resize_tablespace_to_hwm(v_data_ts, 'DATA');
    ELSE
        phase_start('Resize data tablespace');
        reclaim_log('RESIZE_DF', 'INFO',
            'Skipping intermediate resize: indexes in same TS will autoextend during rebuild. Final resize after rebuild.');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('=== Phase 5: SKIPPED (indexes share DATA TS, final resize after rebuild) ===');
    END IF;

    -- ========================================================================
    -- Step 9: Resize index tablespace if separate
    -- ========================================================================
    IF v_separate_index THEN
        phase_start('Resize index tablespace');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('=== Phase 6: resize separate index tablespace ===');
        resize_tablespace_to_hwm(v_index_ts, 'INDEX');
    ELSE
        reclaim_log('RESIZE_DF', 'INFO',
            'Index TS shared with data TS -- no separate resize needed.');
    END IF;

    -- ========================================================================
    -- Step 10: Recreate non-constraint indexes
    -- ========================================================================
    phase_start('Recreate secondary indexes');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 7: recreating ' || v_idx_count || ' indexes + '
        || v_pk_count || ' PK/UK + ' || v_fk_count || ' FK ===');

    FOR i IN 1 .. v_idx_count LOOP
        BEGIN
            EXECUTE IMMEDIATE v_idx_ddl(i) || ' PARALLEL ' || c_parallel_degree;
            EXECUTE IMMEDIATE 'ALTER INDEX ' || v_idx_owner(i) || '.' || v_idx_name(i) || ' NOPARALLEL';
            v_recreated_idx := v_recreated_idx + 1;
        EXCEPTION
            WHEN OTHERS THEN
                v_recreate_errors := v_recreate_errors + 1;
                v_failed_recreate_list := SUBSTR(
                    NVL(v_failed_recreate_list, '') || 'IDX:' || v_idx_owner(i) || '.'
                    || v_idx_name(i) || '; ', 1, 4000);
                reclaim_log('RECREATE_IDX', 'ERROR',
                    'Recreate index FAILED: ' || v_idx_owner(i) || '.' || v_idx_name(i)
                    || ': ' || SUBSTR(SQLERRM, 1, 500));
        END;

        IF MOD(i, c_log_every_n) = 0 THEN
            reclaim_log('RECREATE_IDX', 'INFO',
                'Recreated ' || v_recreated_idx || '/' || v_idx_count
                || ' indexes (errors=' || v_recreate_errors || ').');
        END IF;
    END LOOP;
    reclaim_log('RECREATE_IDX', 'SUCCESS',
        'Recreated ' || v_recreated_idx || '/' || v_idx_count || ' indexes.');

    -- ========================================================================
    -- Step 11: Recreate PK / UK constraints
    -- ========================================================================
    phase_start('Recreate PK/UK constraints');
    -- Enable parallel index build for constraint backing indexes
    EXECUTE IMMEDIATE 'ALTER SESSION FORCE PARALLEL DDL PARALLEL ' || c_parallel_degree;
    FOR i IN 1 .. v_pk_count LOOP
        BEGIN
            EXECUTE IMMEDIATE v_pk_ddl(i);
            v_recreated_pk := v_recreated_pk + 1;
        EXCEPTION
            WHEN OTHERS THEN
                v_recreate_errors := v_recreate_errors + 1;
                v_failed_recreate_list := SUBSTR(
                    NVL(v_failed_recreate_list, '') || 'PK:' || v_pk_owner(i) || '.'
                    || v_pk_name(i) || '; ', 1, 4000);
                reclaim_log('RECREATE_PK', 'ERROR',
                    'Recreate PK/UK FAILED: ' || v_pk_owner(i) || '.' || v_pk_name(i)
                    || ' on ' || v_pk_table(i)
                    || ': ' || SUBSTR(SQLERRM, 1, 500));
        END;
    END LOOP;
    EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DDL';
    reclaim_log('RECREATE_PK', 'SUCCESS',
        'Recreated ' || v_recreated_pk || '/' || v_pk_count || ' PK/UK constraints.');

    -- ========================================================================
    -- Step 12: Recreate FK constraints
    -- ========================================================================
    phase_start('Recreate FK constraints');
    FOR i IN 1 .. v_fk_count LOOP
        BEGIN
            EXECUTE IMMEDIATE v_fk_ddl(i);
            v_recreated_fk := v_recreated_fk + 1;
        EXCEPTION
            WHEN OTHERS THEN
                v_recreate_errors := v_recreate_errors + 1;
                v_failed_recreate_list := SUBSTR(
                    NVL(v_failed_recreate_list, '') || 'FK:' || v_fk_owner(i) || '.'
                    || v_fk_name(i) || '; ', 1, 4000);
                reclaim_log('RECREATE_FK', 'ERROR',
                    'Recreate FK FAILED: ' || v_fk_owner(i) || '.' || v_fk_name(i)
                    || ' on ' || v_fk_table(i)
                    || ': ' || SUBSTR(SQLERRM, 1, 500));
        END;
    END LOOP;
    reclaim_log('RECREATE_FK', 'SUCCESS',
        'Recreated ' || v_recreated_fk || '/' || v_fk_count || ' FK constraints.');

    DBMS_OUTPUT.PUT_LINE('  Recreate summary: Idx=' || v_recreated_idx
        || '/' || v_idx_count || ', PK/UK=' || v_recreated_pk
        || '/' || v_pk_count || ', FK=' || v_recreated_fk
        || '/' || v_fk_count || ', errors=' || v_recreate_errors);

    -- ========================================================================
    -- Step 13: Final resize after recreate
    -- ========================================================================
    phase_start('Final resize');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 8: final resize after index recreate ===');
    resize_tablespace_to_hwm(v_data_ts, 'DATA (final)');
    IF v_separate_index THEN
        resize_tablespace_to_hwm(v_index_ts, 'INDEX (final)');
    END IF;
    FOR i IN 1 .. v_other_idx_ts_count LOOP
        resize_tablespace_to_hwm(v_other_idx_ts(i), v_other_idx_ts(i) || ' (final)');
    END LOOP;

    -- ========================================================================
    -- Final summary banner
    -- ========================================================================
    phase_end;
    DECLARE
        v_final_data_hwm   NUMBER := get_hwm_gb(v_data_ts);
        v_final_used       NUMBER := get_used_gb(v_data_ts);
        v_final_data_df    NUMBER;
        v_final_index_hwm  NUMBER := CASE WHEN v_separate_index THEN get_hwm_gb(v_index_ts) ELSE NULL END;
        v_final_index_df   NUMBER;
        v_temp_hwm         NUMBER;
        v_temp_df          NUMBER;
    BEGIN
        BEGIN
            EXECUTE IMMEDIATE
                'SELECT ROUND(SUM(bytes)/1024/1024/1024, 4) FROM dba_data_files
                  WHERE tablespace_name = :1'
                INTO v_final_data_df USING v_data_ts;
        EXCEPTION WHEN OTHERS THEN v_final_data_df := NULL;
        END;
        IF v_separate_index THEN
            BEGIN
                EXECUTE IMMEDIATE
                    'SELECT ROUND(SUM(bytes)/1024/1024/1024, 4) FROM dba_data_files
                      WHERE tablespace_name = :1'
                    INTO v_final_index_df USING v_index_ts;
            EXCEPTION WHEN OTHERS THEN v_final_index_df := NULL;
            END;
        END IF;

        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('  RECLAIM COMPLETE');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('  Data TS (' || v_data_ts || '):');
        DBMS_OUTPUT.PUT_LINE('    HWM:       ' || ROUND(v_initial_data_hwm_gb, 4)
            || ' GB -> ' || ROUND(v_final_data_hwm, 4) || ' GB');
        DBMS_OUTPUT.PUT_LINE('    Datafile:  ' || NVL(TO_CHAR(v_initial_data_df_gb), '?')
            || ' GB -> ' || NVL(TO_CHAR(v_final_data_df), '?') || ' GB');
        DBMS_OUTPUT.PUT_LINE('    Used:      ' || v_initial_used_gb
            || ' GB -> ' || v_final_used || ' GB');
        IF v_separate_index THEN
            DBMS_OUTPUT.PUT_LINE('  Index TS (' || v_index_ts || ', SEPARATE):');
            DBMS_OUTPUT.PUT_LINE('    HWM:       ' || ROUND(v_initial_index_hwm_gb, 4)
                || ' GB -> ' || ROUND(v_final_index_hwm, 4) || ' GB');
            DBMS_OUTPUT.PUT_LINE('    Datafile:  ' || NVL(TO_CHAR(v_initial_index_df_gb), '?')
                || ' GB -> ' || NVL(TO_CHAR(v_final_index_df), '?') || ' GB');
        END IF;
        FOR i IN 1 .. v_other_idx_ts_count LOOP
            v_temp_hwm := get_hwm_gb(v_other_idx_ts(i));
            BEGIN
                EXECUTE IMMEDIATE
                    'SELECT ROUND(SUM(bytes)/1024/1024/1024, 4) FROM dba_data_files
                      WHERE tablespace_name = :1'
                    INTO v_temp_df USING v_other_idx_ts(i);
            EXCEPTION WHEN OTHERS THEN v_temp_df := NULL;
            END;
            DBMS_OUTPUT.PUT_LINE('  Index TS (' || v_other_idx_ts(i) || '):');
            DBMS_OUTPUT.PUT_LINE('    HWM:       ' || ROUND(v_other_idx_init_hwm(i), 4)
                || ' GB -> ' || ROUND(v_temp_hwm, 4) || ' GB');
            DBMS_OUTPUT.PUT_LINE('    Datafile:  ' || NVL(TO_CHAR(v_other_idx_init_df(i)), '?')
                || ' GB -> ' || NVL(TO_CHAR(v_temp_df), '?') || ' GB');
        END LOOP;
        DBMS_OUTPUT.PUT_LINE('  Drop/recreate:');
        DBMS_OUTPUT.PUT_LINE('    FK:    ' || v_dropped_fk || ' dropped, '
            || v_recreated_fk || ' recreated');
        DBMS_OUTPUT.PUT_LINE('    PK/UK: ' || v_dropped_pk || ' dropped, '
            || v_recreated_pk || ' recreated');
        DBMS_OUTPUT.PUT_LINE('    Idx:   ' || v_dropped_idx || ' dropped, '
            || v_recreated_idx || ' recreated');
        DBMS_OUTPUT.PUT_LINE('  Iterative drain:');
        DBMS_OUTPUT.PUT_LINE('    Drained:   ' || v_drained || ' / ' || v_drain_list.COUNT
            || ' (failed: ' || v_drain_failed || ')');
        DBMS_OUTPUT.PUT_LINE('    Refilled:  ' || v_refilled || ' / ' || v_refill_list.COUNT
            || ' (failed: ' || v_refill_failed || ')');
        DBMS_OUTPUT.PUT_LINE('    LONG skip: ' || v_skipped_long);
        DBMS_OUTPUT.PUT_LINE('  SHRINK SPACE: skipped (drain path compacts)');
        DBMS_OUTPUT.PUT_LINE('  Recreate errors: ' || v_recreate_errors);
        IF v_recreate_errors > 0 THEN
            DBMS_OUTPUT.PUT_LINE('    FAILED: ' || v_failed_recreate_list);
        END IF;
        DBMS_OUTPUT.PUT_LINE('  Duration:  '
            || ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400)
            || ' s');
        print_phase_summary;
        DBMS_OUTPUT.PUT_LINE('============================================================');

        reclaim_log('RECLAIM_END',
            CASE WHEN v_recreate_errors = 0 AND v_refill_failed = 0 THEN 'SUCCESS'
                 ELSE 'WARNING' END,
            'Reclaim complete.'
            || ' Data: HWM ' || ROUND(v_initial_data_hwm_gb, 4) || '->'
                              || ROUND(v_final_data_hwm, 4) || 'GB,'
            || ' datafile ' || NVL(TO_CHAR(v_initial_data_df_gb), '?') || '->'
                             || NVL(TO_CHAR(v_final_data_df), '?') || 'GB.'
            || CASE WHEN v_separate_index THEN
                ' Index: HWM ' || ROUND(v_initial_index_hwm_gb, 4) || '->'
                                || ROUND(v_final_index_hwm, 4) || 'GB,'
                || ' datafile ' || NVL(TO_CHAR(v_initial_index_df_gb), '?') || '->'
                                || NVL(TO_CHAR(v_final_index_df), '?') || 'GB.'
                ELSE '' END
            || CASE WHEN v_other_idx_ts_count > 0 THEN
                ' Extra idx TS=' || v_other_idx_ts_count || '.'
                ELSE '' END
            || ' Drain=' || v_drained || '/' || v_drain_list.COUNT
            || ', refill=' || v_refilled || '/' || v_refill_list.COUNT
            || ', drain_fail=' || v_drain_failed
            || ', refill_fail=' || v_refill_failed
            || '. Recreated: idx=' || v_recreated_idx || '/' || v_idx_count
            || ', pk/uk=' || v_recreated_pk || '/' || v_pk_count
            || ', fk=' || v_recreated_fk || '/' || v_fk_count
            || ', errors=' || v_recreate_errors
            || CASE WHEN v_recreate_errors > 0
                    THEN ' [FAILED: ' || v_failed_recreate_list || ']'
                    ELSE '' END);
    END;

    -- ========================================================================
    -- Step 14: Shrink UNDO and TEMP (kept from old script, unchanged logic)
    -- ========================================================================
    phase_start('Shrink UNDO/TEMP');
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 9: shrink UNDO and TEMP tablespaces ===');

    DECLARE
        v_undo_ts        VARCHAR2(128);
        v_undo_file      VARCHAR2(513);
        v_undo_dir       VARCHAR2(513);
        v_undo_size_gb   NUMBER;
        v_new_undo_ts    VARCHAR2(128);
        v_new_undo_file  VARCHAR2(513);
        v_resize_ok      BOOLEAN := FALSE;
    BEGIN
        BEGIN
            EXECUTE IMMEDIATE
                'SELECT value FROM v$parameter WHERE name = ''undo_tablespace'''
                INTO v_undo_ts;
            EXECUTE IMMEDIATE
                'SELECT file_name, ROUND(bytes/1024/1024/1024, 2)
                   FROM dba_data_files WHERE tablespace_name = :1
                   FETCH FIRST 1 ROW ONLY'
                INTO v_undo_file, v_undo_size_gb USING v_undo_ts;
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('  Could not identify UNDO tablespace: ' || SQLERRM);
                GOTO skip_undo;
        END;

        DBMS_OUTPUT.PUT_LINE('  UNDO tablespace: ' || v_undo_ts
            || ' (' || v_undo_size_gb || ' GB)');

        IF v_undo_size_gb < 2 THEN
            DBMS_OUTPUT.PUT_LINE('  UNDO already small. Skipping.');
            GOTO skip_undo;
        END IF;

        FOR target_gb IN 1..4 LOOP
            BEGIN
                EXECUTE IMMEDIATE
                    'ALTER DATABASE DATAFILE ''' || v_undo_file || ''' RESIZE '
                    || target_gb || 'G';
                DBMS_OUTPUT.PUT_LINE('  Resized to ' || target_gb || ' GB (freed '
                    || ROUND(v_undo_size_gb - target_gb, 2) || ' GB)');
                v_resize_ok := TRUE;
                EXIT;
            EXCEPTION WHEN OTHERS THEN NULL;
            END;
        END LOOP;

        IF NOT v_resize_ok THEN
            DBMS_OUTPUT.PUT_LINE('  Simple resize failed. Swapping UNDO tablespace...');
            v_undo_dir := SUBSTR(v_undo_file, 1,
                GREATEST(NVL(INSTR(v_undo_file, '/', -1), 0),
                         NVL(INSTR(v_undo_file, '\', -1), 0)));
            IF UPPER(v_undo_ts) LIKE '%2' THEN
                v_new_undo_ts   := 'UNDOTBS1';
                v_new_undo_file := v_undo_dir || 'undotbs01.dbf';
            ELSE
                v_new_undo_ts   := 'UNDOTBS2';
                v_new_undo_file := v_undo_dir || 'undotbs02.dbf';
            END IF;
            BEGIN
                EXECUTE IMMEDIATE
                    'CREATE UNDO TABLESPACE ' || v_new_undo_ts ||
                    ' DATAFILE ''' || v_new_undo_file || ''' SIZE 1G' ||
                    ' AUTOEXTEND ON NEXT 256M MAXSIZE UNLIMITED';
                BEGIN
                    EXECUTE IMMEDIATE
                        'ALTER SYSTEM SET undo_tablespace = '
                        || v_new_undo_ts || ' SCOPE=BOTH';
                EXCEPTION
                    WHEN OTHERS THEN
                        EXECUTE IMMEDIATE
                            'ALTER SYSTEM SET undo_tablespace = '
                            || v_new_undo_ts || ' SCOPE=MEMORY';
                        DBMS_OUTPUT.PUT_LINE('  WARNING: No spfile -- '
                            || 'undo_tablespace change is MEMORY only. '
                            || 'Update init.ora manually before next restart!');
                END;
                BEGIN
                    EXECUTE IMMEDIATE
                        'DROP TABLESPACE ' || v_undo_ts ||
                        ' INCLUDING CONTENTS AND DATAFILES';
                    DBMS_OUTPUT.PUT_LINE('  Swapped UNDO -> ' || v_new_undo_ts
                        || ', freed ~' || v_undo_size_gb || ' GB');
                EXCEPTION WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('  WARNING: Could not drop ' || v_undo_ts
                        || ': ' || SQLERRM);
                END;
            EXCEPTION WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('  UNDO swap failed: ' || SQLERRM);
            END;
        END IF;

        <<skip_undo>> NULL;
    END;

    FOR f IN (
        SELECT f.file_name, f.bytes,
               ROUND(f.bytes/1024/1024/1024, 2) AS size_gb
          FROM dba_temp_files f
         WHERE f.bytes > 1073741824
         ORDER BY f.file_name
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE
                'ALTER DATABASE TEMPFILE ''' || f.file_name || ''' RESIZE 1G';
            DBMS_OUTPUT.PUT_LINE('  TEMP ' || f.file_name || ': '
                || f.size_gb || ' GB -> 1 GB');
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('  TEMP ' || f.file_name
                    || ' could not shrink: ' || SQLERRM);
        END;
    END LOOP;

    -- Close the 'Shrink UNDO/TEMP' phase so its PHASE_END row is logged.
    phase_end;

EXCEPTION
    WHEN OTHERS THEN
        DECLARE
            v_err_msg VARCHAR2(4000) := SQLERRM;
        BEGIN
            -- Close any in-progress phase so its partial duration is captured.
            BEGIN phase_end; EXCEPTION WHEN OTHERS THEN NULL; END;

            -- Safety: if SCRATCH still exists, warn prominently.
            DECLARE
                v_scratch_exists NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_scratch_exists FROM dba_tablespaces
                 WHERE tablespace_name = 'EPF_SCRATCH';
                IF v_scratch_exists > 0 THEN
                    DBMS_OUTPUT.PUT_LINE('');
                    DBMS_OUTPUT.PUT_LINE('  !! EPF_SCRATCH tablespace still exists.');
                    DBMS_OUTPUT.PUT_LINE('  !! Some tables may be in SCRATCH. Check before dropping.');
                END IF;
            EXCEPTION WHEN OTHERS THEN NULL;
            END;

            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('============================================================');
            DBMS_OUTPUT.PUT_LINE('  RECLAIM FAILED');
            DBMS_OUTPUT.PUT_LINE('============================================================');
            DBMS_OUTPUT.PUT_LINE('  ' || v_err_msg);
            DBMS_OUTPUT.PUT_LINE('  Drop/recreate progress at failure:');
            DBMS_OUTPUT.PUT_LINE('    FK:    ' || v_dropped_fk || ' dropped, '
                || v_recreated_fk || ' recreated');
            DBMS_OUTPUT.PUT_LINE('    PK/UK: ' || v_dropped_pk || ' dropped, '
                || v_recreated_pk || ' recreated');
            DBMS_OUTPUT.PUT_LINE('    Idx:   ' || v_dropped_idx || ' dropped, '
                || v_recreated_idx || ' recreated');
            DBMS_OUTPUT.PUT_LINE('  Iterative drain progress at failure:');
            DBMS_OUTPUT.PUT_LINE('    Drained:  ' || v_drained || ' (failed: ' || v_drain_failed || ')');
            DBMS_OUTPUT.PUT_LINE('    Refilled: ' || v_refilled || ' (failed: ' || v_refill_failed || ')');
            print_phase_summary;
            IF v_idx_count > 0
               AND (v_recreated_idx < v_dropped_idx
                    OR v_recreated_pk < v_dropped_pk
                    OR v_recreated_fk < v_dropped_fk)
            THEN
                DBMS_OUTPUT.PUT_LINE('  WARNING: schema is in a partial state.'
                    || ' Some indexes / constraints have been dropped but not'
                    || ' recreated. Re-run reclaim or restore from backup'
                    || ' before bringing the application back online.');
            END IF;
            reclaim_log('RECLAIM_END', 'ERROR',
                'Reclaim aborted by exception: ' || SUBSTR(v_err_msg, 1, 3000)
                || ' [drop/recreate counts: fk=' || v_dropped_fk || '/' || v_recreated_fk
                || ', pk=' || v_dropped_pk || '/' || v_recreated_pk
                || ', idx=' || v_dropped_idx || '/' || v_recreated_idx
                || ', drained=' || v_drained || ', refilled=' || v_refilled || ']');
        END;
        RAISE;
END;
/
