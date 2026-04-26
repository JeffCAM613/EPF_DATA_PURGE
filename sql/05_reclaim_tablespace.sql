-- ============================================================================
-- EPF Data Purge - Online Tablespace Reclaim (Squeeze Method)
-- ============================================================================
-- Dynamically discovers and relocates segments pinning the High Water Mark
-- (HWM) of the OPPAYMENTS default tablespace, then resizes the datafile.
--
-- How it works:
--   1. Detect the OPPAYMENTS default tablespace and its datafile
--   2. SHRINK SPACE CASCADE on all tables to compact segments (frees
--      allocated-but-unused space back to the tablespace after a purge)
--   3. Squeeze loop: find the highest segment, squeeze the datafile ceiling,
--      MOVE/REBUILD the segment so it lands lower, repeat until HWM is near
--      actual used space
--   4. Resize the datafile to just above the final HWM
--
-- Usage:
--   @sql/05_reclaim_tablespace.sql
--
-- Prerequisites:
--   - Must be run as a user with ALTER TABLESPACE, ALTER DATABASE, DBA views
--     (SYS or a user with DBA role)
--   - Oracle 12.2+ required (uses MOVE ONLINE / REBUILD ONLINE)
--   - Tablespace must use ASSM (Automatic Segment Space Management)
--   - Application can remain fully online (row-level locks only)
--   - Tables with LOBs will have both the table and LOB segments moved
--
-- Parameters (set before running):
--   &1  = target_pct_free  - stop when HWM is within this % of used space
--                            (default 10 = stop when HWM < used_space * 1.1)
--   &2  = max_iterations   - safety limit to prevent infinite loops (default 2000)
--   &3  = skip_stall_checks - set to Y to disable stall detection and always
--                             run all max_iterations (default N)
-- ============================================================================

-- DBMS_OUTPUT is suppressed because the entire reclaim runs as one big
-- PL/SQL block and DBMS_OUTPUT only flushes at block end -- producing a
-- redundant flood of stale lines after the live monitor already showed
-- progress via epf_purge_log. All user-visible reclaim output now flows
-- through reclaim_log() -> epf_purge_log -> live monitor. To debug the
-- script standalone in sqlplus, change OFF to ON.
SET SERVEROUTPUT OFF
SET LINESIZE 200
SET FEEDBACK OFF
SET VERIFY OFF

-- Accept parameters via positional args. Wrapper scripts always pass all
-- three; ad-hoc invocations can use:
--   @05_reclaim_tablespace.sql 10 2000 N
-- (Previously this section did `DEFINE x = literal` which silently
--  overrode any wrapper-pre-DEFINE -- the wrapper's --no-stall-check and
--  --max-iterations had no effect. Switching to positional bindings
--  fixes that.)
DEFINE target_pct_free   = &1
DEFINE max_iterations    = &2
DEFINE skip_stall_checks = &3

DECLARE
    -- Configuration
    c_target_pct_free    CONSTANT NUMBER  := &target_pct_free;
    c_max_iterations     CONSTANT NUMBER  := &max_iterations;
    c_skip_stall_checks  CONSTANT BOOLEAN := UPPER('&skip_stall_checks') = 'Y';
    c_safety_margin_mb   CONSTANT NUMBER  := 50;   -- MB above HWM for resize ceiling
    c_stall_check_every  CONSTANT NUMBER  := 100;  -- check progress every N iterations
    c_stall_min_drop_gb  CONSTANT NUMBER  := 0;    -- any progress at all resets the counter
    c_max_stalls         CONSTANT NUMBER  := 3;    -- exit after N consecutive zero-progress checkpoints

    -- Working variables
    v_tablespace       VARCHAR2(128);
    v_datafile         VARCHAR2(513);
    v_df_bytes         NUMBER;
    v_block_size       NUMBER;
    v_hwm_gb           NUMBER;
    v_used_gb          NUMBER;
    v_used_gb_before   NUMBER;
    v_target_hwm_gb    NUMBER;
    v_iteration        NUMBER := 0;
    v_seg_owner        VARCHAR2(128);
    v_seg_name         VARCHAR2(128);
    v_seg_type         VARCHAR2(18);
    v_seg_end_gb       NUMBER;
    v_seg_mb           NUMBER;
    v_new_hwm_gb       NUMBER;
    v_resize_bytes     NUMBER;
    v_ceiling_bytes    NUMBER;
    v_initial_hwm_gb   NUMBER;
    v_initial_df_gb    NUMBER;
    v_has_lobs         NUMBER;
    v_shrink_count     NUMBER := 0;
    v_shrink_errors    NUMBER := 0;
    v_shrink_iter_seen        NUMBER := 0;        -- iterations entered (for cadence)
    v_shrink_last_logged_iter NUMBER := 0;        -- iter# of the last cadence log
    v_shrink_last_log_ts      TIMESTAMP;          -- wall-time of the last cadence log
    v_shrink_total_tables     NUMBER := 0;        -- total tables to shrink (for X/Y display)
    c_shrink_log_every_n  CONSTANT NUMBER := 10;  -- log SHRINK_PROGRESS every N tables
    c_shrink_log_max_secs CONSTANT NUMBER := 30;  -- ...or every N seconds, whichever is sooner
    -- The cadence log fires BEFORE each SHRINK SPACE CASCADE (with the
    -- in-flight table name) rather than after. This way, if a single SHRINK
    -- statement takes many minutes (common for tables with heavy LOB segments
    -- under high retention), the monitor's last visible line names the table
    -- currently being processed -- the user can verify progress via
    -- v$session_longops / dba_segments instead of assuming the run is hung.
    -- The wallclock floor (30s) bounds the silent gap between cadence logs.
    v_checkpoint_hwm   NUMBER := NULL;
    v_stall_count      NUMBER := 0;  -- consecutive stall checkpoints
    v_started          TIMESTAMP := SYSTIMESTAMP;

    -- run_id of the current purge run (for log entries)
    v_run_id           RAW(16) := NULL;

    -- Squeeze-progress cadence: log every N iterations. NO wallclock floor.
    -- A floor turns into "every iter" when individual MOVE/REBUILD ops are
    -- slow (LOB segments under high retention can take 90-300s each), which
    -- the user explicitly does not want. With pure 25-iter cadence, max
    -- log volume = c_max_iterations / 25 lines (~80 for 2000 max).
    -- The "Phase 2 squeeze starting" log fires before the loop, so the user
    -- always sees a phase boundary even before the first cadence-fired log.
    c_squeeze_log_every_n  CONSTANT NUMBER := 25;

    -- Cursor: find the segment whose highest extent is closest to the HWM
    -- This is the one pinning the HWM (or close to it)
    CURSOR c_top_segment(p_ts VARCHAR2) IS
        SELECT owner, segment_name, segment_type,
               ROUND(MAX(block_id + blocks) * v_block_size / 1024/1024/1024, 6) AS extent_end_gb,
               ROUND(SUM(bytes)/1024/1024, 1) AS segment_mb
        FROM dba_extents
        WHERE tablespace_name = p_ts
        GROUP BY owner, segment_name, segment_type
        ORDER BY extent_end_gb DESC
        FETCH FIRST 1 ROW ONLY;

    -- Cursor: find LOB columns for a table (to move LOB segments too)
    CURSOR c_lob_cols(p_owner VARCHAR2, p_table VARCHAR2) IS
        SELECT column_name
        FROM dba_lobs
        WHERE owner = p_owner AND table_name = p_table;

    -- Cursor: find unusable indexes after a table MOVE
    CURSOR c_unusable_idx(p_owner VARCHAR2, p_table VARCHAR2) IS
        SELECT owner, index_name
        FROM dba_indexes
        WHERE table_name = p_table
          AND table_owner = p_owner
          AND status = 'UNUSABLE';

    -- Helper: get current HWM in GB
    FUNCTION get_hwm_gb RETURN NUMBER IS
        l_hwm NUMBER;
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT ROUND(MAX(block_id + blocks) * :1 / 1024/1024/1024, 6)
             FROM dba_extents WHERE tablespace_name = :2'
            INTO l_hwm USING v_block_size, v_tablespace;
        RETURN NVL(l_hwm, 0);
    END;

    -- Helper: get total used space in GB
    FUNCTION get_used_gb RETURN NUMBER IS
        l_used NUMBER;
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT ROUND(SUM(bytes) / 1024/1024/1024, 4)
             FROM dba_segments WHERE tablespace_name = :1'
            INTO l_used USING v_tablespace;
        RETURN NVL(l_used, 0);
    END;

    -- Autonomous-transaction logger: writes to oppayments.epf_purge_log so the
    -- live monitor can display reclaim progress in real-time.
    PROCEDURE reclaim_log(
        p_operation IN VARCHAR2,
        p_status    IN VARCHAR2,
        p_message   IN VARCHAR2,
        p_elapsed   IN NUMBER DEFAULT NULL
    )
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        IF v_run_id IS NULL THEN
            COMMIT;
            RETURN;
        END IF;
        INSERT INTO oppayments.epf_purge_log (
            run_id, log_timestamp, module, operation, table_name,
            rows_affected, batch_number, retention_days, status,
            message, error_code, error_message, elapsed_seconds
        ) VALUES (
            v_run_id, SYSTIMESTAMP, 'RECLAIM', p_operation, NULL,
            0, NULL, NULL, p_status,
            SUBSTR(p_message, 1, 4000), NULL, NULL, p_elapsed
        );
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
    END reclaim_log;

BEGIN
    -- ========================================================================
    -- Step 1: Discover tablespace, datafile, block size
    -- ========================================================================
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT default_tablespace FROM dba_users WHERE username = ''OPPAYMENTS'''
            INTO v_tablespace;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: OPPAYMENTS user not found.');
            RETURN;
    END;

    BEGIN
        EXECUTE IMMEDIATE
            'SELECT file_name, bytes FROM dba_data_files
             WHERE tablespace_name = :1 FETCH FIRST 1 ROW ONLY'
            INTO v_datafile, v_df_bytes USING v_tablespace;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.PUT_LINE('ERROR: No datafile found for tablespace ' || v_tablespace);
            RETURN;
    END;

    EXECUTE IMMEDIATE
        'SELECT value FROM v$parameter WHERE name = ''db_block_size'''
        INTO v_block_size;

    v_hwm_gb   := get_hwm_gb;
    v_used_gb  := get_used_gb;
    v_used_gb_before := v_used_gb;
    v_target_hwm_gb := v_used_gb * (1 + c_target_pct_free / 100);
    v_initial_hwm_gb := v_hwm_gb;
    v_initial_df_gb  := ROUND(v_df_bytes / 1024/1024/1024, 2);

    -- Find the latest purge run_id so we can attach reclaim logs to it
    BEGIN
        EXECUTE IMMEDIATE
            'SELECT run_id FROM (
                SELECT run_id FROM oppayments.epf_purge_log
                WHERE operation = ''RUN_END''
                ORDER BY log_timestamp DESC
            ) WHERE ROWNUM = 1'
            INTO v_run_id;
    EXCEPTION
        WHEN OTHERS THEN v_run_id := SYS_GUID();  -- standalone reclaim, new id
    END;

    reclaim_log('RECLAIM_START', 'INFO',
        'Reclaim started. TS=' || v_tablespace
        || ', Datafile=' || v_initial_df_gb || 'GB'
        || ', HWM=' || ROUND(v_hwm_gb, 4) || 'GB'
        || ', Used=' || v_used_gb || 'GB'
        || ', Max iterations=' || c_max_iterations);

    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  EPF ONLINE TABLESPACE RECLAIM');
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  Tablespace:     ' || v_tablespace);
    DBMS_OUTPUT.PUT_LINE('  Datafile:       ' || v_datafile);
    DBMS_OUTPUT.PUT_LINE('  Datafile size:  ' || v_initial_df_gb || ' GB');
    DBMS_OUTPUT.PUT_LINE('  Block size:     ' || v_block_size);
    DBMS_OUTPUT.PUT_LINE('  HWM:            ' || ROUND(v_hwm_gb, 4) || ' GB');
    DBMS_OUTPUT.PUT_LINE('  Used space:     ' || v_used_gb || ' GB');
    DBMS_OUTPUT.PUT_LINE('  Max iterations: ' || c_max_iterations);
    DBMS_OUTPUT.PUT_LINE('  Stall checks:   ' || CASE WHEN c_skip_stall_checks THEN 'DISABLED (will run all ' || c_max_iterations || ' iterations)' ELSE 'ENABLED (exit after ' || c_max_stalls || ' consecutive stalls)' END);
    DBMS_OUTPUT.PUT_LINE('============================================================');

    -- ========================================================================
    -- Step 1b: SHRINK SPACE on all tables in the tablespace
    -- ========================================================================
    -- After a purge, segments still claim their original allocated size.
    -- SHRINK SPACE compacts rows within each segment and releases free
    -- blocks back to the tablespace, dramatically reducing "used space".
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 1: SHRINK SPACE (compacting segments) ===');

    -- Pre-count the table set so progress logs can show "X of Y processed".
    -- Filter MUST mirror the FOR loop's WHERE clause exactly.
    SELECT COUNT(*) INTO v_shrink_total_tables
    FROM dba_tables
    WHERE tablespace_name = v_tablespace
      AND temporary = 'N'
      AND secondary = 'N'
      AND iot_type IS NULL
      AND table_name NOT IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT');

    -- Force the very first iteration to fire a cadence log by setting the
    -- wallclock baseline far in the past. Without this, iter 1 only logs
    -- when 10 tables have already been processed -- meaning the user could
    -- wait through the first 10 SHRINK SPACE CASCADE calls (potentially many
    -- minutes apiece) before seeing any output.
    v_shrink_last_log_ts := SYSTIMESTAMP - INTERVAL '1' HOUR;

    FOR tbl IN (
        SELECT owner, table_name
        FROM dba_tables
        WHERE tablespace_name = v_tablespace
          AND temporary = 'N'
          AND secondary = 'N'
          AND iot_type IS NULL
          -- Exclude the purge log tables: the live monitor polls them
          -- concurrently and SHRINK SPACE takes row-level locks that
          -- block the monitor's SELECT, causing it to appear "stuck".
          AND table_name NOT IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT')
        ORDER BY owner, table_name
    ) LOOP
        v_shrink_iter_seen := v_shrink_iter_seen + 1;

        -- Pre-shrink heartbeat: log BEFORE the SHRINK starts so the monitor's
        -- last visible line always names the table currently in flight.
        -- Fires when EITHER cadence-N tables passed since last log, OR
        -- wallclock floor exceeded -- whichever comes first. The first
        -- iteration always fires (last_log_ts initialised in the past).
        IF v_shrink_iter_seen - v_shrink_last_logged_iter >= c_shrink_log_every_n
           OR (CAST(SYSTIMESTAMP AS DATE) - CAST(v_shrink_last_log_ts AS DATE)) * 86400
                  >= c_shrink_log_max_secs
        THEN
            reclaim_log('SHRINK_PROGRESS', 'INFO',
                'Shrinking ' || tbl.owner || '.' || tbl.table_name
                || ' (' || (v_shrink_iter_seen - 1) || '/' || v_shrink_total_tables
                || ' processed; ' || v_shrink_count || ' done, '
                || v_shrink_errors || ' skipped). If this line stays visible '
                || 'for several minutes, a single SHRINK SPACE CASCADE is in '
                || 'progress -- monitor reports nothing until it finishes.',
                ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400));
            v_shrink_last_logged_iter := v_shrink_iter_seen;
            v_shrink_last_log_ts := SYSTIMESTAMP;
        END IF;

        BEGIN
            -- Enable row movement (required for SHRINK)
            EXECUTE IMMEDIATE
                'ALTER TABLE ' || tbl.owner || '.' || tbl.table_name
                || ' ENABLE ROW MOVEMENT';

            -- Shrink table + dependent indexes + LOBs
            EXECUTE IMMEDIATE
                'ALTER TABLE ' || tbl.owner || '.' || tbl.table_name
                || ' SHRINK SPACE CASCADE';

            v_shrink_count := v_shrink_count + 1;

        EXCEPTION
            WHEN OTHERS THEN
                -- Common: tables with LONG columns, clustered tables, etc.
                v_shrink_errors := v_shrink_errors + 1;
                DBMS_OUTPUT.PUT_LINE('  SHRINK SKIP: ' || tbl.owner || '.' || tbl.table_name
                    || ' (' || SQLERRM || ')');
        END;
    END LOOP;

    -- Recalculate used space and target after shrink
    v_used_gb := get_used_gb;
    v_hwm_gb  := get_hwm_gb;
    v_target_hwm_gb := v_used_gb * (1 + c_target_pct_free / 100);

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('  Shrink summary:');
    DBMS_OUTPUT.PUT_LINE('    Tables shrunk:  ' || v_shrink_count);
    DBMS_OUTPUT.PUT_LINE('    Tables skipped: ' || v_shrink_errors);
    DBMS_OUTPUT.PUT_LINE('    Used before:    ' || v_used_gb_before || ' GB');
    DBMS_OUTPUT.PUT_LINE('    Used after:     ' || v_used_gb || ' GB');
    DBMS_OUTPUT.PUT_LINE('    Space freed:    '
        || ROUND(v_used_gb_before - v_used_gb, 4) || ' GB');
    DBMS_OUTPUT.PUT_LINE('    HWM:            ' || ROUND(v_hwm_gb, 4) || ' GB');
    DBMS_OUTPUT.PUT_LINE('    Target HWM:     <= ' || ROUND(v_target_hwm_gb, 4) || ' GB'
        || ' (used + ' || c_target_pct_free || '%)');
    DBMS_OUTPUT.PUT_LINE('');

    reclaim_log('SHRINK_DONE', 'SUCCESS',
        'Phase 1 done. Shrunk=' || v_shrink_count || ' tables'
        || ', Used=' || v_used_gb_before || '->' || v_used_gb || 'GB'
        || ' (freed ' || ROUND(v_used_gb_before - v_used_gb, 2) || 'GB)'
        || ', HWM=' || ROUND(v_hwm_gb, 4) || 'GB'
        || ', Target=' || ROUND(v_target_hwm_gb, 4) || 'GB',
        ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400));

    -- Already below target after shrink?
    IF v_hwm_gb <= v_target_hwm_gb THEN
        DBMS_OUTPUT.PUT_LINE('HWM is already below target after shrink. Skipping squeeze.');
        DBMS_OUTPUT.PUT_LINE('Attempting final datafile resize...');
        v_resize_bytes := CEIL((v_hwm_gb + c_safety_margin_mb / 1024) * 1024) * 1024 * 1024;
        IF v_resize_bytes < v_df_bytes THEN
            BEGIN
                EXECUTE IMMEDIATE
                    'ALTER DATABASE DATAFILE ''' || v_datafile || ''' RESIZE ' || v_resize_bytes;
                DBMS_OUTPUT.PUT_LINE('Datafile resized to '
                    || ROUND(v_resize_bytes / 1024/1024/1024, 2) || ' GB');
                DBMS_OUTPUT.PUT_LINE('Space freed: '
                    || ROUND(v_initial_df_gb - v_resize_bytes / 1024/1024/1024, 2) || ' GB');
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('Datafile resize failed: ' || SQLERRM);
            END;
        ELSE
            DBMS_OUTPUT.PUT_LINE('Datafile already at minimum size.');
        END IF;
        GOTO reclaim_done;
    END IF;

    -- ========================================================================
    -- Step 2: Iterative squeeze loop (defragment HWM)
    -- ========================================================================
    DBMS_OUTPUT.PUT_LINE('=== Phase 2: SQUEEZE (lowering HWM from '
        || ROUND(v_hwm_gb, 4) || ' to ' || ROUND(v_target_hwm_gb, 4) || ' GB) ===');

    -- Live SQUEEZE_START so the monitor draws a clear phase boundary
    -- before the (potentially long) wait for the first cadence-fired log.
    reclaim_log('SQUEEZE_PROGRESS', 'INFO',
        'Phase 2 squeeze starting. HWM=' || ROUND(v_hwm_gb, 4) || 'GB'
        || ', Target=' || ROUND(v_target_hwm_gb, 4) || 'GB'
        || ', MaxIter=' || c_max_iterations,
        ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400));

    LOOP
        v_iteration := v_iteration + 1;
        IF v_iteration > c_max_iterations THEN
            -- Surface the cap-hit reason live so the user knows reclaim
            -- exited because of the iteration limit (not because the target
            -- was reached). This drives the "consider re-running with a
            -- larger --max-iterations" recommendation in the wrapper.
            reclaim_log('SQUEEZE_PROGRESS', 'WARNING',
                'Squeeze stopped: max iterations (' || c_max_iterations
                || ') reached. HWM=' || ROUND(v_hwm_gb, 4) || 'GB still'
                || ' above target ' || ROUND(v_target_hwm_gb, 4) || 'GB.'
                || ' Re-run reclaim with a larger --max-iterations to continue.',
                ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400));
            EXIT;
        END IF;

        v_hwm_gb := get_hwm_gb;

        -- Check if we've reached the target
        IF v_hwm_gb <= v_target_hwm_gb THEN
            reclaim_log('SQUEEZE_PROGRESS', 'INFO',
                'Squeeze done at iter ' || v_iteration || ': HWM='
                || ROUND(v_hwm_gb, 4) || 'GB <= target '
                || ROUND(v_target_hwm_gb, 4) || 'GB.',
                ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400));
            EXIT;
        END IF;

        -- Find the top segment (pinning HWM)
        OPEN c_top_segment(v_tablespace);
        FETCH c_top_segment INTO v_seg_owner, v_seg_name, v_seg_type, v_seg_end_gb, v_seg_mb;
        CLOSE c_top_segment;

        DBMS_OUTPUT.PUT_LINE('[' || v_iteration || '] HWM=' || ROUND(v_hwm_gb, 4)
            || 'GB  Top: ' || v_seg_owner || '.' || v_seg_name
            || ' (' || v_seg_type || ', ' || v_seg_mb || 'MB, ends@' || ROUND(v_seg_end_gb, 4) || 'GB)');

        -- Squeeze the ceiling: resize datafile to just above HWM
        -- This prevents Oracle from placing new extents at the top
        v_ceiling_bytes := CEIL(v_hwm_gb * 1024 + c_safety_margin_mb) * 1024 * 1024;
        IF v_ceiling_bytes < v_df_bytes THEN
            BEGIN
                EXECUTE IMMEDIATE
                    'ALTER DATABASE DATAFILE ''' || v_datafile || ''' RESIZE ' || v_ceiling_bytes;
                v_df_bytes := v_ceiling_bytes;
                DBMS_OUTPUT.PUT_LINE('       Ceiling squeezed to '
                    || ROUND(v_ceiling_bytes / 1024/1024/1024, 2) || ' GB');
            EXCEPTION
                WHEN OTHERS THEN
                    -- ORA-03297: can't resize below used data. Try a bit higher.
                    IF SQLCODE = -3297 THEN
                        DBMS_OUTPUT.PUT_LINE('       Ceiling squeeze skipped (data at boundary)');
                    ELSE
                        RAISE;
                    END IF;
            END;
        END IF;

        -- Move/Rebuild the top segment (ONLINE to avoid table-level locks)
        IF v_seg_type = 'TABLE' THEN
            -- Move table online (row-level locks only, requires Oracle 12.2+)
            BEGIN
                EXECUTE IMMEDIATE
                    'ALTER TABLE ' || v_seg_owner || '.' || v_seg_name
                    || ' MOVE ONLINE TABLESPACE ' || v_tablespace;
            EXCEPTION
                WHEN OTHERS THEN
                    -- ORA-00997: tables with LONG/LONG RAW columns cannot be MOVEd
                    IF SQLCODE = -997 THEN
                        DBMS_OUTPUT.PUT_LINE('       SKIP (LONG column): '
                            || v_seg_owner || '.' || v_seg_name
                            || ' - cannot MOVE tables with LONG datatype');
                        GOTO next_iteration;
                    ELSE
                        RAISE;
                    END IF;
            END;

            -- Move any LOB columns of this table
            FOR lob_rec IN c_lob_cols(v_seg_owner, v_seg_name) LOOP
                BEGIN
                    EXECUTE IMMEDIATE
                        'ALTER TABLE ' || v_seg_owner || '.' || v_seg_name
                        || ' MOVE LOB (' || lob_rec.column_name
                        || ') STORE AS (TABLESPACE ' || v_tablespace || ')';
                    DBMS_OUTPUT.PUT_LINE('       Moved LOB: ' || lob_rec.column_name);
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('       LOB move failed (' || lob_rec.column_name
                            || '): ' || SQLERRM);
                END;
            END LOOP;

            -- Rebuild any unusable indexes on this table (safety net; MOVE ONLINE
            -- keeps indexes valid, but check anyway in case of partial failure)
            FOR idx_rec IN c_unusable_idx(v_seg_owner, v_seg_name) LOOP
                BEGIN
                    EXECUTE IMMEDIATE
                        'ALTER INDEX ' || idx_rec.owner || '.' || idx_rec.index_name || ' REBUILD ONLINE';
                    DBMS_OUTPUT.PUT_LINE('       Rebuilt index: '
                        || idx_rec.owner || '.' || idx_rec.index_name);
                EXCEPTION
                    WHEN OTHERS THEN
                        DBMS_OUTPUT.PUT_LINE('       Index rebuild failed ('
                            || idx_rec.index_name || '): ' || SQLERRM);
                END;
            END LOOP;

        ELSIF v_seg_type = 'INDEX' THEN
            EXECUTE IMMEDIATE
                'ALTER INDEX ' || v_seg_owner || '.' || v_seg_name || ' REBUILD ONLINE';

        ELSIF v_seg_type = 'LOBSEGMENT' THEN
            -- Find the parent table for this LOB segment and move it
            DECLARE
                v_lob_owner  VARCHAR2(128);
                v_lob_table  VARCHAR2(128);
                v_lob_column VARCHAR2(128);
            BEGIN
                EXECUTE IMMEDIATE
                    'SELECT owner, table_name, column_name FROM dba_lobs
                     WHERE segment_name = :1 FETCH FIRST 1 ROW ONLY'
                    INTO v_lob_owner, v_lob_table, v_lob_column
                    USING v_seg_name;

                EXECUTE IMMEDIATE
                    'ALTER TABLE ' || v_lob_owner || '.' || v_lob_table
                    || ' MOVE LOB (' || v_lob_column
                    || ') STORE AS (TABLESPACE ' || v_tablespace || ')';
                DBMS_OUTPUT.PUT_LINE('       Moved LOB: '
                    || v_lob_owner || '.' || v_lob_table || '(' || v_lob_column || ')');

                -- Rebuild unusable indexes after LOB move
                FOR idx_rec IN c_unusable_idx(v_lob_owner, v_lob_table) LOOP
                    BEGIN
                        EXECUTE IMMEDIATE
                            'ALTER INDEX ' || idx_rec.owner || '.' || idx_rec.index_name
                            || ' REBUILD ONLINE';
                        DBMS_OUTPUT.PUT_LINE('       Rebuilt index: '
                            || idx_rec.owner || '.' || idx_rec.index_name);
                    EXCEPTION
                        WHEN OTHERS THEN
                            DBMS_OUTPUT.PUT_LINE('       Index rebuild failed ('
                                || idx_rec.index_name || '): ' || SQLERRM);
                    END;
                END LOOP;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    DBMS_OUTPUT.PUT_LINE('       WARNING: No parent table found for LOB segment '
                        || v_seg_name || '. Skipping.');
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('       LOB segment move failed: ' || SQLERRM);
            END;

        ELSIF v_seg_type = 'LOBINDEX' THEN
            -- LOB indexes are moved implicitly when the LOB segment moves.
            -- Find parent LOB segment and move it.
            DECLARE
                v_lob_owner  VARCHAR2(128);
                v_lob_table  VARCHAR2(128);
                v_lob_column VARCHAR2(128);
            BEGIN
                EXECUTE IMMEDIATE
                    'SELECT owner, table_name, column_name FROM dba_lobs
                     WHERE index_name = :1 FETCH FIRST 1 ROW ONLY'
                    INTO v_lob_owner, v_lob_table, v_lob_column
                    USING v_seg_name;

                EXECUTE IMMEDIATE
                    'ALTER TABLE ' || v_lob_owner || '.' || v_lob_table
                    || ' MOVE LOB (' || v_lob_column
                    || ') STORE AS (TABLESPACE ' || v_tablespace || ')';
                DBMS_OUTPUT.PUT_LINE('       Moved LOB (via index): '
                    || v_lob_owner || '.' || v_lob_table || '(' || v_lob_column || ')');

                FOR idx_rec IN c_unusable_idx(v_lob_owner, v_lob_table) LOOP
                    BEGIN
                        EXECUTE IMMEDIATE
                            'ALTER INDEX ' || idx_rec.owner || '.' || idx_rec.index_name
                            || ' REBUILD ONLINE';
                        DBMS_OUTPUT.PUT_LINE('       Rebuilt index: '
                            || idx_rec.owner || '.' || idx_rec.index_name);
                    EXCEPTION
                        WHEN OTHERS THEN
                            DBMS_OUTPUT.PUT_LINE('       Index rebuild failed ('
                                || idx_rec.index_name || '): ' || SQLERRM);
                    END;
                END LOOP;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    DBMS_OUTPUT.PUT_LINE('       WARNING: No parent table found for LOB index '
                        || v_seg_name || '. Skipping.');
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('       LOB index move failed: ' || SQLERRM);
            END;

        ELSE
            DBMS_OUTPUT.PUT_LINE('       WARNING: Unknown segment type ' || v_seg_type
                || '. Skipping.');
        END IF;

        <<next_iteration>>
        -- Check if HWM actually dropped
        v_new_hwm_gb := get_hwm_gb;
        IF v_new_hwm_gb >= v_hwm_gb THEN
            DBMS_OUTPUT.PUT_LINE('       HWM unchanged (' || ROUND(v_new_hwm_gb, 4)
                || ' GB). Will retry with tighter ceiling.');
        ELSE
            DBMS_OUTPUT.PUT_LINE('       HWM dropped: ' || ROUND(v_hwm_gb, 4)
                || ' -> ' || ROUND(v_new_hwm_gb, 4) || ' GB');
        END IF;

        -- Stall detection: every N iterations, check if meaningful progress was made
        IF v_checkpoint_hwm IS NULL THEN
            v_checkpoint_hwm := v_new_hwm_gb;
        END IF;

        -- SQUEEZE_PROGRESS cadence: every N iters. No wallclock floor.
        -- For 2000 max iters at N=25 that yields at most ~80 lines.
        IF MOD(v_iteration, c_squeeze_log_every_n) = 0 THEN
            reclaim_log('SQUEEZE_PROGRESS', 'INFO',
                'Iter ' || v_iteration || '/' || c_max_iterations
                || ', HWM=' || ROUND(v_new_hwm_gb, 4) || 'GB'
                || ', Target=' || ROUND(v_target_hwm_gb, 4) || 'GB'
                || ', Segment=' || v_seg_owner || '.' || v_seg_name
                || ' (' || v_seg_type || ')',
                ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400));
        END IF;

        IF MOD(v_iteration, c_stall_check_every) = 0 THEN
            -- Recalculate target: MOVE/REBUILD compacts segments further,
            -- so used_gb drops during squeeze. Refresh to get a tighter target.
            v_used_gb := get_used_gb;
            v_target_hwm_gb := v_used_gb * (1 + c_target_pct_free / 100);

            IF v_checkpoint_hwm - v_new_hwm_gb <= c_stall_min_drop_gb THEN
                v_stall_count := v_stall_count + 1;
                IF NOT c_skip_stall_checks AND v_stall_count >= c_max_stalls THEN
                    reclaim_log('SQUEEZE_PROGRESS', 'WARNING',
                        'Squeeze stopped: ' || c_max_stalls || ' consecutive '
                        || 'zero-progress checkpoints at iter ' || v_iteration
                        || '. HWM=' || ROUND(v_new_hwm_gb, 4) || 'GB still'
                        || ' above target ' || ROUND(v_target_hwm_gb, 4) || 'GB.'
                        || ' Pass --no-stall-check to keep going,'
                        || ' or re-run with --max-iterations to extend.',
                        ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400));
                    EXIT;
                ELSE
                    reclaim_log('SQUEEZE_PROGRESS', 'WARNING',
                        'Stall ' || v_stall_count || '/' || c_max_stalls
                        || ' at iter ' || v_iteration || ': HWM dropped only '
                        || ROUND(v_checkpoint_hwm - v_new_hwm_gb, 4) || 'GB in '
                        || 'last ' || c_stall_check_every || ' iters. '
                        || CASE WHEN c_skip_stall_checks THEN 'Stall checks disabled, continuing.'
                                ELSE 'Continuing with tighter ceiling.' END,
                        ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400));
                END IF;
            ELSE
                v_stall_count := 0;  -- reset on progress
            END IF;
            v_checkpoint_hwm := v_new_hwm_gb;
        END IF;
    END LOOP;

    -- ========================================================================
    -- Step 3: Final datafile resize
    -- ========================================================================
    <<reclaim_done>>
    v_hwm_gb := get_hwm_gb;
    v_used_gb := get_used_gb;
    v_resize_bytes := CEIL((v_hwm_gb + c_safety_margin_mb / 1024) * 1024) * 1024 * 1024;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  RECLAIM COMPLETE');
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  Shrink:');
    DBMS_OUTPUT.PUT_LINE('    Tables shrunk:  ' || v_shrink_count
        || '  (skipped: ' || v_shrink_errors || ')');
    DBMS_OUTPUT.PUT_LINE('    Used space:     ' || v_used_gb_before
        || ' GB -> ' || v_used_gb || ' GB'
        || ' (freed ' || ROUND(v_used_gb_before - v_used_gb, 2) || ' GB)');
    DBMS_OUTPUT.PUT_LINE('  Squeeze:');
    DBMS_OUTPUT.PUT_LINE('    Iterations:     ' || v_iteration);
    DBMS_OUTPUT.PUT_LINE('    HWM:            ' || ROUND(v_initial_hwm_gb, 4)
        || ' GB -> ' || ROUND(v_hwm_gb, 4) || ' GB');
    DBMS_OUTPUT.PUT_LINE('  Datafile:');
    DBMS_OUTPUT.PUT_LINE('    Before:         ' || v_initial_df_gb || ' GB');

    BEGIN
        EXECUTE IMMEDIATE
            'ALTER DATABASE DATAFILE ''' || v_datafile || ''' RESIZE ' || v_resize_bytes;
        DBMS_OUTPUT.PUT_LINE('    After:          '
            || ROUND(v_resize_bytes / 1024/1024/1024, 2) || ' GB');
        DBMS_OUTPUT.PUT_LINE('    OS space freed: '
            || ROUND(v_initial_df_gb - v_resize_bytes / 1024/1024/1024, 2) || ' GB');
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('    Resize failed:  ' || SQLERRM);
            DBMS_OUTPUT.PUT_LINE('    Current HWM: ' || ROUND(v_hwm_gb, 4)
                || ' GB. Manual resize may be needed.');
    END;

    DBMS_OUTPUT.PUT_LINE('  Duration:         '
        || ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400)
        || 's');
    DBMS_OUTPUT.PUT_LINE('============================================================');

    reclaim_log('RECLAIM_END', 'SUCCESS',
        'Reclaim complete. Iterations=' || v_iteration
        || ', HWM=' || ROUND(v_initial_hwm_gb, 4) || '->' || ROUND(v_hwm_gb, 4) || 'GB'
        || ', Used=' || v_used_gb_before || '->' || v_used_gb || 'GB'
        || ', Datafile=' || v_initial_df_gb || '->'
        || ROUND(v_resize_bytes / 1024/1024/1024, 2) || 'GB',
        ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400));

    -- ========================================================================
    -- Step 4: Shrink UNDO and TEMP tablespaces
    -- ========================================================================
    -- After bulk deletes, the undo tablespace can be bloated with old undo data.
    -- Simple resize often fails (ORA-03297) because undo extents are scattered.
    -- Solution: create a new UNDO tablespace, switch to it, drop the old one.
    -- This deletes the physical file immediately (including contents and datafiles).
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Phase 4: Shrink UNDO and TEMP tablespaces ===');

    -- Shrink UNDO: try resize first, fall back to tablespace swap
    DECLARE
        v_undo_ts        VARCHAR2(128);
        v_undo_file      VARCHAR2(513);
        v_undo_dir       VARCHAR2(513);
        v_undo_size_gb   NUMBER;
        v_new_undo_ts    VARCHAR2(128);
        v_new_undo_file  VARCHAR2(513);
        v_resize_ok      BOOLEAN := FALSE;
    BEGIN
        -- Get current UNDO tablespace name and its datafile
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

        DBMS_OUTPUT.PUT_LINE('  UNDO tablespace: ' || v_undo_ts);
        DBMS_OUTPUT.PUT_LINE('  UNDO file:       ' || v_undo_file || ' (' || v_undo_size_gb || ' GB)');

        -- Skip if already small (< 2 GB)
        IF v_undo_size_gb < 2 THEN
            DBMS_OUTPUT.PUT_LINE('  UNDO already small (' || v_undo_size_gb || ' GB). Skipping.');
            GOTO skip_undo;
        END IF;

        -- Try simple resize first (1G, 2G, 4G)
        FOR target_gb IN 1..4 LOOP
            BEGIN
                EXECUTE IMMEDIATE
                    'ALTER DATABASE DATAFILE ''' || v_undo_file || ''' RESIZE '
                    || target_gb || 'G';
                DBMS_OUTPUT.PUT_LINE('  Resized to ' || target_gb || ' GB (freed '
                    || ROUND(v_undo_size_gb - target_gb, 2) || ' GB)');
                v_resize_ok := TRUE;
                EXIT;
            EXCEPTION
                WHEN OTHERS THEN NULL;  -- try next size
            END;
        END LOOP;

        -- If resize failed, swap tablespace: create new → switch → drop old
        IF NOT v_resize_ok THEN
            DBMS_OUTPUT.PUT_LINE('  Simple resize failed (undo extents scattered).');
            DBMS_OUTPUT.PUT_LINE('  Swapping to a new UNDO tablespace...');

            -- Derive directory from existing file path
            v_undo_dir := SUBSTR(v_undo_file, 1,
                GREATEST(NVL(INSTR(v_undo_file, '/', -1), 0),
                         NVL(INSTR(v_undo_file, '\', -1), 0)));

            -- Name the new tablespace (toggle between UNDOTBS1/UNDOTBS2)
            IF UPPER(v_undo_ts) LIKE '%2' THEN
                v_new_undo_ts   := 'UNDOTBS1';
                v_new_undo_file := v_undo_dir || 'undotbs01.dbf';
            ELSE
                v_new_undo_ts   := 'UNDOTBS2';
                v_new_undo_file := v_undo_dir || 'undotbs02.dbf';
            END IF;

            BEGIN
                -- Create the new UNDO tablespace (1 GB, autoextend to unlimited)
                EXECUTE IMMEDIATE
                    'CREATE UNDO TABLESPACE ' || v_new_undo_ts ||
                    ' DATAFILE ''' || v_new_undo_file || ''' SIZE 1G' ||
                    ' AUTOEXTEND ON NEXT 256M MAXSIZE UNLIMITED';
                DBMS_OUTPUT.PUT_LINE('  Created: ' || v_new_undo_ts ||
                    ' (' || v_new_undo_file || ', 1 GB)');

                -- Switch to the new tablespace
                EXECUTE IMMEDIATE
                    'ALTER SYSTEM SET undo_tablespace = ' || v_new_undo_ts;
                DBMS_OUTPUT.PUT_LINE('  Switched undo_tablespace to ' || v_new_undo_ts);

                -- Drop the old tablespace (deletes the physical file)
                BEGIN
                    EXECUTE IMMEDIATE
                        'DROP TABLESPACE ' || v_undo_ts ||
                        ' INCLUDING CONTENTS AND DATAFILES';
                    DBMS_OUTPUT.PUT_LINE('  Dropped old tablespace: ' || v_undo_ts);
                    DBMS_OUTPUT.PUT_LINE('  Freed ~' || v_undo_size_gb || ' GB on disk');
                EXCEPTION
                    WHEN OTHERS THEN
                        -- May fail if active transactions still reference old undo
                        DBMS_OUTPUT.PUT_LINE('  WARNING: Could not drop ' || v_undo_ts
                            || ': ' || SQLERRM);
                        DBMS_OUTPUT.PUT_LINE('  The new tablespace is active. Old one can be');
                        DBMS_OUTPUT.PUT_LINE('  dropped later: DROP TABLESPACE ' || v_undo_ts
                            || ' INCLUDING CONTENTS AND DATAFILES;');
                END;
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('  UNDO swap failed: ' || SQLERRM);
                    DBMS_OUTPUT.PUT_LINE('  Manual cleanup needed. See docs/space_reclamation_guide.md');
            END;
        END IF;

        <<skip_undo>>
        NULL;
    END;

    -- Shrink TEMP tablespace
    FOR f IN (
        SELECT f.file_name, f.bytes,
               ROUND(f.bytes/1024/1024/1024, 2) AS size_gb
        FROM dba_temp_files f
        WHERE f.bytes > 1073741824  -- only if > 1 GB
        ORDER BY f.file_name
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  TEMP file: ' || f.file_name || ' (' || f.size_gb || ' GB)');
        BEGIN
            EXECUTE IMMEDIATE
                'ALTER DATABASE TEMPFILE ''' || f.file_name || ''' RESIZE 1G';
            DBMS_OUTPUT.PUT_LINE('    Resized to 1 GB (freed '
                || ROUND(f.size_gb - 1, 2) || ' GB)');
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('    Could not shrink: ' || SQLERRM);
        END;
    END LOOP;

    -- ========================================================================
    -- Step 5: Report orphaned redo log files
    -- ========================================================================
    -- When redo log groups are dropped, Oracle removes the group from the
    -- control file but does NOT delete the physical OS file. Report them.
    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('=== Orphaned Redo Log Files ===');
    DBMS_OUTPUT.PUT_LINE('  Oracle does not delete physical files when log groups are dropped.');
    DBMS_OUTPUT.PUT_LINE('  Active log files:');
    FOR r IN (SELECT member FROM v$logfile ORDER BY group#) LOOP
        DBMS_OUTPUT.PUT_LINE('    ' || r.member);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE('  Delete any other G*_1.log files in the same directory manually.');

EXCEPTION
    -- Top-level safety net: any unhandled exception during reclaim emits a
    -- RECLAIM_END (status=ERROR) so the live monitor terminates promptly
    -- instead of waiting for the idle timeout. The exception is then re-raised
    -- so the wrapper script also sees the failure.
    WHEN OTHERS THEN
        DECLARE
            v_err_msg VARCHAR2(4000) := SQLERRM;
            v_elapsed NUMBER :=
                ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(v_started AS DATE)) * 86400);
        BEGIN
            DBMS_OUTPUT.PUT_LINE('');
            DBMS_OUTPUT.PUT_LINE('============================================================');
            DBMS_OUTPUT.PUT_LINE('  RECLAIM FAILED');
            DBMS_OUTPUT.PUT_LINE('============================================================');
            DBMS_OUTPUT.PUT_LINE('  ' || v_err_msg);
            reclaim_log('RECLAIM_END', 'ERROR',
                'Reclaim aborted by exception: ' || SUBSTR(v_err_msg, 1, 3500),
                v_elapsed);
        END;
        RAISE;
END;
/
