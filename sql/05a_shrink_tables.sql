-- ============================================================================
-- EPF Data Purge - Standalone SHRINK SPACE (post-purge segment compaction)
-- ============================================================================
-- Runs SHRINK SPACE on all OPPAYMENTS tables + LOB segments in the data
-- tablespace. This is an in-place compaction that reduces segment sizes
-- without moving data between blocks. It does NOT lower the datafile HWM
-- (only MOVE + RESIZE does that), but it DOES reduce the segment bytes
-- reported by dba_segments -- which is what the space comparison measures.
--
-- Two passes:
--   1. SHRINK SPACE CASCADE on every table (compacts row data + SecureFile LOBs)
--   2. MODIFY LOB ... (SHRINK SPACE) on every LOB column individually
--      (catches BasicFile LOBs that CASCADE can't handle, and SecureFile LOBs
--      whose parent table failed SHRINK due to other issues like LONG columns)
--
-- Why standalone?
--   DELETE alone leaves empty blocks inside segments. Without SHRINK the
--   space comparison after a purge shows zero change (segment sizes unchanged).
--   This script makes the purge result visible in the BEFORE/AFTER report
--   without requiring the heavy reclaim (drop indexes / MOVE / resize).
--
-- Prerequisites:
--   - Run as OPPAYMENTS (owns the tables it shrinks)
--   - Row movement is enabled per-table before each SHRINK
--   - Tables with LONG columns or other incompatibilities fail silently
--
-- Limitations:
--   - BasicFile LOBs do NOT support SHRINK SPACE (ORA-10635). These can only
--     be compacted via ALTER TABLE ... MOVE LOB (which invalidates indexes).
--     The full reclaim (05_reclaim_tablespace.sql) handles those.
--   - SecureFile LOBs shrink well. MODIFY LOB ... (SHRINK SPACE) compacts them.
--
-- Called by: epf_purge.sh / epf_purge.bat after purge completes (unless dry run)
-- Also still embedded in: 05_reclaim_tablespace.sql Phase 4a (for reclaim-only)
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK OFF

DECLARE
    v_data_ts       VARCHAR2(30);
    v_shrink_count  NUMBER := 0;
    v_shrink_errors NUMBER := 0;
    v_shrink_total  NUMBER := 0;
    v_lob_count     NUMBER := 0;
    v_lob_errors    NUMBER := 0;
    v_lob_total     NUMBER := 0;
    v_run_id        RAW(16);
    v_start_ts      TIMESTAMP := SYSTIMESTAMP;

    -- Log to epf_purge_log so the monitor can display progress
    PROCEDURE shrink_log(p_operation VARCHAR2, p_status VARCHAR2, p_message VARCHAR2) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO oppayments.epf_purge_log (
            run_id, log_timestamp, module, operation, status, message,
            elapsed_seconds
        ) VALUES (
            v_run_id, SYSTIMESTAMP, 'SHRINK', p_operation, p_status, p_message,
            EXTRACT(SECOND FROM (SYSTIMESTAMP - v_start_ts))
            + EXTRACT(MINUTE FROM (SYSTIMESTAMP - v_start_ts)) * 60
        );
        COMMIT;
    END;
BEGIN
    -- Discover the data tablespace from the connected user's default tablespace
    SELECT default_tablespace INTO v_data_ts
      FROM user_users;

    -- Get the current run_id (from the purge that just completed)
    BEGIN
        SELECT run_id INTO v_run_id FROM (
            SELECT run_id FROM oppayments.epf_purge_log
            WHERE operation = 'RUN_START'
            ORDER BY log_timestamp DESC
        ) WHERE ROWNUM = 1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_run_id := SYS_GUID();
    END;

    shrink_log('SHRINK_PROGRESS', 'INFO',
        'Starting SHRINK SPACE (tablespace: ' || v_data_ts || ')');

    -- ====================================================================
    -- Pass 1: SHRINK SPACE CASCADE on every table
    -- ====================================================================
    SELECT COUNT(*) INTO v_shrink_total
      FROM user_tables
     WHERE tablespace_name = v_data_ts
       AND temporary = 'N'
       AND secondary = 'N'
       AND iot_type IS NULL
       AND table_name NOT IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT');

    DBMS_OUTPUT.PUT_LINE('=== SHRINK SPACE: in-place compaction (tablespace: ' || v_data_ts || ') ===');
    DBMS_OUTPUT.PUT_LINE('  Pass 1: SHRINK SPACE CASCADE on ' || v_shrink_total || ' tables...');

    FOR tbl IN (
        SELECT table_name
          FROM user_tables
         WHERE tablespace_name = v_data_ts
           AND temporary = 'N'
           AND secondary = 'N'
           AND iot_type IS NULL
           AND table_name NOT IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT')
         ORDER BY table_name
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER TABLE "' || tbl.table_name
                || '" ENABLE ROW MOVEMENT';
            EXECUTE IMMEDIATE 'ALTER TABLE "' || tbl.table_name
                || '" SHRINK SPACE CASCADE';
            v_shrink_count := v_shrink_count + 1;
        EXCEPTION
            WHEN OTHERS THEN
                -- Tables with LONG columns, functional-index deps, etc.
                v_shrink_errors := v_shrink_errors + 1;
        END;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('  Tables shrunk: ' || v_shrink_count || '/' || v_shrink_total
        || ' (skipped ' || v_shrink_errors || ')');

    shrink_log('SHRINK_PROGRESS', 'INFO',
        'Pass 1 done. Tables shrunk=' || v_shrink_count || '/' || v_shrink_total
        || ' (skipped ' || v_shrink_errors || ')');

    -- ====================================================================
    -- Pass 2: MODIFY LOB ... (SHRINK SPACE) on each LOB column
    -- ====================================================================
    -- Catches LOB segments that CASCADE didn't reach (BasicFile LOBs will
    -- error with ORA-10635 and be skipped; SecureFile LOBs will compact).
    SELECT COUNT(*) INTO v_lob_total
      FROM user_lobs
     WHERE tablespace_name = v_data_ts
       AND table_name NOT IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT');

    IF v_lob_total > 0 THEN
        DBMS_OUTPUT.PUT_LINE('  Pass 2: SHRINK LOB segments (' || v_lob_total || ' LOBs)...');

        FOR lob_rec IN (
            SELECT table_name, column_name
              FROM user_lobs
             WHERE tablespace_name = v_data_ts
               AND table_name NOT IN ('EPF_PURGE_LOG', 'EPF_PURGE_SPACE_SNAPSHOT')
             ORDER BY table_name, column_name
        ) LOOP
            BEGIN
                EXECUTE IMMEDIATE 'ALTER TABLE "' || lob_rec.table_name
                    || '" MODIFY LOB ("' || lob_rec.column_name || '") (SHRINK SPACE)';
                v_lob_count := v_lob_count + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    -- ORA-10635: Invalid segment or tablespace type (BasicFile LOBs)
                    -- or other incompatibility -- skip silently
                    v_lob_errors := v_lob_errors + 1;
            END;
        END LOOP;

        DBMS_OUTPUT.PUT_LINE('  LOBs shrunk: ' || v_lob_count || '/' || v_lob_total
            || ' (skipped ' || v_lob_errors || ')');
    END IF;

    shrink_log('SHRINK_DONE', 'SUCCESS',
        'Shrink complete. Tables=' || v_shrink_count || '/' || v_shrink_total
        || ', LOBs=' || v_lob_count || '/' || v_lob_total
        || ' (table skip ' || v_shrink_errors || ', LOB skip ' || v_lob_errors || ')');

    DBMS_OUTPUT.PUT_LINE('=== SHRINK SPACE complete ===');
END;
/
