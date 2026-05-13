-- ============================================================================
-- EPF Data Purge - Dump Full Run Log
-- ============================================================================
-- Writes EVERY epf_purge_log entry for the most recent run, in chronological
-- order, formatted to match the live monitor's output. The wrapper invokes
-- this at the very end of a run so the canonical log file always contains
-- the complete batch-level history -- even if the live monitor missed
-- entries due to wrapper / monitor file-write interleaving (the wrapper's
-- long-running sqlplus stream and the monitor's per-poll writes both append
-- to the same file; under load some monitor lines can be overwritten before
-- being flushed). This dump is the source of truth.
--
-- No bind variables; auto-detects the run via "latest RUN_END" so it works
-- in any post-run context (success, error, reclaim-only mode where there is
-- no RUN_END but there is a RECLAIM_END).
--
-- Usage: invoked from epf_purge.bat / epf_purge.sh after all phases finish.
--   sqlplus -S oppayments/...@tns @sql/13_dump_run_log.sql
-- ============================================================================
SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK OFF
SET HEADING OFF
SET ECHO OFF
SET LINESIZE 500
SET PAGESIZE 0
SET TRIMSPOOL ON

DECLARE
    l_run_id   RAW(16);
    l_total    NUMBER := 0;
    l_started  TIMESTAMP;
    l_ended    TIMESTAMP;
BEGIN
    -- Find the most recent run. Prefer the one with a RUN_END (full purge);
    -- fall back to the latest run by RUN_START (reclaim-only or in-progress).
    BEGIN
        SELECT run_id INTO l_run_id FROM (
            SELECT run_id FROM oppayments.epf_purge_log
            WHERE operation IN ('RUN_END', 'RECLAIM_END')
            ORDER BY log_timestamp DESC
        ) WHERE ROWNUM = 1;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            BEGIN
                SELECT run_id INTO l_run_id FROM (
                    SELECT run_id FROM oppayments.epf_purge_log
                    WHERE operation = 'RUN_START'
                    ORDER BY log_timestamp DESC
                ) WHERE ROWNUM = 1;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    DBMS_OUTPUT.PUT_LINE('[INFO] No epf_purge_log entries found -- nothing to dump.');
                    RETURN;
            END;
    END;

    SELECT MIN(log_timestamp), MAX(log_timestamp), COUNT(*)
      INTO l_started, l_ended, l_total
      FROM oppayments.epf_purge_log
     WHERE run_id = l_run_id;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('================================================================================');
    DBMS_OUTPUT.PUT_LINE('  COMPLETE RUN LOG (replay from epf_purge_log)');
    DBMS_OUTPUT.PUT_LINE('  Run ID:    ' || RAWTOHEX(l_run_id));
    DBMS_OUTPUT.PUT_LINE('  Started:   ' || TO_CHAR(l_started, 'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('  Finished:  ' || TO_CHAR(l_ended,   'YYYY-MM-DD HH24:MI:SS'));
    DBMS_OUTPUT.PUT_LINE('  Entries:   ' || l_total);
    DBMS_OUTPUT.PUT_LINE('--------------------------------------------------------------------------------');
    DBMS_OUTPUT.PUT_LINE('  This block is the canonical record. Lines higher up may be missing or');
    DBMS_OUTPUT.PUT_LINE('  truncated due to wrapper/monitor concurrent file appends; this dump is');
    DBMS_OUTPUT.PUT_LINE('  written at end-of-run with no concurrency.');
    DBMS_OUTPUT.PUT_LINE('================================================================================');

    FOR rec IN (
        SELECT TO_CHAR(log_timestamp, 'HH24:MI:SS')                AS ts,
               RPAD(NVL(module, '-'), 16)                          AS modn,
               RPAD(NVL(operation, '-'), 16)                       AS opn,
               NVL(TO_CHAR(batch_number), '')                      AS bn,
               NVL(TO_CHAR(rows_affected), '')                     AS ra,
               status,
               table_name,
               REPLACE(REPLACE(NVL(message, ''), CHR(10), ' '), CHR(13), '') AS msg,
               NVL(TO_CHAR(ROUND(elapsed_seconds, 1)), '')         AS el
          FROM oppayments.epf_purge_log
         WHERE run_id = l_run_id
         ORDER BY log_id
    ) LOOP
        -- Compose one line per log entry. Format mirrors the monitor's:
        --   [hh:mm:ss] MODULE       OPERATION       [batch=N rows=M] MESSAGE  (elapsed s)
        DBMS_OUTPUT.PUT_LINE(
            '[' || rec.ts || '] '
            || rec.modn
            || rec.opn
            || CASE WHEN rec.bn IS NOT NULL AND rec.bn != ''
                    THEN '[batch=' || LPAD(rec.bn, 5) || '] ' ELSE '' END
            || CASE WHEN rec.ra IS NOT NULL AND rec.ra != ''
                         AND rec.ra != '0'
                    THEN '[rows=' || LPAD(rec.ra, 10) || '] ' ELSE '' END
            || CASE WHEN rec.table_name IS NOT NULL
                    THEN rec.table_name || '  ' ELSE '' END
            || rec.msg
            || CASE WHEN rec.el IS NOT NULL AND rec.el != ''
                    THEN '  (' || rec.el || 's)' ELSE '' END
            || CASE WHEN rec.status = 'ERROR'   THEN '  *ERROR*'
                    WHEN rec.status = 'WARNING' THEN '  *WARN*'
                    ELSE '' END
        );
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('================================================================================');
    DBMS_OUTPUT.PUT_LINE('  END OF RUN LOG (' || l_total || ' entries)');
    DBMS_OUTPUT.PUT_LINE('================================================================================');

    -- ========================================================================
    -- Per-module purge duration summary
    -- ========================================================================
    -- Computed from MIN/MAX log_timestamp per module (the package logs both
    -- a starting INFO row and a final SUCCESS row for every module, so the
    -- spread captures end-to-end module wallclock).
    DECLARE
        l_have_modules BOOLEAN := FALSE;
    BEGIN
        FOR rec IN (
            SELECT module,
                   SUM(CASE WHEN status = 'SUCCESS' THEN rows_affected ELSE 0 END) AS rows_total,
                   SUM(CASE WHEN status = 'ERROR'   THEN 1 ELSE 0 END) AS errs,
                   ROUND((CAST(MAX(log_timestamp) AS DATE)
                        - CAST(MIN(log_timestamp) AS DATE)) * 86400, 1) AS dur_s
              FROM oppayments.epf_purge_log
             WHERE run_id = l_run_id
               AND module IN ('PAYMENTS','AUDIT_LOGS','TECH_LOGS',
                              'BANK_STATEMENTS','FILE_INTEGRATION')
               AND operation = 'DELETE'
             GROUP BY module
             ORDER BY module
        ) LOOP
            IF NOT l_have_modules THEN
                DBMS_OUTPUT.PUT_LINE('');
                DBMS_OUTPUT.PUT_LINE('  Per-module purge duration:');
                l_have_modules := TRUE;
            END IF;
            DBMS_OUTPUT.PUT_LINE('    ' || RPAD(rec.module, 18)
                || 'rows='     || LPAD(NVL(TO_CHAR(rec.rows_total), '0'), 12)
                || '   errors='   || LPAD(TO_CHAR(rec.errs), 4)
                || '   duration=' || LPAD(NVL(TO_CHAR(rec.dur_s), '?'), 10) || ' s');
        END LOOP;
    END;

    -- ========================================================================
    -- Per-phase reclaim duration summary
    -- ========================================================================
    -- Pairs PHASE_START / PHASE_END rows by ordinal within this run. The
    -- reclaim script emits one start+end pair per major phase (Capture DDL,
    -- Drop FK, Drop PK/UK, Drop secondary indexes, SHRINK SPACE, MOVE
    -- tables, MOVE LOB segments, Resize data/index TS, Recreate indexes,
    -- Recreate PK/UK, Recreate FK, Final resize, Shrink UNDO/TEMP).
    DECLARE
        l_have_phases BOOLEAN := FALSE;
        l_total       NUMBER  := 0;
    BEGIN
        FOR rec IN (
            WITH s AS (
                SELECT message AS phase, log_timestamp AS ts,
                       ROW_NUMBER() OVER (ORDER BY log_id) AS rn
                  FROM oppayments.epf_purge_log
                 WHERE run_id = l_run_id
                   AND operation = 'PHASE_START'
            ),
            e AS (
                SELECT log_timestamp AS ts,
                       ROW_NUMBER() OVER (ORDER BY log_id) AS rn
                  FROM oppayments.epf_purge_log
                 WHERE run_id = l_run_id
                   AND operation = 'PHASE_END'
            )
            SELECT s.phase,
                   ROUND((CAST(e.ts AS DATE) - CAST(s.ts AS DATE)) * 86400, 1) AS dur_s
              FROM s JOIN e ON e.rn = s.rn
             ORDER BY s.rn
        ) LOOP
            IF NOT l_have_phases THEN
                DBMS_OUTPUT.PUT_LINE('');
                DBMS_OUTPUT.PUT_LINE('  Per-phase reclaim duration:');
                l_have_phases := TRUE;
            END IF;
            DBMS_OUTPUT.PUT_LINE('    ' || RPAD(rec.phase, 34)
                || LPAD(NVL(TO_CHAR(rec.dur_s), '?'), 9) || ' s');
            l_total := l_total + NVL(rec.dur_s, 0);
        END LOOP;
        IF l_have_phases THEN
            DBMS_OUTPUT.PUT_LINE('    ' || RPAD('TOTAL (sum of phases)', 34)
                || LPAD(TO_CHAR(ROUND(l_total, 1)), 9) || ' s');
        END IF;
    END;

    DBMS_OUTPUT.PUT_LINE('================================================================================');
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('[ERROR] dump_run_log failed: ' || SQLERRM);
END;
/

EXIT;
