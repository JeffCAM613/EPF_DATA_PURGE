-- ============================================================================
-- EPF Data Purge - Diagnostic & Planning Queries
-- ============================================================================
-- Sections 1-4, 6-7: Run as OPPAYMENTS (or a user with SELECT on OPPAYMENTS)
-- Section 5 (DB Health): Requires SYSDBA or DBA role (v$ views)
-- ============================================================================

SET LINESIZE 200
SET PAGESIZE 100
SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK ON

--
-- ============================================================
--   1. ROW COUNTS PER MONTH (all purgeable tables combined)
-- ============================================================
--

SELECT TO_CHAR(month_start, 'YYYY-MM') AS month,
       SUM(cnt) AS total_rows
FROM (
    -- bulk_payment by value_date
    SELECT TRUNC(value_date, 'MM') AS month_start, COUNT(*) AS cnt
    FROM oppayments.bulk_payment GROUP BY TRUNC(value_date, 'MM')
    UNION ALL
    -- payment by bulk_payment.value_date
    SELECT TRUNC(bp.value_date, 'MM'), COUNT(*)
    FROM oppayments.payment p
    JOIN oppayments.bulk_payment bp ON p.bulk_payment_id = bp.bulk_payment_id
    GROUP BY TRUNC(bp.value_date, 'MM')
    UNION ALL
    -- file_integration by integration_date
    SELECT TRUNC(integration_date, 'MM'), COUNT(*)
    FROM oppayments.file_integration GROUP BY TRUNC(integration_date, 'MM')
    UNION ALL
    -- audit_trail by audit_timestamp
    SELECT TRUNC(audit_timestamp, 'MM'), COUNT(*)
    FROM oppayments.audit_trail GROUP BY TRUNC(audit_timestamp, 'MM')
    UNION ALL
    -- spec_trt_log by dtlog
    SELECT TRUNC(dtlog, 'MM'), COUNT(*)
    FROM op.spec_trt_log GROUP BY TRUNC(dtlog, 'MM')
    UNION ALL
    -- file_dispatching by date_reception
    SELECT TRUNC(date_reception, 'MM'), COUNT(*)
    FROM oppayments.file_dispatching GROUP BY TRUNC(date_reception, 'MM')
)
GROUP BY TO_CHAR(month_start, 'YYYY-MM')
ORDER BY 1;


--
-- ============================================================
--   2. ROW COUNTS PER TABLE (current totals)
-- ============================================================
--

SELECT table_name, TO_CHAR(num_rows, '999,999,999,999') AS est_rows,
       TO_CHAR(last_analyzed, 'YYYY-MM-DD HH24:MI') AS stats_date
FROM all_tables
WHERE owner = 'OPPAYMENTS'
  AND table_name IN (
    'BULK_PAYMENT', 'BULK_PAYMENT_ADDITIONAL_INFO', 'PAYMENT',
    'PAYMENT_ADDITIONAL_INFO', 'PAYMENT_AUDIT', 'IMPORT_AUDIT',
    'TRANSMISSION_EXECUTION', 'TRANSMISSION_EXECUTION_AUDIT',
    'TRANSMISSION_EXCEPTION', 'NOTIFICATION_EXECUTION',
    'WORKFLOW_EXECUTION', 'APPROBATION_EXECUTION',
    'FILE_INTEGRATION', 'AUDIT_TRAIL', 'AUDIT_ARCHIVE',
    'FILE_DISPATCHING', 'DIRECTORY_DISPATCHING',
    'PAYMENT_CURRENCY_EXCHANGE', 'PAYMENT_FEE',
    'DEBIT_MANDATE', 'PAYMENT_BANK_RESPONSE')
ORDER BY num_rows DESC NULLS LAST;


--
-- ============================================================
--   3. SEGMENT SIZES (tables + indexes + LOBs)
-- ============================================================
--

SELECT NVL(l.table_name, s.segment_name) AS table_name,
       s.segment_type,
       ROUND(SUM(s.bytes)/1024/1024, 1) AS size_mb
FROM dba_segments s
LEFT JOIN dba_lobs l ON s.owner = l.owner
  AND s.segment_name = l.segment_name
  AND s.segment_type = 'LOBSEGMENT'
WHERE s.owner = 'OPPAYMENTS'
GROUP BY NVL(l.table_name, s.segment_name), s.segment_type
HAVING SUM(s.bytes)/1024/1024 > 1
ORDER BY SUM(s.bytes) DESC;


--
-- ============================================================
--   4. ESTIMATED PURGE VOLUME (rows eligible per retention)
-- ============================================================
--

SELECT 'BULK_PAYMENT (90d)' AS scope,
       COUNT(*) AS eligible_rows
FROM oppayments.bulk_payment
WHERE value_date < TRUNC(SYSDATE) - 90
UNION ALL
SELECT 'PAYMENT (90d)',
       COUNT(*)
FROM oppayments.payment p
JOIN oppayments.bulk_payment bp ON p.bulk_payment_id = bp.bulk_payment_id
WHERE bp.value_date < TRUNC(SYSDATE) - 90
UNION ALL
SELECT 'AUDIT_TRAIL (90d)',
       COUNT(*)
FROM oppayments.audit_trail
WHERE audit_timestamp < TRUNC(SYSDATE) - 90
UNION ALL
SELECT 'SPEC_TRT_LOG (90d)',
       COUNT(*)
FROM op.spec_trt_log
WHERE dtlog < TRUNC(SYSDATE) - 90
UNION ALL
SELECT 'FILE_DISPATCHING (90d)',
       COUNT(*)
FROM oppayments.file_dispatching
WHERE date_reception < TRUNC(SYSDATE) - 90;


--
-- ============================================================
--   5. DATABASE HEALTH CHECK  *** REQUIRES SYSDBA / DBA ROLE ***
--      (errors below are expected if connected as OPPAYMENTS)
-- ============================================================
--

-- --- Redo Log Configuration ---
BEGIN
    FOR r IN (SELECT group#, ROUND(bytes/1024/1024) AS size_mb, status, members FROM v$log ORDER BY group#) LOOP
        DBMS_OUTPUT.PUT_LINE('  Group ' || r.group# || ': ' || r.size_mb || ' MB  [' || r.status || ']  members=' || r.members);
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('  (skipped - requires SYSDBA: ' || SQLERRM || ')');
END;
/

-- --- Redo Log Switch Waits ---
BEGIN
    FOR r IN (SELECT event, total_waits, ROUND(time_waited_micro/1e6, 1) AS wait_sec
              FROM v$system_event WHERE event LIKE 'log file switch%') LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || RPAD(r.event, 45) || '  waits=' || r.total_waits || '  wait_sec=' || r.wait_sec);
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('  (skipped - requires SYSDBA: ' || SQLERRM || ')');
END;
/

-- --- Buffer Cache Hit Ratio ---
BEGIN
    FOR r IN (
        SELECT ROUND(
            1 - (
                SUM(CASE WHEN name = 'physical reads' THEN value END) /
                NULLIF(SUM(CASE WHEN name IN ('db block gets','consistent gets') THEN value END), 0)
            ), 4) * 100 AS hit_ratio_pct
        FROM v$sysstat
        WHERE name IN ('db block gets', 'consistent gets', 'physical reads')
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  Buffer cache hit ratio: ' || r.hit_ratio_pct || '%');
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('  (skipped - requires SYSDBA: ' || SQLERRM || ')');
END;
/

-- --- SGA Configuration ---
BEGIN
    FOR r IN (SELECT name, ROUND(value/1024/1024) AS value_mb FROM v$sga ORDER BY value DESC) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || RPAD(r.name, 30) || r.value_mb || ' MB');
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('  (skipped - requires SYSDBA: ' || SQLERRM || ')');
END;
/

-- --- Undo Tablespace ---
BEGIN
    FOR r IN (
        SELECT t.tablespace_name,
               ROUND(SUM(f.bytes)/1024/1024) AS size_mb,
               f.autoextensible
        FROM dba_data_files f
        JOIN dba_tablespaces t ON f.tablespace_name = t.tablespace_name
        WHERE t.contents = 'UNDO'
        GROUP BY t.tablespace_name, f.autoextensible
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || r.tablespace_name || ': ' || r.size_mb || ' MB  autoextend=' || r.autoextensible);
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('  (skipped - requires DBA views: ' || SQLERRM || ')');
END;
/

-- --- Active Sessions on OPPAYMENTS ---
BEGIN
    FOR r IN (
        SELECT sid, serial#, status, event,
               ROUND(last_call_et/60, 1) AS idle_min,
               sql_id, module, action
        FROM v$session WHERE username = 'OPPAYMENTS'
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  SID=' || r.sid || ',' || r."SERIAL#" ||
            '  ' || r.status || '  event=' || r.event ||
            '  idle=' || r.idle_min || 'min  sql=' || r.sql_id);
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('  (skipped - requires SYSDBA: ' || SQLERRM || ')');
END;
/

-- --- Blocking Locks ---
BEGIN
    FOR r IN (
        SELECT l.sid AS blocked_sid, l.type, l.id1, l.id2, s.username, s.event
        FROM v$lock l JOIN v$session s ON l.sid = s.sid
        WHERE l.request > 0
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  BLOCKED SID=' || r.blocked_sid ||
            '  type=' || r.type || '  user=' || r.username || '  event=' || r.event);
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('  (skipped - requires SYSDBA: ' || SQLERRM || ')');
END;
/


--
-- ============================================================
--   6. PURGE HISTORY (last 10 runs)
-- ============================================================
--

SELECT run_id,
       MIN(log_timestamp) AS started,
       MAX(log_timestamp) AS finished,
       ROUND(SUM(NVL(elapsed_seconds, 0))/60, 1) AS total_min,
       SUM(CASE WHEN operation = 'DELETE' THEN rows_affected ELSE 0 END) AS rows_deleted,
       SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS errors
FROM oppayments.epf_purge_log
WHERE run_id IN (
    SELECT run_id FROM (
        SELECT DISTINCT run_id
        FROM oppayments.epf_purge_log
        WHERE operation = 'RUN_START'
        ORDER BY run_id DESC
    ) WHERE ROWNUM <= 10
)
GROUP BY run_id
ORDER BY MIN(log_timestamp) DESC;


--
-- ============================================================
--   7. INDEX HEALTH (unusable / invalid indexes)
-- ============================================================
--

SELECT index_name, table_name, status, last_analyzed
FROM all_indexes
WHERE owner = 'OPPAYMENTS'
  AND status NOT IN ('VALID', 'N/A')
ORDER BY table_name, index_name;

-- --- Stale Statistics (tables not analyzed in 30+ days) ---
SELECT table_name, num_rows,
       TO_CHAR(last_analyzed, 'YYYY-MM-DD') AS last_analyzed,
       ROUND(SYSDATE - last_analyzed) AS days_stale
FROM all_tables
WHERE owner = 'OPPAYMENTS'
  AND (last_analyzed IS NULL OR last_analyzed < SYSDATE - 30)
ORDER BY num_rows DESC NULLS LAST;
