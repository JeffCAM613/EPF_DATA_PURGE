-- ============================================================================
-- EPF Purge: Table Size Audit & Purge Coverage Check
-- ============================================================================
-- Shows ALL tables in the OPPAYMENTS tablespace with their sizes, sorted by
-- size descending. Flags each table as PURGED (included in the purge tool)
-- or NOT PURGED (not touched by the purge — potential candidates).
--
-- Run as OPPAYMENTS (uses user_segments) or as SYS (uses dba_segments).
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET PAGESIZE 1000
SET HEADING ON FEEDBACK OFF

COLUMN owner            FORMAT A15
COLUMN table_name       FORMAT A35
COLUMN size_mb          FORMAT 999,990.00
COLUMN row_count        FORMAT 999,999,999
COLUMN purge_status     FORMAT A12
COLUMN suggestion       FORMAT A50

-- Try DBA view first, fall back to user view
DECLARE
    v_use_dba BOOLEAN := TRUE;
    v_dummy   NUMBER;
BEGIN
    BEGIN
        EXECUTE IMMEDIATE 'SELECT 1 FROM dba_segments WHERE ROWNUM = 1' INTO v_dummy;
    EXCEPTION
        WHEN OTHERS THEN v_use_dba := FALSE;
    END;

    IF v_use_dba THEN
        DBMS_OUTPUT.PUT_LINE('Using dba_segments (full tablespace view)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Using user_segments (current user only)');
    END IF;
    DBMS_OUTPUT.PUT_LINE('');
END;
/

WITH purged_tables AS (
    -- All tables included in the purge tool (27 tables)
    SELECT table_name FROM (
        SELECT 'BULK_PAYMENT' AS table_name FROM DUAL UNION ALL
        SELECT 'BULK_PAYMENT_ADDITIONAL_INFO' FROM DUAL UNION ALL
        SELECT 'BULK_SIGNATURE' FROM DUAL UNION ALL
        SELECT 'MANDATORY_SIGNERS' FROM DUAL UNION ALL
        SELECT 'OIDC_REQUEST_TOKEN' FROM DUAL UNION ALL
        SELECT 'PAYMENT' FROM DUAL UNION ALL
        SELECT 'PAYMENT_ADDITIONAL_INFO' FROM DUAL UNION ALL
        SELECT 'PAYMENT_AUDIT' FROM DUAL UNION ALL
        SELECT 'IMPORT_AUDIT' FROM DUAL UNION ALL
        SELECT 'IMPORT_AUDIT_MESSAGES' FROM DUAL UNION ALL
        SELECT 'TRANSMISSION_EXECUTION' FROM DUAL UNION ALL
        SELECT 'TRANSMISSION_EXECUTION_AUDIT' FROM DUAL UNION ALL
        SELECT 'TRANSMISSION_EXCEPTION' FROM DUAL UNION ALL
        SELECT 'NOTIFICATION_EXECUTION' FROM DUAL UNION ALL
        SELECT 'APPROBATION_EXECUTION' FROM DUAL UNION ALL
        SELECT 'APPROBATION_EXECUTION_OPT' FROM DUAL UNION ALL
        SELECT 'WORKFLOW_EXECUTION' FROM DUAL UNION ALL
        SELECT 'WORKFLOW_EXECUTION_OPT' FROM DUAL UNION ALL
        SELECT 'BULKPAYMENT_EXCEPTION' FROM DUAL UNION ALL
        SELECT 'INVOICE' FROM DUAL UNION ALL
        SELECT 'INVOICE_ADDITIONAL_INFO' FROM DUAL UNION ALL
        SELECT 'FILE_INTEGRATION' FROM DUAL UNION ALL
        SELECT 'AUDIT_TRAIL' FROM DUAL UNION ALL
        SELECT 'AUDIT_ARCHIVE' FROM DUAL UNION ALL
        SELECT 'DIRECTORY_DISPATCHING' FROM DUAL UNION ALL
        SELECT 'FILE_DISPATCHING' FROM DUAL UNION ALL
        -- Also track the purge's own tables
        SELECT 'EPF_PURGE_LOG' FROM DUAL UNION ALL
        SELECT 'EPF_PURGE_SPACE_SNAPSHOT' FROM DUAL
    )
),
-- Segment sizes grouped by table (rolling up LOB + INDEX segments)
seg_sizes AS (
    SELECT s.segment_name,
           s.segment_type,
           ROUND(SUM(s.bytes) / 1048576, 2) AS size_mb
    FROM user_segments s
    GROUP BY s.segment_name, s.segment_type
),
-- Map LOB segments and INDEX segments to their parent tables
table_sizes AS (
    SELECT COALESCE(l.table_name, ix.table_name, s.segment_name) AS table_name,
           SUM(s.size_mb) AS total_mb
    FROM seg_sizes s
    LEFT JOIN user_lobs l
        ON l.segment_name = s.segment_name
    LEFT JOIN user_indexes ix
        ON ix.index_name = s.segment_name
        AND s.segment_type IN ('INDEX', 'LOBINDEX')
        AND l.table_name IS NULL  -- don't double-map LOB indexes
    GROUP BY COALESCE(l.table_name, ix.table_name, s.segment_name)
),
-- Get row counts (user_tables.num_rows from last ANALYZE/stats gather)
row_counts AS (
    SELECT table_name, NVL(num_rows, 0) AS num_rows
    FROM user_tables
)
SELECT ts.table_name,
       ts.total_mb AS size_mb,
       NVL(rc.num_rows, 0) AS row_count,
       CASE WHEN pt.table_name IS NOT NULL THEN 'PURGED'
            ELSE '** NOT **'
       END AS purge_status,
       CASE
           WHEN pt.table_name IS NOT NULL THEN ''
           WHEN ts.total_mb < 1 THEN 'Small — probably safe to ignore'
           WHEN UPPER(ts.table_name) LIKE '%LOG%'
             OR UPPER(ts.table_name) LIKE '%AUDIT%'
             OR UPPER(ts.table_name) LIKE '%HIST%'
             OR UPPER(ts.table_name) LIKE '%ARCHIVE%'
             OR UPPER(ts.table_name) LIKE '%TRACE%'
             OR UPPER(ts.table_name) LIKE '%EVENT%'
             THEN 'Likely purgeable — check date columns'
           WHEN UPPER(ts.table_name) LIKE '%TEMP%'
             OR UPPER(ts.table_name) LIKE '%TMP%'
             OR UPPER(ts.table_name) LIKE '%STAGING%'
             OR UPPER(ts.table_name) LIKE '%QUEUE%'
             THEN 'Temp/queue data — may be stale'
           WHEN ts.total_mb >= 100 THEN 'LARGE — investigate if purgeable'
           ELSE ''
       END AS suggestion
FROM table_sizes ts
LEFT JOIN purged_tables pt ON pt.table_name = ts.table_name
LEFT JOIN row_counts rc ON rc.table_name = ts.table_name
WHERE ts.total_mb >= 0.01
ORDER BY ts.total_mb DESC;

PROMPT
PROMPT ============================================================
PROMPT   Summary
PROMPT ============================================================

SELECT
    ROUND(SUM(CASE WHEN pt.table_name IS NOT NULL THEN ts.total_mb ELSE 0 END), 2) AS purged_mb,
    ROUND(SUM(CASE WHEN pt.table_name IS NULL THEN ts.total_mb ELSE 0 END), 2) AS not_purged_mb,
    ROUND(SUM(ts.total_mb), 2) AS total_mb,
    ROUND(SUM(CASE WHEN pt.table_name IS NOT NULL THEN ts.total_mb ELSE 0 END)
        / NULLIF(SUM(ts.total_mb), 0) * 100, 1) AS purge_coverage_pct
FROM (
    SELECT COALESCE(l.table_name, ix.table_name, s.segment_name) AS table_name,
           SUM(ROUND(s.bytes / 1048576, 2)) AS total_mb
    FROM user_segments s
    LEFT JOIN user_lobs l ON l.segment_name = s.segment_name
    LEFT JOIN user_indexes ix
        ON ix.index_name = s.segment_name
        AND s.segment_type IN ('INDEX', 'LOBINDEX')
        AND l.table_name IS NULL
    GROUP BY COALESCE(l.table_name, ix.table_name, s.segment_name)
) ts
LEFT JOIN (
    SELECT table_name FROM (
        SELECT 'BULK_PAYMENT' AS table_name FROM DUAL UNION ALL
        SELECT 'BULK_PAYMENT_ADDITIONAL_INFO' FROM DUAL UNION ALL
        SELECT 'BULK_SIGNATURE' FROM DUAL UNION ALL
        SELECT 'MANDATORY_SIGNERS' FROM DUAL UNION ALL
        SELECT 'OIDC_REQUEST_TOKEN' FROM DUAL UNION ALL
        SELECT 'PAYMENT' FROM DUAL UNION ALL
        SELECT 'PAYMENT_ADDITIONAL_INFO' FROM DUAL UNION ALL
        SELECT 'PAYMENT_AUDIT' FROM DUAL UNION ALL
        SELECT 'IMPORT_AUDIT' FROM DUAL UNION ALL
        SELECT 'IMPORT_AUDIT_MESSAGES' FROM DUAL UNION ALL
        SELECT 'TRANSMISSION_EXECUTION' FROM DUAL UNION ALL
        SELECT 'TRANSMISSION_EXECUTION_AUDIT' FROM DUAL UNION ALL
        SELECT 'TRANSMISSION_EXCEPTION' FROM DUAL UNION ALL
        SELECT 'NOTIFICATION_EXECUTION' FROM DUAL UNION ALL
        SELECT 'APPROBATION_EXECUTION' FROM DUAL UNION ALL
        SELECT 'APPROBATION_EXECUTION_OPT' FROM DUAL UNION ALL
        SELECT 'WORKFLOW_EXECUTION' FROM DUAL UNION ALL
        SELECT 'WORKFLOW_EXECUTION_OPT' FROM DUAL UNION ALL
        SELECT 'BULKPAYMENT_EXCEPTION' FROM DUAL UNION ALL
        SELECT 'INVOICE' FROM DUAL UNION ALL
        SELECT 'INVOICE_ADDITIONAL_INFO' FROM DUAL UNION ALL
        SELECT 'FILE_INTEGRATION' FROM DUAL UNION ALL
        SELECT 'AUDIT_TRAIL' FROM DUAL UNION ALL
        SELECT 'AUDIT_ARCHIVE' FROM DUAL UNION ALL
        SELECT 'DIRECTORY_DISPATCHING' FROM DUAL UNION ALL
        SELECT 'FILE_DISPATCHING' FROM DUAL UNION ALL
        SELECT 'EPF_PURGE_LOG' FROM DUAL UNION ALL
        SELECT 'EPF_PURGE_SPACE_SNAPSHOT' FROM DUAL
    )
) pt ON pt.table_name = ts.table_name
WHERE ts.total_mb >= 0.01;

PROMPT
PROMPT ============================================================
PROMPT   Module Breakdown (Purge Depth Coverage)
PROMPT ============================================================

COLUMN module          FORMAT A20
COLUMN module_size_mb  FORMAT 999,990.00
COLUMN module_pct      FORMAT 990.0

WITH purged_modules AS (
    SELECT table_name, module FROM (
        SELECT 'BULK_PAYMENT' AS table_name, 'PAYMENTS' AS module FROM DUAL UNION ALL
        SELECT 'BULK_PAYMENT_ADDITIONAL_INFO', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'BULK_SIGNATURE', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'MANDATORY_SIGNERS', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'OIDC_REQUEST_TOKEN', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'PAYMENT', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'PAYMENT_ADDITIONAL_INFO', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'PAYMENT_AUDIT', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'IMPORT_AUDIT', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'IMPORT_AUDIT_MESSAGES', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'TRANSMISSION_EXECUTION', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'TRANSMISSION_EXECUTION_AUDIT', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'TRANSMISSION_EXCEPTION', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'NOTIFICATION_EXECUTION', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'APPROBATION_EXECUTION', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'APPROBATION_EXECUTION_OPT', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'WORKFLOW_EXECUTION', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'WORKFLOW_EXECUTION_OPT', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'BULKPAYMENT_EXCEPTION', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'INVOICE', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'INVOICE_ADDITIONAL_INFO', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'FILE_INTEGRATION', 'PAYMENTS' FROM DUAL UNION ALL
        SELECT 'AUDIT_TRAIL', 'LOGS' FROM DUAL UNION ALL
        SELECT 'AUDIT_ARCHIVE', 'LOGS' FROM DUAL UNION ALL
        SELECT 'DIRECTORY_DISPATCHING', 'BANK_STATEMENTS' FROM DUAL UNION ALL
        SELECT 'FILE_DISPATCHING', 'BANK_STATEMENTS' FROM DUAL UNION ALL
        SELECT 'EPF_PURGE_LOG', 'INTERNAL' FROM DUAL UNION ALL
        SELECT 'EPF_PURGE_SPACE_SNAPSHOT', 'INTERNAL' FROM DUAL
    )
),
table_sizes AS (
    SELECT COALESCE(l.table_name, ix.table_name, s.segment_name) AS table_name,
           SUM(ROUND(s.bytes / 1048576, 2)) AS total_mb
    FROM user_segments s
    LEFT JOIN user_lobs l ON l.segment_name = s.segment_name
    LEFT JOIN user_indexes ix
        ON ix.index_name = s.segment_name
        AND s.segment_type IN ('INDEX', 'LOBINDEX')
        AND l.table_name IS NULL
    GROUP BY COALESCE(l.table_name, ix.table_name, s.segment_name)
)
SELECT NVL(pm.module, '** NOT PURGED **') AS module,
       ROUND(SUM(ts.total_mb), 2) AS module_size_mb,
       ROUND(SUM(ts.total_mb)
           / NULLIF((SELECT SUM(total_mb) FROM table_sizes WHERE total_mb >= 0.01), 0) * 100, 1) AS module_pct
FROM table_sizes ts
LEFT JOIN purged_modules pm ON pm.table_name = ts.table_name
WHERE ts.total_mb >= 0.01
GROUP BY pm.module
ORDER BY module_size_mb DESC;

EXIT;
