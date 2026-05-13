-- ============================================================================
-- EPF Purge: Post-Reclaim Space Comparison
-- ============================================================================
-- Captures AFTER snapshot and prints before/after space comparison.
-- Run as the purge user (oppayments) after reclaim completes.
--
-- Optional define: depth (ALL | PAYMENTS | LOGS | BANK_STATEMENTS).
-- If not provided, the comparison shows ALL modules. The wrapper passes the
-- actual purge depth via DEFINE so the report only lists modules the user
-- actually purged.
-- ============================================================================
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET HEADING OFF FEEDBACK OFF
SET VERIFY OFF
DEFINE depth = ALL
DECLARE
    l_run_id RAW(16);
BEGIN
    SELECT run_id INTO l_run_id FROM (
        SELECT run_id FROM oppayments.epf_purge_log
        WHERE operation = 'RUN_END'
        ORDER BY log_timestamp DESC
    ) WHERE ROWNUM = 1;
    DELETE FROM oppayments.epf_purge_space_snapshot
    WHERE run_id = l_run_id AND snapshot_phase = 'AFTER';
    COMMIT;
    oppayments.epf_purge_pkg.capture_space_snapshot(l_run_id, 'AFTER');
    oppayments.epf_purge_pkg.print_space_comparison(l_run_id, '&depth');
END;
/
EXIT;
