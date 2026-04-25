-- ============================================================================
-- EPF Purge: Module Data Sizes
-- ============================================================================
-- Shows current data sizes grouped by purge module (PAYMENTS, LOGS,
-- BANK_STATEMENTS) to help choose the appropriate purge depth.
--
-- Run as OPPAYMENTS user. Uses user_segments (no DBA grants needed).
-- Note: LOGS total excludes op.spec_trt_log (different schema).
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET HEADING OFF FEEDBACK OFF VERIFY OFF

DECLARE
    TYPE t_mod IS TABLE OF VARCHAR2(1) INDEX BY VARCHAR2(50);
    m t_mod;
    v_total NUMBER := 0;
    v_pay   NUMBER := 0;
    v_log   NUMBER := 0;
    v_bst   NUMBER := 0;
BEGIN
    -- PAYMENTS depth (22 tables)
    m('BULK_PAYMENT')                    := 'P';
    m('BULK_PAYMENT_ADDITIONAL_INFO')    := 'P';
    m('BULK_SIGNATURE')                  := 'P';
    m('MANDATORY_SIGNERS')               := 'P';
    m('OIDC_REQUEST_TOKEN')              := 'P';
    m('PAYMENT')                         := 'P';
    m('PAYMENT_ADDITIONAL_INFO')         := 'P';
    m('PAYMENT_AUDIT')                   := 'P';
    m('IMPORT_AUDIT')                    := 'P';
    m('IMPORT_AUDIT_MESSAGES')           := 'P';
    m('TRANSMISSION_EXECUTION')          := 'P';
    m('TRANSMISSION_EXECUTION_AUDIT')    := 'P';
    m('TRANSMISSION_EXCEPTION')          := 'P';
    m('NOTIFICATION_EXECUTION')          := 'P';
    m('APPROBATION_EXECUTION')           := 'P';
    m('APPROBATION_EXECUTION_OPT')       := 'P';
    m('WORKFLOW_EXECUTION')              := 'P';
    m('WORKFLOW_EXECUTION_OPT')          := 'P';
    m('BULKPAYMENT_EXCEPTION')           := 'P';
    m('INVOICE')                         := 'P';
    m('INVOICE_ADDITIONAL_INFO')         := 'P';
    m('FILE_INTEGRATION')                := 'P';
    -- LOGS depth (2 oppayments tables + op.spec_trt_log not visible here)
    m('AUDIT_TRAIL')                     := 'L';
    m('AUDIT_ARCHIVE')                   := 'L';
    -- BANK_STATEMENTS depth (2 tables)
    m('DIRECTORY_DISPATCHING')           := 'B';
    m('FILE_DISPATCHING')                := 'B';

    FOR r IN (
        SELECT COALESCE(lb.table_name, ix.table_name, s.segment_name) AS tn,
               SUM(s.bytes) AS byt
        FROM user_segments s
        LEFT JOIN user_lobs lb ON lb.segment_name = s.segment_name
        LEFT JOIN user_indexes ix ON ix.index_name = s.segment_name
            AND s.segment_type IN ('INDEX', 'LOBINDEX')
            AND lb.table_name IS NULL
        GROUP BY COALESCE(lb.table_name, ix.table_name, s.segment_name)
    ) LOOP
        v_total := v_total + r.byt;
        IF m.EXISTS(r.tn) THEN
            CASE m(r.tn)
                WHEN 'P' THEN v_pay := v_pay + r.byt;
                WHEN 'L' THEN v_log := v_log + r.byt;
                WHEN 'B' THEN v_bst := v_bst + r.byt;
            END CASE;
        END IF;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('    Current data sizes by purge module:');
    DBMS_OUTPUT.PUT_LINE('    -------------------------------------------');
    IF v_total > 0 THEN
        DBMS_OUTPUT.PUT_LINE('    PAYMENTS ........... '
            || LPAD(TO_CHAR(ROUND(v_pay/1073741824, 2), '9990.00'), 8) || ' GB  ('
            || LPAD(TO_CHAR(ROUND(v_pay/v_total*100, 1), '990.0'), 5) || '%)');
        DBMS_OUTPUT.PUT_LINE('    LOGS ............... '
            || LPAD(TO_CHAR(ROUND(v_log/1073741824, 2), '9990.00'), 8) || ' GB  ('
            || LPAD(TO_CHAR(ROUND(v_log/v_total*100, 1), '990.0'), 5) || '%)');
        DBMS_OUTPUT.PUT_LINE('    BANK_STATEMENTS .... '
            || LPAD(TO_CHAR(ROUND(v_bst/1073741824, 2), '9990.00'), 8) || ' GB  ('
            || LPAD(TO_CHAR(ROUND(v_bst/v_total*100, 1), '990.0'), 5) || '%)');
        DBMS_OUTPUT.PUT_LINE('    Other (not purged) . '
            || LPAD(TO_CHAR(ROUND((v_total-v_pay-v_log-v_bst)/1073741824, 2), '9990.00'), 8) || ' GB  ('
            || LPAD(TO_CHAR(ROUND((v_total-v_pay-v_log-v_bst)/v_total*100, 1), '990.0'), 5) || '%)');
    ELSE
        DBMS_OUTPUT.PUT_LINE('    (no segment data found)');
    END IF;
    DBMS_OUTPUT.PUT_LINE('    -------------------------------------------');
    DBMS_OUTPUT.PUT_LINE('    TOTAL .............. '
        || LPAD(TO_CHAR(ROUND(v_total/1073741824, 2), '9990.00'), 8) || ' GB');
    DBMS_OUTPUT.PUT_LINE('    * LOGS excludes op.spec_trt_log (different schema)');
    DBMS_OUTPUT.PUT_LINE('');
END;
/

EXIT;
