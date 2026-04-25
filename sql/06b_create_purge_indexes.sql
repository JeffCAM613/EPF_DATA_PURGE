-- ============================================================================
-- EPF Data Purge - Temporary FK Indexes for Purge Performance
-- ============================================================================
-- The purge uses FORALL DELETE ... WHERE fk_column = :id. Without indexes on
-- FK columns, each row in the batch triggers a full table scan — making the
-- purge dramatically slower on large tables.
--
-- This script creates indexes ONLY where they are missing. Existing indexes
-- (including PK/unique) on the same column are detected and skipped.
--
-- Run as OPPAYMENTS before the purge. Drop after purge with 06c_drop_purge_indexes.sql.
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK OFF

DECLARE
    TYPE t_idx_def IS RECORD (
        table_name  VARCHAR2(128),
        column_name VARCHAR2(128)
    );
    TYPE t_idx_list IS TABLE OF t_idx_def INDEX BY PLS_INTEGER;

    l_indexes     t_idx_list;
    l_exists      NUMBER;
    l_created     NUMBER := 0;
    l_skipped     NUMBER := 0;
    l_idx_name    VARCHAR2(128);
BEGIN
    DBMS_OUTPUT.PUT_LINE('============================================================');
    DBMS_OUTPUT.PUT_LINE('  Creating temporary FK indexes for purge performance');
    DBMS_OUTPUT.PUT_LINE('============================================================');

    -- Define all FK columns used in DELETE WHERE clauses
    -- (bulk_payment_id family)
    l_indexes(1).table_name  := 'BULK_PAYMENT_ADDITIONAL_INFO'; l_indexes(1).column_name  := 'BULK_PAYMENT_ID';
    l_indexes(2).table_name  := 'BULK_SIGNATURE';               l_indexes(2).column_name  := 'BULK_PAYMENT_ID';
    l_indexes(3).table_name  := 'MANDATORY_SIGNERS';            l_indexes(3).column_name  := 'BULK_PAYMENT_ID';
    l_indexes(4).table_name  := 'OIDC_REQUEST_TOKEN';           l_indexes(4).column_name  := 'BULK_PAYMENT_ID';
    l_indexes(5).table_name  := 'PAYMENT_AUDIT';                l_indexes(5).column_name  := 'BULK_PAYMENT_ID';
    l_indexes(6).table_name  := 'TRANSMISSION_EXECUTION_AUDIT'; l_indexes(6).column_name  := 'BULK_PAYMENT_ID';
    l_indexes(7).table_name  := 'IMPORT_AUDIT';                 l_indexes(7).column_name  := 'BULK_PAYMENT_ID';
    l_indexes(8).table_name  := 'NOTIFICATION_EXECUTION';       l_indexes(8).column_name  := 'BULK_PAYMENT_ID';
    l_indexes(9).table_name  := 'TRANSMISSION_EXECUTION';       l_indexes(9).column_name  := 'BULK_PAYMENT_ID';
    l_indexes(10).table_name := 'TRANSMISSION_EXCEPTION';       l_indexes(10).column_name := 'BULK_PAYMENT_ID';
    l_indexes(11).table_name := 'WORKFLOW_EXECUTION_OPT';       l_indexes(11).column_name := 'BULK_PAYMENT_ID';
    l_indexes(12).table_name := 'PAYMENT';                      l_indexes(12).column_name := 'BULK_PAYMENT_ID';

    -- (payment_id family)
    l_indexes(13).table_name := 'PAYMENT_AUDIT';                l_indexes(13).column_name := 'PAYMENT_ID';
    l_indexes(14).table_name := 'WORKFLOW_EXECUTION';           l_indexes(14).column_name := 'PAYMENT_ID';
    l_indexes(15).table_name := 'BULKPAYMENT_EXCEPTION';        l_indexes(15).column_name := 'PAYMENT_ID';
    l_indexes(16).table_name := 'INVOICE';                      l_indexes(16).column_name := 'PAYMENT_ID';
    l_indexes(17).table_name := 'PAYMENT_ADDITIONAL_INFO';      l_indexes(17).column_name := 'PAYMENT_ID';

    -- (other FK columns used in subquery deletes)
    l_indexes(18).table_name := 'IMPORT_AUDIT_MESSAGES';        l_indexes(18).column_name := 'IMPORT_AUDIT_ID';
    l_indexes(19).table_name := 'APPROBATION_EXECUTION_OPT';    l_indexes(19).column_name := 'EXECUTION_ID';
    l_indexes(20).table_name := 'APPROBATION_EXECUTION';        l_indexes(20).column_name := 'EXECUTION_ID';

    -- (audit module)
    l_indexes(21).table_name := 'AUDIT_TRAIL';                  l_indexes(21).column_name := 'AUDIT_TIMESTAMP';

    -- (bank statements module)
    l_indexes(22).table_name := 'DIRECTORY_DISPATCHING';        l_indexes(22).column_name := 'FILE_DISPATCHING_ID';
    l_indexes(23).table_name := 'FILE_DISPATCHING';             l_indexes(23).column_name := 'DATE_RECEPTION';

    -- (file integration)
    l_indexes(24).table_name := 'FILE_INTEGRATION';             l_indexes(24).column_name := 'INTEGRATION_DATE';

    FOR i IN 1..l_indexes.COUNT LOOP
        -- Check if an index already exists with this column as the leading column
        SELECT COUNT(*) INTO l_exists
        FROM user_ind_columns
        WHERE table_name = l_indexes(i).table_name
          AND column_name = l_indexes(i).column_name
          AND column_position = 1;

        IF l_exists > 0 THEN
            l_skipped := l_skipped + 1;
            DBMS_OUTPUT.PUT_LINE('  [SKIP] ' || l_indexes(i).table_name
                || '(' || l_indexes(i).column_name || ') — index already exists');
        ELSE
            l_idx_name := 'EPF_TMP_' || SUBSTR(l_indexes(i).table_name, 1, 20) || '_' || i;
            BEGIN
                EXECUTE IMMEDIATE
                    'CREATE INDEX oppayments.' || l_idx_name
                    || ' ON oppayments.' || l_indexes(i).table_name
                    || ' (' || l_indexes(i).column_name || ')'
                    || ' NOLOGGING PARALLEL 4';
                -- Reset to normal after creation
                EXECUTE IMMEDIATE
                    'ALTER INDEX oppayments.' || l_idx_name || ' NOPARALLEL LOGGING';
                l_created := l_created + 1;
                DBMS_OUTPUT.PUT_LINE('  [OK]   ' || l_idx_name || ' on '
                    || l_indexes(i).table_name || '(' || l_indexes(i).column_name || ')');
            EXCEPTION
                WHEN OTHERS THEN
                    DBMS_OUTPUT.PUT_LINE('  [WARN] Could not create index on '
                        || l_indexes(i).table_name || '(' || l_indexes(i).column_name
                        || '): ' || SQLERRM);
            END;
        END IF;
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('');
    DBMS_OUTPUT.PUT_LINE('Summary: ' || l_created || ' indexes created, '
        || l_skipped || ' already existed');
    DBMS_OUTPUT.PUT_LINE('============================================================');
END;
/
EXIT;
