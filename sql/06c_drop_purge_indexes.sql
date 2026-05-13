-- ============================================================================
-- EPF Data Purge - Drop Temporary FK Indexes
-- ============================================================================
-- Drops only the EPF_TMP_* indexes created by 06b_create_purge_indexes.sql.
-- Safe to run if none exist (no errors, just skips).
-- Run as OPPAYMENTS after the purge completes.
-- ============================================================================

SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK OFF

DECLARE
    l_dropped NUMBER := 0;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Dropping temporary purge indexes (EPF_TMP_*)...');

    FOR idx IN (
        SELECT index_name
        FROM user_indexes
        WHERE index_name LIKE 'EPF\_TMP\_%' ESCAPE '\'
        ORDER BY index_name
    ) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP INDEX oppayments.' || idx.index_name;
            l_dropped := l_dropped + 1;
            DBMS_OUTPUT.PUT_LINE('  Dropped: ' || idx.index_name);
        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('  WARNING: Could not drop ' || idx.index_name
                    || ': ' || SQLERRM);
        END;
    END LOOP;

    IF l_dropped = 0 THEN
        DBMS_OUTPUT.PUT_LINE('  No temporary purge indexes found.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Dropped ' || l_dropped || ' temporary index(es).');
    END IF;
END;
/
EXIT;
