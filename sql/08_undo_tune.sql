-- ============================================================================
-- EPF Purge: UNDO Tuning for Bulk Deletes
-- ============================================================================
-- Lowers undo_retention to 60s and caps undo datafile autoextend to 8G
-- to prevent unbounded undo tablespace growth during long-running purges.
-- Run as SYS / SYSDBA before purge; restore with 09_undo_restore.sql after.
-- ============================================================================
SET SERVEROUTPUT ON SIZE UNLIMITED
SET HEADING OFF FEEDBACK OFF VERIFY OFF
DECLARE
    v_ret NUMBER;
BEGIN
    SELECT value INTO v_ret FROM v$parameter WHERE name = 'undo_retention';
    DBMS_OUTPUT.PUT_LINE('UNDO_RETENTION_ORIGINAL=' || v_ret);
    IF v_ret > 60 THEN
        EXECUTE IMMEDIATE 'ALTER SYSTEM SET undo_retention = 60';
        DBMS_OUTPUT.PUT_LINE('Lowered undo_retention from ' || v_ret || 's to 60s');
    ELSE
        DBMS_OUTPUT.PUT_LINE('undo_retention already low: ' || v_ret || 's');
    END IF;
    FOR f IN (SELECT file_name, maxbytes
              FROM dba_data_files
              WHERE tablespace_name = (SELECT value FROM v$parameter WHERE name = 'undo_tablespace')
                AND (autoextensible = 'YES' AND (maxbytes = 0 OR maxbytes > 8589934592)))
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER DATABASE DATAFILE ''' || f.file_name || ''' AUTOEXTEND ON MAXSIZE 8G';
            DBMS_OUTPUT.PUT_LINE('Capped autoextend: ' || f.file_name || ' maxsize=8G');
        EXCEPTION WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Could not cap ' || f.file_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/
EXIT;
