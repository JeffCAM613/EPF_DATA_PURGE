-- ============================================================================
-- EPF Data Purge - Drop Package Script
-- ============================================================================
-- Removes the epf_purge_pkg package from the oppayments schema.
-- The epf_purge_log table is intentionally NOT dropped so audit history
-- is preserved after the package is removed.
--
-- Run this if you want to clean up after a purge execution, or if you
-- need to redeploy the package from scratch.
-- ============================================================================

BEGIN
    EXECUTE IMMEDIATE 'DROP PACKAGE oppayments.epf_purge_pkg';
    DBMS_OUTPUT.PUT_LINE('Package oppayments.epf_purge_pkg dropped successfully.');
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE = -4043 THEN
            DBMS_OUTPUT.PUT_LINE('Package oppayments.epf_purge_pkg does not exist. Nothing to drop.');
        ELSE
            DBMS_OUTPUT.PUT_LINE('Error dropping package: ' || SQLERRM);
            RAISE;
        END IF;
END;
/
