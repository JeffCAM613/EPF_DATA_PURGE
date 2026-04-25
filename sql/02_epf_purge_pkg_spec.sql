-- ============================================================================
-- EPF Data Purge - PL/SQL Package Specification
-- ============================================================================
-- Package: oppayments.epf_purge_pkg
--
-- Provides configurable, batched data purge operations for the ePF
-- (electronic Payment Factory) system. Supports selective purge by module,
-- complete audit logging, dry-run mode, and optional space reclamation.
--
-- Usage:
--   BEGIN
--       oppayments.epf_purge_pkg.run_purge(
--           p_retention_days => 90,
--           p_purge_depth    => 'ALL',
--           p_batch_size     => 5000,
--           p_dry_run        => FALSE
--       );
--
-- Disk-level space reclamation is handled outside this package by the
-- epf_tablespace_reclaim tool (export/import/recreate-as-BIGFILE).
--   END;
-- ============================================================================

CREATE OR REPLACE PACKAGE oppayments.epf_purge_pkg
AUTHID CURRENT_USER
AS
    -- ========================================================================
    -- Constants: purge depth options
    -- ========================================================================
    C_DEPTH_ALL              CONSTANT VARCHAR2(30) := 'ALL';
    C_DEPTH_PAYMENTS         CONSTANT VARCHAR2(30) := 'PAYMENTS';
    C_DEPTH_LOGS             CONSTANT VARCHAR2(30) := 'LOGS';
    C_DEPTH_BANK_STATEMENTS  CONSTANT VARCHAR2(30) := 'BANK_STATEMENTS';

    -- ========================================================================
    -- Constants: defaults
    -- ========================================================================
    C_DEFAULT_RETENTION_DAYS CONSTANT NUMBER := 30;
    C_DEFAULT_BATCH_SIZE     CONSTANT NUMBER := 5000;

    -- ========================================================================
    -- Collection types (used for BULK COLLECT operations)
    -- ========================================================================
    TYPE t_id_table IS TABLE OF NUMBER INDEX BY PLS_INTEGER;

    -- ========================================================================
    -- Master entry point
    -- ========================================================================
    -- Orchestrates the full purge process:
    --   1. Ensures the log table exists
    --   2. Calls module procedures based on p_purge_depth
    --   3. Prints a summary of the run
    --
    -- Parameters:
    --   p_retention_days  - Data older than this many days is purged (default 30)
    --   p_purge_depth     - Which modules to purge: ALL, PAYMENTS, LOGS,
    --                       or BANK_STATEMENTS (default ALL)
    --   p_batch_size      - Number of parent records per batch commit (default 5000)
    --   p_dry_run         - If TRUE, counts rows but does not delete anything
    PROCEDURE run_purge(
        p_retention_days   IN NUMBER   DEFAULT C_DEFAULT_RETENTION_DAYS,
        p_purge_depth      IN VARCHAR2 DEFAULT C_DEPTH_ALL,
        p_batch_size       IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run          IN BOOLEAN  DEFAULT FALSE
    );

    -- ========================================================================
    -- Module procedures (can also be called individually)
    -- ========================================================================

    -- Purges bulk payments and all dependent child records (13 tables).
    -- Tables affected (in delete order):
    --   bulk_payment_additional_info, payment_audit (by bulk_payment_id),
    --   import_audit, transmission_execution_audit, transmission_execution,
    --   transmission_exception, notification_execution,
    --   approbation_execution (via workflow_execution -> payment),
    --   workflow_execution (via payment), payment_audit (by payment_id),
    --   payment_additional_info (via payment), payment, bulk_payment
    PROCEDURE purge_bulk_payments(
        p_run_id           IN RAW,
        p_cutoff_date      IN DATE,
        p_batch_size       IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run          IN BOOLEAN  DEFAULT FALSE
    );

    -- Purges file integration records (fast-import payment files).
    -- Tables affected: file_integration
    PROCEDURE purge_file_integrations(
        p_run_id           IN RAW,
        p_cutoff_date      IN DATE,
        p_batch_size       IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run          IN BOOLEAN  DEFAULT FALSE
    );

    -- Purges functional audit trail and archive records.
    -- Tables affected: audit_archive, audit_trail
    PROCEDURE purge_audit_logs(
        p_run_id           IN RAW,
        p_cutoff_date      IN DATE,
        p_batch_size       IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run          IN BOOLEAN  DEFAULT FALSE
    );

    -- Purges technical log records.
    -- Tables affected: op.spec_trt_log
    PROCEDURE purge_tech_logs(
        p_run_id           IN RAW,
        p_cutoff_date      IN DATE,
        p_batch_size       IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run          IN BOOLEAN  DEFAULT FALSE
    );

    -- Purges bank statement dispatching records.
    -- Tables affected: directory_dispatching, file_dispatching
    PROCEDURE purge_bank_statements(
        p_run_id           IN RAW,
        p_cutoff_date      IN DATE,
        p_batch_size       IN NUMBER   DEFAULT C_DEFAULT_BATCH_SIZE,
        p_dry_run          IN BOOLEAN  DEFAULT FALSE
    );

    -- ========================================================================
    -- Utilities
    -- ========================================================================

    -- Ensures the epf_purge_log and epf_purge_space_snapshot tables exist (idempotent).
    PROCEDURE ensure_log_table;

    -- Prints a formatted summary of a purge run to DBMS_OUTPUT.
    PROCEDURE print_run_summary(p_run_id IN RAW);

    -- ========================================================================
    -- Space usage snapshots
    -- ========================================================================

    -- Captures current segment sizes for all oppayments objects (tables,
    -- indexes, LOBs) into epf_purge_space_snapshot. Call with 'BEFORE' before
    -- purge and 'AFTER' after purge to enable comparison.
    -- Also captures LOB segments and resolves them to their parent table.
    PROCEDURE capture_space_snapshot(
        p_run_id         IN RAW,
        p_snapshot_phase IN VARCHAR2  -- 'BEFORE' or 'AFTER'
    );

    -- Prints a before/after comparison of segment sizes for a given run,
    -- depth-aware: only the modules covered by p_depth are shown, all 27
    -- purged tables are listed (sorted by module then name), per-module
    -- subtotals plus PURGED TABLES TOTAL and TABLESPACE TOTAL.
    --
    -- Parameters:
    --   p_run_id - The run to report on
    --   p_depth  - ALL, PAYMENTS, LOGS, or BANK_STATEMENTS (default ALL)
    PROCEDURE print_space_comparison(
        p_run_id IN RAW,
        p_depth  IN VARCHAR2 DEFAULT C_DEPTH_ALL
    );

END epf_purge_pkg;
/
