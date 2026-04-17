# Change Report: Old Scripts vs New Package

This document outlines every change made during the refactoring, organized by category. Use this for internal reporting or change management review.

---

## 1. Project Structure Changes

### Before
```
EPF_DATA_PURGE/
├── 1.PurgeRemisesDePaiements.sql
├── 2.PurgeTableLogsEtArchive.sql
└── 3.PurgeRelevesBancaires.sql
```
3 standalone anonymous PL/SQL blocks, no project organization, no documentation.

### After
```
EPF_DATA_PURGE/
├── README.md
├── .gitignore
├── sql/            (4 SQL scripts + legacy/ folder)
├── bin/            (purge scripts + tablespace reclaim scripts, .sh + .bat)
├── config/         (example config for automation)
├── logs/           (runtime logs, gitignored)
└── docs/           (space reclamation guide, this report)
```
Original scripts preserved in `sql/legacy/` for reference.

---

## 2. Language & Naming Changes

| Item | Before | After |
|------|--------|-------|
| File names | French (`PurgeRemisesDePaiements`, `PurgeTableLogsEtArchive`, `PurgeRelevesBancaires`) | English (`epf_purge_pkg_spec`, `epf_purge_pkg_body`, etc.) |
| Code comments | French (`Purge des remises des paiements par date`, `Purge des tables de logs et archive`) | English |
| SQL comments | French inline headers | English with structured header blocks |
| Database table/column names | French (`bulk_payment`, `audit_trail`, `file_dispatching`, etc.) | **Unchanged** — all French database identifiers are preserved exactly |
| Variable names | French/English mix (`dateSuppression`, `dateSuppressionTimeStamp`, `nBulkPaymentId`) | English (`p_cutoff_date`, `l_bp_ids`, `l_rows_deleted`) |

---

## 3. Architecture Changes

| Aspect | Before | After |
|--------|--------|-------|
| Code structure | 3 standalone anonymous PL/SQL blocks | 1 PL/SQL package (`epf_purge_pkg`) with spec + body |
| Execution model | Run SQL files directly in sqlplus | Wrapper script deploys package → executes → optionally drops |
| Entry point | Run each `.sql` file manually in order | Single `run_purge()` procedure orchestrates everything |
| Modularity | Monolithic blocks, no reuse | Separate procedures per module, callable individually or via orchestrator |
| Error handling | `WHEN OTHERS` logs to `op.SPEC_OUTILS.AddSpecLog` (external dependency) | Self-contained logging to `epf_purge_log` table using autonomous transactions |

---

## 4. Processing Strategy Changes

### Before: Row-by-Row (Slow-by-Slow)
```
FOR each bulk_payment_id IN cursor
    DELETE from table1 WHERE id = current_id;
    DELETE from table2 WHERE id = current_id;
    ... (13 DELETE statements)
    COMMIT;   -- commit every single row
END LOOP;
```
- **1 row per commit** across all scripts
- **Repeated subqueries**: `SELECT payment_id FROM payment WHERE bulk_payment_id = X` was executed 4 times per loop iteration in Script 1
- **No batching**: each parent record processed individually

### After: Bulk Batch Processing
```
LOOP
    FETCH cursor BULK COLLECT INTO id_array LIMIT 1000;
    -- Pre-collect dependent IDs once per batch
    SELECT payment_id BULK COLLECT INTO pay_ids FROM payment WHERE bulk_payment_id IN (...);
    SELECT execution_id BULK COLLECT INTO exec_ids FROM workflow_execution WHERE payment_id IN (...);
    -- FORALL array-bound deletes
    FORALL i IN 1..id_array.COUNT DELETE FROM table1 WHERE id = id_array(i);
    FORALL i IN 1..id_array.COUNT DELETE FROM table2 WHERE id = id_array(i);
    ...
    COMMIT;   -- commit every 1000 rows
END LOOP;
```
- **1000 rows per commit** (configurable)
- **BULK COLLECT + FORALL**: array-bound DML, dramatically reduces context switches between PL/SQL and SQL engines
- **Pre-collected dependent IDs**: payment_ids and execution_ids fetched once per batch, reused across 5+ dependent table deletes
- **Eliminates repeated subqueries**: from 4 executions per row to 1 per batch

### Performance Impact
| Metric | Before | After |
|--------|--------|-------|
| Context switches per 1000 rows | 13,000 (13 statements × 1000 rows) | 13 (13 FORALL statements × 1 batch) |
| Subquery executions per 1000 bulk payments | 4,000 | 2 |
| Commits per 1000 rows | 1,000 | 1 |
| Redo log pressure | Very high (commit per row) | Low (commit per batch) |

---

## 5. Parameterization Changes

| Parameter | Before | After |
|-----------|--------|-------|
| Retention period | Hardcoded `nbDays Number := 30` in each script | Configurable `p_retention_days` parameter (default 30) |
| Purge scope | Run all 3 scripts or skip manually | `p_purge_depth`: ALL, PAYMENTS, LOGS, BANK_STATEMENTS |
| Batch size | N/A (row-by-row) | `p_batch_size` (default 1000) |
| Dry run | Not available | `p_dry_run`: counts without deleting |
| Space reclamation | Not available | `p_reclaim_space`: optional SHRINK SPACE after purge |
| Database connection | Edit and run in sqlplus manually | Wrapper scripts with interactive prompts or config file |

---

## 6. Logging & Auditability Changes

### Before
- Only **errors** were logged (via `op.SPEC_OUTILS.AddSpecLog`)
- No record of what was **successfully** deleted
- No row counts
- No timestamps for individual operations
- No way to query purge history from other applications
- Dependent on external logging utility (`op.SPEC_OUTILS` package)

### After
- **Every operation** logged to `oppayments.epf_purge_log` table
- Logs include: module, table name, rows affected, batch number, timestamps, elapsed time, status, errors
- Each run has a unique `run_id` (GUID) linking all entries
- **Autonomous transactions**: log entries survive even if the batch rolls back
- Log table is **queryable by other applications** (dashboards, monitoring, compliance)
- Self-contained: no dependency on external logging utilities
- Wrapper scripts also capture sqlplus output to local log files

### New Log Table Columns
`log_id`, `run_id`, `log_timestamp`, `module`, `operation`, `table_name`, `rows_affected`, `batch_number`, `retention_days`, `status`, `message`, `error_code`, `error_message`, `elapsed_seconds`

---

## 7. Space Reclamation (New Feature)

### Before
- No space reclamation capability
- Team manually performed export/import to new database after purge (unsustainable)
- Disk usage remained unchanged after purge

### After
- Built-in `reclaim_space` procedure with 3 tiers (lightweight, online):
  - **Tier 1**: `ALTER TABLE ... SHRINK SPACE CASCADE` (online, no downtime)
  - **Tier 2**: `ALTER TABLESPACE ... COALESCE` (merge free extents)
  - **Tier 3**: `ALTER DATABASE DATAFILE ... RESIZE` (actually shrink OS files, requires DBA)
- Each tier logged with success/failure per table
- **Tier 4: Full tablespace reclaim** via dedicated tool (`bin/epf_tablespace_reclaim.sh`):
  - Exports all schemas sharing the tablespace via Data Pump (`expdp`)
  - Drops the tablespace entirely (including contents and datafiles)
  - Recreates it as BIGFILE with AUTOEXTEND ON, ASSM, locally managed
  - Reassigns default tablespace to all affected users
  - Reimports all schemas via Data Pump (`impdp`)
  - Verifies all objects and recompiles invalid ones
  - Automatic detection of Data Pump directory (PDB_DATA_PUMP_DIR or DATA_PUMP_DIR)
- Dedicated documentation: `docs/space_reclamation_guide.md`

---

## 8. Deployment & Execution Changes

### Before
1. Open sqlplus manually
2. Connect to database
3. Run `@1.PurgeRemisesDePaiements.sql`
4. Run `@2.PurgeTableLogsEtArchive.sql`
5. Run `@3.PurgeRelevesBancaires.sql`
6. No prerequisite checks, no summary output

### After
1. Run `bin/epf_purge.sh` or `bin/epf_purge.bat`
2. Script automatically:
   - Validates prerequisites (sqlplus, ORACLE_HOME, connectivity, SQL files)
   - Prompts for configuration with explanations (or reads config file)
   - Deploys log table DDL + package spec + package body
   - Checks for compilation errors
   - Executes purge with configured parameters
   - Displays summary from `epf_purge_log`
   - Optionally drops the package
   - Writes all output to timestamped log file
3. Supports both interactive and automated (config file) modes
4. Supports scheduling via cron/Task Scheduler

---

## 9. Error Handling Changes

| Aspect | Before | After |
|--------|--------|-------|
| Scope | `WHEN OTHERS` around entire block + inner loop | Per-batch error handling + per-module + global |
| On error | Logs to external `op.SPEC_OUTILS.AddSpecLog` and continues | Logs to `epf_purge_log` (autonomous transaction), rolls back batch, continues with next batch |
| Cursor cleanup | Not handled (cursors left open on error) | Explicit `IF cursor%ISOPEN THEN CLOSE` in exception blocks |
| Error visibility | Only in `op.SPEC_OUTILS` log | In `epf_purge_log` table + DBMS_OUTPUT + wrapper log file |
| Validation | None | Purge depth validated before execution, prerequisites checked by wrapper |

---

## 10. New Capabilities Not in Original

| Feature | Description |
|---------|-------------|
| Dry run mode | Preview row counts without deleting |
| Selective purge | Choose PAYMENTS, LOGS, or BANK_STATEMENTS independently |
| Configurable retention | Pass any number of days (not just 30) |
| Configurable batch size | Tune for performance vs undo space |
| Audit log table | Queryable by other applications |
| Run summary | Formatted output after each execution |
| Space reclamation | 3 tiers of tablespace management |
| Config file support | For automated/scheduled runs |
| Cross-platform wrappers | Both Windows (.bat) and Linux (.sh) |
| Prerequisite validation | Checks sqlplus, connectivity, SQL files before running |
| Password security | Environment variable support, masked input on Linux |

---

## 11. Dependencies Changed

### Removed
- `op.SPEC_OUTILS.AddSpecLog` — no longer called. Error logging is self-contained.

### Added
- `oppayments.epf_purge_log` — new audit table (created automatically)
- `oppayments.epf_purge_pkg` — new PL/SQL package (deployed and optionally dropped by wrapper)
- `SYS.ODCINUMBERLIST` — Oracle built-in collection type used for BULK COLLECT IN-list binding

---

## 12. Files Summary

| File | Status | Purpose |
|------|--------|---------|
| `1.PurgeRemisesDePaiements.sql` | Moved to `sql/legacy/` | Original script (preserved) |
| `2.PurgeTableLogsEtArchive.sql` | Moved to `sql/legacy/` | Original script (preserved) |
| `3.PurgeRelevesBancaires.sql` | Moved to `sql/legacy/` | Original script (preserved) |
| `sql/01_create_purge_log_table.sql` | **New** | Audit log table DDL |
| `sql/02_epf_purge_pkg_spec.sql` | **New** | Package specification |
| `sql/03_epf_purge_pkg_body.sql` | **New** | Package body (all purge logic) |
| `sql/04_drop_epf_purge_pkg.sql` | **New** | Package cleanup |
| `bin/epf_purge.sh` | **New** | Linux purge wrapper script |
| `bin/epf_purge.bat` | **New** | Windows purge wrapper script |
| `bin/epf_tablespace_reclaim.sh` | **New** | Linux tablespace reclaim script |
| `bin/epf_tablespace_reclaim.bat` | **New** | Windows tablespace reclaim script |
| `config/epf_purge.conf.example` | **New** | Example config for automation |
| `docs/space_reclamation_guide.md` | **New** | Tablespace management guide |
| `docs/CHANGE_REPORT.md` | **New** | This report |
| `README.md` | **New** | Full documentation |
| `.gitignore` | **New** | Git ignore rules |
