# EPF Data Purge — Complete Technical Reference

> Comprehensive documentation for the EPF Data Purge tool. Covers architecture, SQL components, execution flow, configuration, space reclamation internals, and operational reference.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [File Structure](#2-file-structure)
3. [Purge Coverage — 27 Tables](#3-purge-coverage--27-tables)
4. [Configuration](#4-configuration)
5. [Command-Line Reference](#5-command-line-reference)
6. [Execution Flow](#6-execution-flow)
7. [Space Reclamation (`--reclaim`)](#7-space-reclamation---reclaim)
8. [Pre-Purge Optimization (`--optimize-db`)](#8-pre-purge-optimization---optimize-db)
9. [PL/SQL Package — `epf_purge_pkg`](#9-plsql-package--epf_purge_pkg)
10. [SQL Components](#10-sql-components)
11. [Live Monitoring](#11-live-monitoring)
12. [Audit & Logging](#12-audit--logging)
13. [Safety & Error Handling](#13-safety--error-handling)
14. [Privilege Model](#14-privilege-model)
15. [Operational Guide](#15-operational-guide)
16. [Troubleshooting Reference](#16-troubleshooting-reference)
17. [Utility Scripts](#17-utility-scripts)

---

## 1. Architecture Overview

The EPF Data Purge Tool is a wrapper-driven PL/SQL purge system for the ePF (electronic Payment Factory) Oracle database. It consists of:

- **Shell wrappers** (`bin/epf_purge.bat`, `bin/epf_purge.sh`) — parse arguments, manage interactive prompts, deploy SQL to the database, orchestrate the purge and post-purge operations, and handle logging.
- **PL/SQL package** (`oppayments.epf_purge_pkg`) — deployed at runtime; performs the actual batched deletes with autonomous-transaction logging.
- **Standalone SQL scripts** — handle space reclamation, DB optimization, temporary index management, and diagnostics.
- **Live monitor** (`bin/epf_monitor.ps1`, `bin/epf_monitor.sh`) — polls the `epf_purge_log` table and displays real-time progress.

### Design Principles

| Principle | Implementation |
|-----------|----------------|
| **Idempotent deployment** | All SQL scripts use `IF NOT EXISTS` / `CREATE OR REPLACE`. Safe to run repeatedly. |
| **Autonomous logging** | Log entries use `PRAGMA AUTONOMOUS_TRANSACTION` — they survive even if the main transaction rolls back. |
| **Batched deletes** | BULK COLLECT + FORALL with configurable batch size. Commits after each batch to keep UNDO small. |
| **FK-ordered deletion** | Children are always deleted before parents to respect foreign key constraints. |
| **Privilege separation** | Purge runs as `oppayments`. Reclaim and optimization run as `SYS` / `SYSDBA`. |
| **Cross-platform** | Feature-parity between Windows (`.bat` + PowerShell monitor) and Linux (`.sh` + Bash monitor). |

---

## 2. File Structure

```
EPF_DATA_PURGE/
│
├── bin/
│   ├── epf_purge.bat                 Windows wrapper script
│   ├── epf_purge.sh                  Linux wrapper script
│   ├── epf_monitor.ps1               Live progress monitor (PowerShell)
│   └── epf_monitor.sh                Live progress monitor (Bash)
│
├── config/
│   └── epf_purge.conf.example        Configuration file template
│
├── logs/                             Runtime logs (auto-created, timestamped)
│
├── sql/
│   ├── 01_create_purge_log_table.sql   Log table + EPF_NUMBER_TAB type DDL
│   ├── 02_epf_purge_pkg_spec.sql       PL/SQL package specification
│   ├── 03_epf_purge_pkg_body.sql       PL/SQL package body (~3000 lines)
│   ├── 04_drop_epf_purge_pkg.sql       Package teardown
│   ├── 05_reclaim_tablespace.sql       Space reclamation (iterative drain/refill)
│   ├── 05a_shrink_tables.sql           SHRINK SPACE utility
│   ├── 06_optimize_db.sql              Redo log enlargement + stats gathering
│   ├── 06b_create_purge_indexes.sql    Temporary FK indexes for purge performance
│   ├── 06c_drop_purge_indexes.sql      Drop temporary FK indexes
│   ├── 12_capture_module_sizes.sql     Pre-purge size discovery per module
│   │
│   └── utility/                        Diagnostic and utility scripts
│       ├── 07_diagnostic_queries.sql     Debugging queries
│       ├── 08_undo_tune.sql              UNDO retention tuning
│       ├── 09_space_compare.sql          Before/after space comparison
│       ├── 10_table_size_audit.sql       Per-table sizing
│       ├── 11_show_module_sizes.sql      Module-level size breakdown
│       ├── 13_dump_run_log.sql           Export run log to screen/file
│       ├── 14_recover_indexes.sql        Emergency index recovery from log
│       ├── 15_segment_map.sql            Segment/HWM analysis
│       ├── 16_fk_coverage_scan.sql       FK coverage validation
│       └── 17_reclaim_recovery.sql       Full reclaim recovery
│
├── EPF_PURGE_USER_GUIDE.md             Simplified user guide
├── EPF_PURGE_REFERENCE.md              This document
├── README.md                           Quick-start guide
└── reclaim_workflow.md                 Space reclamation deep-dive
```

### File Roles

| File | Deployed By | Runs As | When |
|------|-------------|---------|------|
| `01_create_purge_log_table.sql` | Wrapper | `oppayments` | Always (first step) |
| `02_epf_purge_pkg_spec.sql` | Wrapper | `oppayments` | Always (before purge) |
| `03_epf_purge_pkg_body.sql` | Wrapper | `oppayments` | Always (before purge) |
| `04_drop_epf_purge_pkg.sql` | Wrapper | `oppayments` | `--drop-pkg` only |
| `05_reclaim_tablespace.sql` | Wrapper | `SYS` | `--reclaim` / `--reclaim-only` |
| `05a_shrink_tables.sql` | Wrapper | `SYS` | Before reclaim drain phase |
| `06_optimize_db.sql` | Wrapper | `SYS` | `--optimize-db` only |
| `06b_create_purge_indexes.sql` | Wrapper | `oppayments` | Before purge (temp FK indexes) |
| `06c_drop_purge_indexes.sql` | Wrapper | `oppayments` | After purge (cleanup) |
| `12_capture_module_sizes.sql` | Wrapper | `oppayments` | Pre-purge sizing display |

---

## 3. Purge Coverage — 27 Tables

### 3.1. PAYMENTS Module — 22 Tables

The root table is `bulk_payment`. The date filter is `bulk_payment.value_date < SYSDATE - retention_days`. All children/grandchildren are identified by foreign key relationships and deleted in leaf-first order.

```
bulk_payment  ← ROOT (filtered by value_date)
│
├── bulk_payment_additional_info      (direct child)
├── bulk_signature                    (direct child)
├── mandatory_signers                 (direct child)
├── oidc_request_token                (direct child)
├── payment_audit (by bulk_payment_id)(direct child)
├── transmission_execution_audit      (direct child — CLOB: message)
├── import_audit_messages             (grandchild via import_audit)
├── import_audit                      (direct child)
├── notification_execution            (direct child)
├── transmission_execution            (direct child)
├── transmission_exception            (direct child)
├── approbation_execution_opt         (grandchild via workflow_execution_opt)
├── workflow_execution_opt            (direct child)
│
└── payment  (direct child of bulk_payment)
    │
    ├── approbation_execution         (grandchild via workflow_execution)
    ├── workflow_execution            (child of payment)
    ├── payment_audit (by payment_id) (child of payment)
    ├── bulkpayment_exception         (child of payment)
    ├── invoice_additional_info       (grandchild via invoice)
    ├── invoice                       (child of payment)
    └── payment_additional_info       (child of payment)

file_integration  ← SEPARATE (filtered by integration_date)
```

**Deletion strategy:** For each batch of `bulk_payment` IDs:
1. Collect all `payment.id` values for the batch into `EPF_NUMBER_TAB` (materialized once).
2. Delete all grandchild tables using the materialized payment ID list.
3. Delete all direct children of `bulk_payment`.
4. Delete `payment` rows.
5. Delete `bulk_payment` rows.
6. COMMIT.

`file_integration` is purged separately using its own `integration_date` column.

### 3.2. LOGS Module — 3 Tables

| Table | Schema | Date Column | Relationship |
|-------|--------|-------------|-------------|
| `audit_archive` | `oppayments` | FK from `audit_trail` | Child — deleted first |
| `audit_trail` | `oppayments` | `audit_timestamp` | Parent |
| `spec_trt_log` | `op` | `dtlog` | Standalone |

### 3.3. BANK_STATEMENTS Module — 2 Tables

| Table | Schema | Date Column | CLOB Column |
|-------|--------|-------------|-------------|
| `directory_dispatching` | `oppayments` | FK from `file_dispatching` | `breakdown_content` |
| `file_dispatching` | `oppayments` | `date_reception` | `file_content` |

### 3.4. CLOB-Bearing Tables

| Table | CLOB Column | Impact on Purge |
|-------|-------------|-----------------|
| `transmission_execution_audit` | `message` | XML/text content. Extra UNDO generated per row. |
| `directory_dispatching` | `breakdown_content` | File breakdown data. |
| `file_dispatching` | `file_content` | Full file contents. Largest UNDO impact per row. |

### 3.5. Coverage Statistics

Typical coverage across tested environments:

| Metric | Value |
|--------|-------|
| Tables covered | 27 of ~35 in schema |
| Schema data covered | 92–99% (varies by environment) |
| Data outside coverage | Configuration/reference tables (users, permissions, templates, etc.) |

---

## 4. Configuration

### 4.1. Configuration File

Copy `config/epf_purge.conf.example` to `config/epf_purge.conf` and edit:

```properties
# Oracle Connection
TNS_NAME=MYDB
# USERNAME=oppayments              # Default: oppayments
# PASSWORD=                        # Prefer EPF_PURGE_PASSWORD env var
# SYS_PASSWORD=                    # Prefer EPF_SYS_PASSWORD env var

# Purge Parameters
RETENTION_DAYS=30                   # Days of data to keep
PURGE_DEPTH=ALL                     # ALL | PAYMENTS | LOGS | BANK_STATEMENTS
# PURGE_MODE=FULL                  # FULL | CLOB_ONLY | CLOB_N_LOGS
# BATCH_SIZE=1000                  # Rows per commit

# Execution Flags
# DRY_RUN=N
# RECLAIM_SPACE=N
# OPTIMIZE_DB=N
# ASSUME_YES=N
# DROP_PACKAGE_AFTER=N
# DROP_LOGS=N
# TRUNCATE_LOGS=N
```

**Precedence order:** Environment variables > CLI arguments > Config file > Defaults

### 4.2. Environment Variables

| Variable | Purpose |
|----------|---------|
| `EPF_PURGE_PASSWORD` | OPPAYMENTS user password. Highest precedence for `--password`. |
| `EPF_SYS_PASSWORD` | SYS user password. Highest precedence for `--sys-password`. |

### 4.3. Purge Modes

| Mode | Behavior |
|------|----------|
| `FULL` | Delete entire rows (default). Reclaims all space including CLOBs. |
| `CLOB_ONLY` | Truncate CLOB columns to `EMPTY_CLOB()` but keep rows intact. Useful when row metadata must be preserved but file contents can be purged. |
| `CLOB_N_LOGS` | Truncate CLOBs on payment/bank-statement tables + full delete on LOGS module. Hybrid approach. |

---

## 5. Command-Line Reference

### 5.1. Full Parameter List

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--tns NAME` | *(prompted)* | Oracle TNS name or connect string. |
| `--user NAME` | `oppayments` | Database username. |
| `--password PASS` | *(prompted)* | Database password. |
| `--sys-password PASS` | *(prompted)* | SYS password (for `--reclaim` / `--optimize-db`). |
| `--retention N` | `30` | Keep data newer than N days. |
| `--depth SCOPE` | `ALL` | `ALL`, `PAYMENTS`, `LOGS`, or `BANK_STATEMENTS`. |
| `--batch-size N` | `1000` | Rows per commit batch. |
| `--dry-run` / `--no-dry-run` | off | Preview mode — count affected rows, delete nothing. |
| `--reclaim` / `--no-reclaim` | off | Post-purge space reclamation. Requires SYS + maintenance window. |
| `--reclaim-only` | off | Skip purge, only reclaim space. |
| `--optimize-db` / `--no-optimize-db` | off | Pre-purge redo log and stats optimization. Requires SYS. |
| `--drop-pkg` / `--no-drop-pkg` | off | Remove PL/SQL package after purge. |
| `--drop-logs` | off | Remove purge log tables (`epf_purge_log`, `epf_purge_space_snapshot`). |
| `--truncate-logs` / `--no-truncate-logs` | off | Clear previous run history before starting. |
| `--config FILE` | *(none)* | Load settings from config file. |
| `-y` / `--assume-yes` | off | Skip all confirmation prompts. |

The `--no-*` variants allow CLI to explicitly disable a flag that a config file enables.

### 5.2. Interactive vs Non-Interactive

Any parameter supplied on the command line or via config file skips its corresponding interactive prompt. To run fully unattended:

1. Supply all required values via CLI + config + env vars.
2. Pass `-y` to skip confirmation prompts.

### 5.3. Usage Examples

**Dry run — preview only:**
```cmd
bin\epf_purge.bat --tns MYDB --retention 90 --dry-run
```

**Standard purge:**
```cmd
bin\epf_purge.bat --tns MYDB --retention 90 --batch-size 5000
```

**Purge + reclaim:**
```cmd
bin\epf_purge.bat --tns MYDB --retention 90 --reclaim --sys-password <SYS_PASS>
```

**Specific module only:**
```cmd
bin\epf_purge.bat --tns MYDB --depth BANK_STATEMENTS --retention 60
```

**Reclaim only (purge already done):**
```cmd
bin\epf_purge.bat --tns MYDB --reclaim-only --sys-password <SYS_PASS>
```

**Fully unattended via config file + env vars:**
```cmd
set EPF_PURGE_PASSWORD=<password>
set EPF_SYS_PASSWORD=<sys_password>
bin\epf_purge.bat --config config\epf_purge.conf -y
```

**Fully unattended — all parameters on the command line, no prompts:**
```cmd
bin\epf_purge.bat --tns MYDB --user oppayments ^
    --password <password> --sys-password <sys_password> ^
    --retention 90 --depth ALL --batch-size 5000 ^
    --no-dry-run --reclaim --optimize-db --drop-pkg -y
```

```bash
# Linux equivalent
./bin/epf_purge.sh --tns MYDB --user oppayments \
    --password <password> --sys-password <sys_password> \
    --retention 90 --depth ALL --batch-size 5000 \
    --no-dry-run --reclaim --optimize-db --drop-pkg -y
```

---

## 6. Execution Flow

```
 1. PARSE          Parse CLI args / load config / run interactive prompts
                           │
 2. CONNECT        Verify sqlplus on PATH, test DB connectivity
                           │
 3. SIZE SCAN      Run 12_capture_module_sizes.sql — display per-module
                   data sizes and purge estimates
                           │
 4. OPTIMIZE       (if --optimize-db) Run 06_optimize_db.sql as SYS:
                   enlarge redo logs to 1 GB, gather optimizer stats
                           │
 5. DEPLOY         Run as oppayments:
                   01_create_purge_log_table.sql → 02_epf_purge_pkg_spec.sql
                   → 03_epf_purge_pkg_body.sql
                           │
 6. VERIFY         Check for package compilation errors
                           │
 7. CREATE INDEXES Run 06b_create_purge_indexes.sql — create temporary
                   FK indexes to speed up child-table lookups
                           │
 8. TUNE UNDO      Lower undo_retention from 900s to 60s
                           │
 9. MONITOR        Start live progress monitor (separate window / background)
                           │
10. PURGE          Execute oppayments.epf_purge_pkg.run_purge():
                     • Snapshot segment sizes (BEFORE)
                     • Delete PAYMENTS (22 tables, batch by batch)
                     • Delete LOGS (3 tables, batch by batch)
                     • Delete BANK_STATEMENTS (2 tables, batch by batch)
                     • Snapshot segment sizes (AFTER)
                     • Print run summary + space comparison
                           │
11. RESTORE        Restore undo_retention to 900s, stop monitor
                           │
12. DROP INDEXES   Run 06c_drop_purge_indexes.sql — remove temp FK indexes
                           │
13. RECLAIM        (if --reclaim) Run 05_reclaim_tablespace.sql as SYS:
                   14-phase iterative drain/refill (see Section 7)
                           │
14. CLEANUP        (if --drop-pkg) Run 04_drop_epf_purge_pkg.sql
                   (if --drop-logs) Drop log tables
                           │
15. LOG            Write timestamped log to logs/ directory
```

---

## 7. Space Reclamation (`--reclaim`)

### 7.1. Why Reclaim Is Needed

After `DELETE`, Oracle marks freed blocks as reusable but the `.dbf` datafile size remains unchanged. The **High Water Mark (HWM)** — the highest block ever allocated — determines the minimum file size. Even after deleting 80% of data, a single index extent near the top of the file prevents shrinking.

### 7.2. Why Reclaim Must Be Offline

Oracle does not provide a built-in way to shrink a datafile while the application remains online. Every standard "online" technique was evaluated against ePF database tables and failed due to fundamental Oracle storage behaviours:

| Approach | What Happens | Why It Fails |
|----------|-------------|---------------|
| **`ALTER TABLE ... SHRINK SPACE`** | In-place row compaction within a table. | Only works on heap tables — **indexes cannot be shrunk**. Indexes are usually what pin the High Water Mark (HWM). On CLOB-heavy tables (e.g. `AUDIT_TRAIL`, `SPEC_TRT_LOG`), SHRINK performs row-by-row compaction and can hang for 16+ hours with no measurable progress. |
| **`ALTER INDEX ... REBUILD ONLINE`** | Rebuilds an index while DML continues. | Oracle places the new index copy near the old one ("locality bias"). The old extents are freed only after the new copy is fully built, but by then it already occupies the same high blocks. Iterating 2,000+ times still leaves the HWM unchanged. |
| **`ALTER TABLE ... MOVE` (in-place)** | Rewrites a table within the same tablespace. | Requires space for both the old and new copies simultaneously. When free space below the HWM is less than the largest table, Oracle auto-extends **above** the HWM — the datafile actually **grows larger** than before. Observed: HWM rising from 22.9 GB to 32.6 GB during an in-place MOVE attempt. |
| **Move to scratch TS and back (one-at-a-time)** | Relocate a single segment to a temporary tablespace, then return it. | Oracle's bitmap allocator reuses the just-freed extents — the table returns to the exact same blocks. 584 tables were round-tripped this way with zero change in the segment map. |

> **Important:** The approaches above do not merely fail to reclaim space — several of them risk **increasing** the datafile size instead.

The only two techniques tested on ePF database tables that **guarantee** effective space reclamation are:

1. **Full drain and refill** (used by this script's `--reclaim`) — drain every segment out of the tablespace into a temporary scratch tablespace so the original becomes truly empty, then refill the data back. Tables pack sequentially from block 0, eliminating all fragmentation. The combined size of the original and scratch tablespaces is actively maintained throughout the process: both datafiles are resized after each transfer batch so that space freed from one offsets growth in the other, keeping total disk usage balanced.

2. **Export/Import** (traditional DBA method) — `expdp` the schema, drop and recreate the tablespace, then `impdp` the data back. Achieves the same result but requires more manual steps and temporary disk for the dump file.

Both techniques require the application to be offline.

**The built-in `--reclaim` is optional.** It is provided as a convenience for environments where space reclamation is needed immediately after purge. If your DBA prefers to manage tablespace reclamation independently — using export/import, their own tooling, or a scheduled maintenance process — they are free to do so. The EPF purge tool only deletes the data; the DBA reclaims disk space at their discretion.

If neither option is taken, the purge still frees logical space within the tablespace — Oracle will reuse it for future inserts — but the `.dbf` file on disk will not shrink.

### 7.3. Strategy: Drop, Compact, Recreate

| Phase | Operation | Details |
|-------|-----------|---------|
| 1 | **Discover tablespaces** | Identify DATA and (optional) INDEX tablespaces for OPPAYMENTS. |
| 2 | **Relocate log tables** | Move `epf_purge_log` and `epf_purge_space_snapshot` to a neutral tablespace (e.g., USERS) so their writes don't anchor the HWM. |
| 3 | **Capture DDL** | `DBMS_METADATA.GET_DDL` for all indexes, PK/UK constraints, and FK constraints in `oppayments` + `op`. Stored in PL/SQL arrays. Also logged to `epf_purge_log` for recovery. |
| 4 | **Drop FK constraints** | FKs first — avoids lock conflicts with PK drops. |
| 5 | **Drop PK/UK constraints** | Backing indexes dropped automatically. |
| 6 | **Drop secondary indexes** | All remaining non-constraint indexes. |
| 7a | **Create scratch TS** | `EPF_SCRATCH` tablespace (1 GB + AUTOEXTEND). |
| 7b | **Drain tables** | `ALTER TABLE ... MOVE TABLESPACE EPF_SCRATCH` for each table (highest HWM first). DATA datafile resized after every ~1 GB drained to release space as SCRATCH grows. |
| 7c | **Resize DATA** | Shrink to HWM + 50 MB margin. Set AUTOEXTEND ON. |
| 7d | **Refill tables** | Move tables back from SCRATCH to DATA (highest HWM first). SCRATCH datafile resized after each batch to release space as DATA grows. |
| 7e | **Drop scratch TS** | Drop `EPF_SCRATCH` (only if empty). |
| 8 | **Resize DATA datafile** | `ALTER DATABASE DATAFILE ... RESIZE`. ORA-03297 handled by stepping up 1 GB. |
| 9 | **Resize INDEX datafile** | Same for separate index tablespace (if applicable). |
| 10 | **Recreate secondary indexes** | From captured DDL. No old copies → allocator places in lowest free space. |
| 11 | **Recreate PK/UK** | Including `USING INDEX TABLESPACE ...` clause. |
| 12 | **Recreate FK** | Last — all referenced PKs now exist. |
| 13 | **Final resize** | Capture true post-recreate footprint. |
| 14 | **Shrink UNDO/TEMP** | Best-effort compaction of UNDO and TEMP tablespaces. |

### 7.4. Key Properties

| Property | Detail |
|----------|--------|
| **Online?** | **No.** Indexes/constraints are dropped. Application must be quiesced. |
| **Deterministic?** | Yes. Single pass, no iterative loop, no stall detection. |
| **Disk profile** | Combined DATA + SCRATCH is actively balanced — both are resized after each transfer batch so total disk usage stays roughly constant. Peak temporary overhead ≈ largest single table. |
| **Duration** | Dominated by `MOVE` (rewrite every block) and `CREATE INDEX` (rebuild from scratch). 30 min to several hours depending on data volume. |
| **LOB handling** | LOB segments moved explicitly before tables. CLOB-heavy tables are typically the slowest phase. |
| **Move order** | Tables moved in ascending `block_id` order. This packs the bottom of the file solidly so higher tables can land in freed low space. Descending order was tested and caused file growth. |

### 7.5. Failure & Recovery

If reclaim is interrupted at any point, run `sql/utility/17_reclaim_recovery.sql` as SYS. It automatically diagnoses the current state, moves stranded tables back from EPF_SCRATCH, drops the scratch tablespace, and recreates all missing indexes/constraints from the DDL backup.

| Scenario | State | Recovery |
|----------|-------|----------|
| Failure in phases 1–3 | Nothing changed. | Re-run when ready. No recovery needed. |
| Failure in phases 4–6 (drop FK/PK/indexes) | Indexes partially dropped, tables still in DATA. | Run `17_reclaim_recovery.sql` — recreates missing objects from backup. |
| Failure in phase 7b (drain) | Tables split: some in DATA, some in SCRATCH. Indexes dropped. | Run `17_reclaim_recovery.sql` — moves tables from SCRATCH back to DATA, drops SCRATCH, recreates all objects. |
| Failure in phase 7d (refill) | Tables split: some still in SCRATCH, most back in DATA. Indexes dropped. | Run `17_reclaim_recovery.sql` — moves remaining SCRATCH tables back, drops SCRATCH, recreates all objects. |
| Failure in phases 10–12 (recreate) | All tables in DATA. Some indexes/constraints partially recreated. | Run `17_reclaim_recovery.sql` — recreates only the missing objects (skips existing). |
| Single table MOVE failure | Table stuck mid-move. | `ALTER TABLE owner.table_name MOVE TABLESPACE target_ts;` then re-run recovery script. |
| EPF_SCRATCH left behind | Tablespace exists but may be empty. | Recovery script drops it if empty, lists remaining segments if not. |
| No DDL backup available | Cannot auto-recreate. | Recovery script prints `impdp` instructions. Restore from metadata export or RMAN. |
| Re-running reclaim | Safe. | Recreate steps check for object existence first. |

### 7.6. Limitations

- Only OPPAYMENTS data/index tablespaces are resized. Tables in other tablespaces (e.g., `op.spec_trt_log` in a third TS) are not touched.
- No partial-progress checkpoint. A crash mid-MOVE may leave a table unusable (fix with manual `ALTER TABLE ... MOVE`).
- Concurrent activity is not tolerated. Read queries will fall back to full scans; DML may violate integrity.

---

## 8. Pre-Purge Optimization (`--optimize-db`)

Runs `sql/06_optimize_db.sql` as SYS. Two operations:

### 8.1. Redo Log Enlargement

Bulk DELETEs generate substantial redo. Small redo logs (e.g., 200 MB) cause frequent `log file switch` waits.

The script:
1. Creates 4 new redo log groups at 1 GB each.
2. Switches logs to cycle through old (small) groups.
3. Drops old (small) groups once they reach `INACTIVE` status.
4. On failure, rolls back by dropping newly created groups.

### 8.2. Optimizer Statistics

Gathers fresh statistics on the `OPPAYMENTS` schema:

```sql
DBMS_STATS.GATHER_SCHEMA_STATS('OPPAYMENTS', estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE);
```

Ensures the optimizer has accurate cardinality estimates for the purge queries.

---

## 9. PL/SQL Package — `epf_purge_pkg`

### 9.1. Package Specification

```
Package: oppayments.epf_purge_pkg
Auth:    CURRENT_USER
```

**Public procedures:**

| Procedure | Purpose |
|-----------|---------|
| `run_purge(...)` | Master entry point. Orchestrates all modules based on `p_purge_depth`. |
| `purge_bulk_payments(...)` | Delete bulk payments + 21 child tables. |
| `purge_file_integrations(...)` | Delete file integration records. |
| `purge_audit_logs(...)` | Delete audit trail + audit archive. |
| `purge_tech_logs(...)` | Delete `op.spec_trt_log`. |
| `purge_bank_statements(...)` | Delete file/directory dispatching. |
| `ensure_log_table` | Create log table if not exists (idempotent). |
| `print_run_summary(p_run_id)` | Output formatted run summary. |
| `capture_space_snapshot(...)` | Record segment sizes (BEFORE/AFTER). |
| `print_space_comparison(...)` | Output before/after space comparison. |

**Parameters for `run_purge`:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `p_retention_days` | NUMBER | 30 | Delete data older than N days. |
| `p_purge_depth` | VARCHAR2 | `'ALL'` | `ALL`, `PAYMENTS`, `LOGS`, or `BANK_STATEMENTS`. |
| `p_batch_size` | NUMBER | 5000 | Rows per batch commit. |
| `p_dry_run` | BOOLEAN | FALSE | Count only — no deletes. |
| `p_purge_mode` | VARCHAR2 | `'FULL'` | `FULL`, `CLOB_ONLY`, or `CLOB_N_LOGS`. |

### 9.2. Internal Mechanics

**Batch processing:**
- BULK COLLECT fetches `p_batch_size` root IDs per iteration.
- For PAYMENTS: materializes all `payment.id` values for the batch into `EPF_NUMBER_TAB` (schema-level nested table type), then uses it in FORALL deletes against 7 grandchild tables — avoiding 7 repeated joins.
- Each batch ends with COMMIT. Progress is logged via autonomous transaction.

**CLOB handling:**
- In `CLOB_ONLY` mode, CLOB columns are set to `EMPTY_CLOB()` via dynamic SQL (discovers columns from `ALL_LOBS`).
- `FULL` mode simply deletes rows; Oracle reclaims the LOB segments.

**Table existence checks:**
- Every table reference is guarded by `tbl_exists()` — the package works on partial EPF deployments where some tables may not exist.

**Error handling:**
- Per-batch: if a batch fails, it rolls back that batch, logs the error, and continues with the next batch.
- Module-level: if a module procedure fails entirely, the error is logged and the next module proceeds.
- All errors are captured in `epf_purge_log` with `status = 'ERROR'`, `error_code`, and `error_message`.

### 9.3. Supporting Types

| Object | Schema | Purpose |
|--------|--------|---------|
| `EPF_NUMBER_TAB` | `oppayments` | `TABLE OF NUMBER` — used for BULK COLLECT of payment IDs. Created by `01_create_purge_log_table.sql`. |

---

## 10. SQL Components

### 10.1. Core Scripts (Auto-Deployed)

| Script | Runs As | Purpose |
|--------|---------|---------|
| `01_create_purge_log_table.sql` | oppayments | Creates `epf_purge_log` table, `epf_purge_space_snapshot` table, and `EPF_NUMBER_TAB` type. Idempotent. |
| `02_epf_purge_pkg_spec.sql` | oppayments | `CREATE OR REPLACE PACKAGE` specification. Defines public API. |
| `03_epf_purge_pkg_body.sql` | oppayments | `CREATE OR REPLACE PACKAGE BODY`. ~3000 lines of purge logic. |
| `04_drop_epf_purge_pkg.sql` | oppayments | Drops the package and type. Used for cleanup after purge. |
| `05_reclaim_tablespace.sql` | SYS | Space reclamation — 14-phase iterative drain/refill. See Section 7. |
| `05a_shrink_tables.sql` | SYS | `ALTER TABLE ... SHRINK SPACE` for all OPPAYMENTS tables. Pre-compaction before drain. |
| `06_optimize_db.sql` | SYS | Enlarges redo logs to 1 GB, gathers optimizer stats. See Section 8. |
| `06b_create_purge_indexes.sql` | oppayments | Creates temporary indexes on FK columns to speed up child-table deletes. |
| `06c_drop_purge_indexes.sql` | oppayments | Drops the temporary indexes created by 06b. |
| `12_capture_module_sizes.sql` | oppayments | Queries `dba_segments` to compute per-module data sizes and retention-based estimates. Output parsed by wrapper for display. |

### 10.2. Log Table Schema

```sql
epf_purge_log (
    log_id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id            RAW(16)        NOT NULL,    -- Groups entries per run
    log_timestamp     TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL,
    module            VARCHAR2(50)   NOT NULL,    -- PAYMENTS, LOGS, BANK_STATEMENTS, RECLAIM, etc.
    operation         VARCHAR2(50)   NOT NULL,    -- RUN_START, DELETE, RECLAIM_END, etc.
    table_name        VARCHAR2(128),              -- Affected table (nullable)
    rows_affected     NUMBER         DEFAULT 0,
    batch_number      NUMBER,
    retention_days    NUMBER,
    status            VARCHAR2(20)   NOT NULL,    -- SUCCESS, ERROR, WARNING, INFO
    message           VARCHAR2(4000),             -- DDL text, status messages, etc.
    error_code        VARCHAR2(50),
    error_message     VARCHAR2(4000),
    elapsed_seconds   NUMBER(10,3)
)
```

Indexes: `run_id`, `log_timestamp`, `module`.

### 10.3. Space Snapshot Table

```sql
epf_purge_space_snapshot (
    snapshot_id       NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id            RAW(16)        NOT NULL,
    snapshot_phase    VARCHAR2(10)   NOT NULL,    -- BEFORE or AFTER
    snapshot_timestamp TIMESTAMP     DEFAULT SYSTIMESTAMP,
    owner             VARCHAR2(128),
    segment_name      VARCHAR2(128),
    segment_type      VARCHAR2(30),
    tablespace_name   VARCHAR2(128),
    bytes             NUMBER,
    parent_table      VARCHAR2(128)               -- For LOB segments: resolved parent
)
```

---

## 11. Live Monitoring

### 11.1. Architecture

The monitor is a standalone process that polls `oppayments.epf_purge_log` every 10 seconds and formats progress output.

| Platform | Script | Mechanism |
|----------|--------|-----------|
| Windows | `bin/epf_monitor.ps1` | Spawned in a new `cmd` window titled "EPF Live Monitor". |
| Linux | `bin/epf_monitor.sh` | Backgrounded process with stdout suppressed. |

### 11.2. What It Shows

- Current operation and table being processed.
- Rows deleted so far (per module and total).
- Elapsed time per phase.
- Reclaim progress (phase name, current table, drain/refill counts).
- Final `RECLAIM_END` / `RUN_END` status line.

### 11.3. Log File

Both wrapper output and monitor samples are merged chronologically into `logs/epf_purge_<YYYYMMDD_HHMMSS>.log`. The log file is the canonical record. A companion `*_monitor.log` may also be created.

---

## 12. Audit & Logging

### 12.1. Database Audit Trail

Every operation is logged to `oppayments.epf_purge_log`:

| Operation Type | Examples |
|----------------|----------|
| Run lifecycle | `RUN_START`, `RUN_END`, `RECLAIM_START`, `RECLAIM_END` |
| Per-table deletes | `DELETE` with `table_name`, `rows_affected`, `batch_number` |
| Space snapshots | `SPACE_SNAPSHOT` with `BEFORE` / `AFTER` phase |
| Reclaim phases | `CAPTURE_INDEX`, `CAPTURE_PK`, `CAPTURE_FK`, `DROP_FK`, `MOVE_TABLE`, etc. |
| Errors | `status = 'ERROR'` with `error_code` and `error_message` |

All entries share a `run_id` (RAW(16)) that groups a single execution.

### 12.2. Querying the Log

```sql
-- Latest run summary by module
SELECT module, status, SUM(rows_affected) AS total_rows,
       ROUND(SUM(NVL(elapsed_seconds, 0)), 1) AS elapsed_sec
FROM oppayments.epf_purge_log
WHERE run_id = (
    SELECT run_id FROM oppayments.epf_purge_log
    WHERE operation = 'RUN_START'
    ORDER BY log_timestamp DESC FETCH FIRST 1 ROW ONLY
)
GROUP BY module, status ORDER BY module, status;

-- All errors
SELECT log_timestamp, module, table_name, error_code, error_message
FROM oppayments.epf_purge_log
WHERE status = 'ERROR' ORDER BY log_timestamp DESC;

-- Captured DDL for recovery (from a specific run)
SELECT operation, table_name, message
FROM oppayments.epf_purge_log
WHERE run_id = <run_id>
  AND operation IN ('CAPTURE_INDEX', 'CAPTURE_PK', 'CAPTURE_FK')
ORDER BY log_id;
```

### 12.3. File Logs

Wrapper output is written to `logs/epf_purge_<YYYYMMDD_HHMMSS>.log`.

---

## 13. Safety & Error Handling

### 13.1. Purge Safety

| Feature | Detail |
|---------|--------|
| **Dry run** | `--dry-run` counts affected rows without deleting. Always run first on new environments. |
| **Batch commits** | Each batch commits independently. Crash only loses the current batch. |
| **UNDO tuning** | Automatically lowers `undo_retention` to 60s during purge to prevent UNDO tablespace growth to 25+ GB. Restored to 900s after. |
| **Per-batch error isolation** | Failed batch rolls back; next batch proceeds. |
| **Autonomous logging** | Log entries use `PRAGMA AUTONOMOUS_TRANSACTION` — survive rollbacks. |
| **Cursor cleanup** | All cursors explicitly closed in exception handlers. |
| **Confirmation prompts** | Interactive confirmation before execution. `-y` to skip. |
| **Idempotent re-run** | Purge selects by date — re-running picks up where it left off. |

### 13.2. Reclaim Safety

| Feature | Detail |
|---------|--------|
| **DDL capture before drop** | All index/constraint DDL is stored in the log table before any drops. |
| **Recreate idempotency** | Recreate steps check for existing objects — safe to re-run. |
| **ORA-03297 handling** | Resize steps catch "data beyond requested size" and step up by 1 GB. |
| **Warning banner** | Wrapper displays an explicit warning about offline operation and waits for confirmation. |

### 13.3. Interruption

- **Purge interrupted:** Committed batches are permanent. Current batch rolls back. Re-run to continue.
- **Reclaim interrupted in phases 4–6:** Indexes/constraints may be partially dropped. Re-run `--reclaim-only` to recreate.
- **Reclaim interrupted in phase 7 (MOVE):** The table being moved may be in an inconsistent state. Fix with manual `ALTER TABLE ... MOVE`.

---

## 14. Privilege Model

| Operation | Runs As | Required Privileges |
|-----------|---------|---------------------|
| Purge | `oppayments` | `DELETE` on all 27 tables, `CREATE TABLE`, `CREATE PROCEDURE`, `CREATE TYPE`, `SELECT` on `all_tables`, `all_tab_columns`, `all_lobs` |
| Space reclaim | `SYS` (SYSDBA) | `ALTER TABLESPACE`, `ALTER DATABASE DATAFILE`, `DBMS_METADATA`, DBA views |
| Optimize DB | `SYS` (SYSDBA) | `ALTER DATABASE`, `DBMS_STATS` |
| Monitor | `oppayments` | `SELECT` on `epf_purge_log` |
| Size scan | `oppayments` | `SELECT` on `dba_segments`, `dba_lobs`, `dba_data_files` (auto-granted when SYS password provided) |

DBA view grants are automatically applied by the wrapper when a SYS password is provided:
```sql
GRANT SELECT ON sys.dba_segments TO oppayments;
GRANT SELECT ON sys.dba_lobs TO oppayments;
GRANT SELECT ON sys.dba_data_files TO oppayments;
```

---

## 15. Operational Guide

### 15.1. Pre-Run Checklist

1. **Restart the Oracle instance** (recommended). Clears stale UNDO/TEMP and gives purge a clean baseline.
2. **Stop the ePF application** (required for `--reclaim` only).
3. **Run a dry run** on any new environment or retention setting.
4. **Take a metadata-only export** before first reclaim:
   ```
   expdp system/<pwd>@<TNS> schemas=OPPAYMENTS,OP content=METADATA_ONLY dumpfile=epf_meta.dmp
   ```
5. **Verify SQL*Plus** connectivity: `sqlplus oppayments/<pwd>@<TNS>`

### 15.2. Recommended Execution Sequence

**First run on a new environment:**
```
1. Dry run           → verify coverage and row counts
2. Purge (live)      → with --optimize-db for redo/stats tuning
3. Verify results    → check epf_purge_log for errors
4. Reclaim (offline) → stop application first, then --reclaim-only
5. Restart app       → verify application health
```

**Routine scheduled runs:**
```
bin\epf_purge.bat --config config\epf_purge.conf -y
```
Reclaim is typically not needed every run — only when accumulated purges have freed significant space. Monitor datafile size vs. schema usage to decide.

### 15.3. Batch Size Tuning

| Batch Size | UNDO Impact | Speed | Recommendation |
|------------|-------------|-------|----------------|
| 500 | Low | Slower | Use when UNDO tablespace is constrained. |
| 1000 | Moderate | Default | Good balance for most environments. |
| 5000 | High | Faster | Use when UNDO is large (10+ GB auto-extend). |
| 10000+ | Very high | Fastest | Only with dedicated UNDO and off-hours window. |

### 15.4. Duration Estimates

| Operation | Typical Duration | Variables |
|-----------|-----------------|-----------|
| Dry run | Seconds | — |
| Purge (ALL, 90-day retention, ~40 GB DB) | 20–30 minutes | Batch size, I/O speed, CLOB volume |
| Reclaim (~40 GB → ~10 GB) | 15–30 minutes | LOB volume, index count, storage throughput |
| Optimize DB | 2–5 minutes | Redo log count, stats gathering scope |

---

## 16. Troubleshooting Reference

| Problem | Diagnosis | Solution |
|---------|-----------|----------|
| `SQL*Plus not found` | `sqlplus` not on PATH | Install Oracle Client (Instant Client is sufficient). Add to PATH. |
| Connection failed | TNS resolution or credential issue | `tnsping <TNS>`. Verify `tnsnames.ora` and credentials. |
| `ORA-30036` (UNDO full) | Batch too large for UNDO tablespace | Reduce `--batch-size` to 500. Or extend UNDO TS. |
| `ORA-03297` during reclaim | Segment above resize target | Handled automatically — script steps up by 1 GB. If persistent, check for non-OPPAYMENTS objects in the TS. |
| Disk space not freed | Normal after DELETE — HWM unchanged | Use `--reclaim` to shrink datafiles. See Section 7. |
| Purge seems stuck | Long-running batch on large CLOB table | `SELECT module, action, client_info FROM v$session WHERE module = 'EPF_PURGE';` |
| Package compilation error | Missing privilege or syntax issue | `SELECT line, text FROM user_errors WHERE name = 'EPF_PURGE_PKG';` |
| Reclaim leaves indexes missing | Reclaim was interrupted | Run `sql/utility/17_reclaim_recovery.sql` as SYS. It moves stranded tables back, recreates all missing objects, and drops SCRATCH. |
| `EPF_SCRATCH` not dropped | Objects still in scratch TS | Run `sql/utility/17_reclaim_recovery.sql` — it moves remaining objects back to DATA then drops SCRATCH. |
| Tables stuck in EPF_SCRATCH | Drain or refill crashed mid-way | Run `sql/utility/17_reclaim_recovery.sql` — diagnoses and repatriates all stranded tables. |
| Monitor window doesn't open | PowerShell execution policy | `Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned` |

---

## 17. Utility Scripts

Located in `sql/utility/`. These are not auto-deployed — run them manually via SQL*Plus as needed.

| Script | Purpose | Run As |
|--------|---------|--------|
| `07_diagnostic_queries.sql` | Collection of diagnostic queries: session info, locks, UNDO usage, segment sizes, long-running operations. | oppayments / SYS |
| `08_undo_tune.sql` | Manually adjust `undo_retention` (increase or decrease). Useful for troubleshooting ORA-30036. | SYS |
| `09_space_compare.sql` | Query `epf_purge_space_snapshot` for before/after space comparison from a previous run. | oppayments |
| `10_table_size_audit.sql` | Detailed per-table sizing with segment breakdown (TABLE, INDEX, LOB). | oppayments (needs DBA views) |
| `11_show_module_sizes.sql` | Module-level size breakdown matching purge coverage categories. | oppayments (needs DBA views) |
| `13_dump_run_log.sql` | Export full `epf_purge_log` contents for a specific run. | oppayments |
| `14_recover_indexes.sql` | Extract captured index/constraint DDL from `epf_ddl_backup` for emergency reconstruction after a failed reclaim. | SYS |
| `15_segment_map.sql` | Maps all segments in a tablespace by block position — shows HWM anchors and fragmentation. | SYS |
| `16_fk_coverage_scan.sql` | Validates that all FK relationships in OPPAYMENTS are covered by the purge order. | oppayments |
| `17_reclaim_recovery.sql` | **Full reclaim recovery.** Diagnoses state after a broken reclaim run: moves stranded tables from EPF_SCRATCH back to DATA, drops SCRATCH, recreates all missing indexes/constraints from DDL backup, resizes datafiles, and verifies the final state. Safe to run at any time. | SYS |
