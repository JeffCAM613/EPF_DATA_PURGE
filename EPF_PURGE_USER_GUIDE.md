# EPF Data Purge — User Guide

## Overview

The **EPF Data Purge Tool** removes old data from the ePF (electronic Payment Factory) Oracle database. Over time, the database accumulates payment records, audit logs, and bank statement files that are no longer needed. Without periodic cleanup, the database grows unbounded — slowing queries, lengthening backups, and consuming disk space.

The tool safely deletes data older than a configurable retention period, logs every action to an audit table, and optionally reclaims disk space by compacting the tablespace.

---

## Purge Coverage

The tool covers **27 tables** across two schemas (`OPPAYMENTS` and `OP`), organized into three modules:

| Module | Tables | Root Filter | Description |
|--------|--------|-------------|-------------|
| **PAYMENTS** | 22 | `bulk_payment.value_date` | Bulk payments, individual payments, and all child/grandchild records (signatures, audits, workflows, invoices, transmissions, etc.) plus `file_integration` records. |
| **LOGS** | 3 | `audit_trail.audit_timestamp` / `op.spec_trt_log.dtlog` | Application audit trail, audit archive, and technical processing logs. |
| **BANK_STATEMENTS** | 2 | `file_dispatching.date_reception` | Bank statement files and directory dispatching records (both contain CLOB data). |

**Typical coverage:** 92–99% of the OPPAYMENTS schema data is within purge scope. The remaining ~1–8% consists of configuration/reference tables that are not purged.

### PAYMENTS — Table Hierarchy (22 tables)

```
bulk_payment  ← ROOT (filtered by value_date)
├── bulk_payment_additional_info
├── bulk_signature
├── mandatory_signers
├── oidc_request_token
├── payment_audit (by bulk_payment_id)
├── transmission_execution_audit    (CLOB: message)
├── import_audit_messages           (via import_audit)
├── import_audit
├── notification_execution
├── transmission_execution
├── transmission_exception
├── approbation_execution_opt       (via workflow_execution_opt)
├── workflow_execution_opt
│
└── payment
    ├── approbation_execution       (via workflow_execution)
    ├── workflow_execution
    ├── payment_audit (by payment_id)
    ├── bulkpayment_exception
    ├── invoice_additional_info     (via invoice)
    ├── invoice
    └── payment_additional_info

file_integration  ← SEPARATE (filtered by integration_date)
```

### LOGS — 3 tables

| Table | Schema | Filter |
|-------|--------|--------|
| `audit_archive` | oppayments | FK from audit_trail (child — deleted first) |
| `audit_trail` | oppayments | `audit_timestamp < cutoff` |
| `spec_trt_log` | op | `dtlog < cutoff` |

### BANK_STATEMENTS — 2 tables

| Table | Schema | Filter | CLOB Column |
|-------|--------|--------|-------------|
| `directory_dispatching` | oppayments | FK from file_dispatching (child) | `breakdown_content` |
| `file_dispatching` | oppayments | `date_reception < cutoff` | `file_content` |

---

## Preparation

### Prerequisites

| Requirement | Details |
|-------------|---------|
| **Oracle SQL*Plus** | Must be installed and available on your system `PATH`. |
| **Database user** | `oppayments` — needs `DELETE` on all purged tables, `CREATE TABLE`, `CREATE PROCEDURE`, `CREATE TYPE`. |
| **SYS / DBA password** | Only needed if using `--reclaim` or `--optimize-db`. |

### Before Running — Checklist

1. **Restart the Oracle instance** (recommended).
   A fresh restart clears stale UNDO, TEMP usage, and memory fragmentation — giving the purge a clean baseline and making space reclamation more predictable.

2. **Stop the ePF application** (required only for `--reclaim`).
   The purge itself is online, but space reclamation drops all indexes and constraints. The application must be quiesced before `--reclaim`.

3. **Run a dry run first** (optional) to preview what will be deleted (see usage below).

4. **Verify SQL*Plus connectivity:**
   ```
   sqlplus oppayments/<password>@<TNS_NAME>
   ```

---

## Usage

### Interactive Mode (Recommended for First Use)

Run the script with no arguments — it prompts for everything:

```cmd
:: Windows
cd C:\path\to\EPF_DATA_PURGE
bin\epf_purge.bat

:: Linux
cd /path/to/EPF_DATA_PURGE
chmod +x bin/epf_purge.sh
./bin/epf_purge.sh
```

The script will prompt for: TNS name, password, retention days, purge depth, dry-run preference, and other options. Each prompt includes a description and default value.

### Dry Run (Optional)

Preview what would be deleted without actually deleting:

```cmd
:: Windows
bin\epf_purge.bat --tns <TNS_NAME> --retention 90 --dry-run

:: Linux
./bin/epf_purge.sh --tns <TNS_NAME> --retention 90 --dry-run
```

### Standard Purge

```cmd
bin\epf_purge.bat --tns <TNS_NAME> --retention 90 --batch-size 5000
```

### Purge + Space Reclamation

Reclaim requires a maintenance window (indexes/constraints are dropped during the process):

```cmd
bin\epf_purge.bat --tns <TNS_NAME> --retention 90 --reclaim --sys-password <sys_password>
```

### Purge a Specific Module

```cmd
:: Only payments (90-day retention)
bin\epf_purge.bat --tns <TNS_NAME> --depth PAYMENTS --retention 90

:: Only logs (30-day retention)
bin\epf_purge.bat --tns <TNS_NAME> --depth LOGS --retention 30

:: Only bank statements (60-day retention)
bin\epf_purge.bat --tns <TNS_NAME> --depth BANK_STATEMENTS --retention 60
```

### Reclaim Only (Purge Already Done)

```cmd
bin\epf_purge.bat --tns <TNS_NAME> --reclaim-only --sys-password <sys_password>
```

### Fully Unattended (Scheduled / Automated)

Supply all values on the command line — no prompts:

```cmd
bin\epf_purge.bat --tns <TNS_NAME> --password <password> --sys-password <sys_password> ^
    --retention 90 --depth ALL --batch-size 5000 ^
    --no-dry-run --reclaim --optimize-db -y
```

Or use a configuration file:

```cmd
:: 1. Copy the example config
copy config\epf_purge.conf.example config\epf_purge.conf

:: 2. Edit config\epf_purge.conf with your settings

:: 3. Run with config (passwords via env vars for security)
set EPF_PURGE_PASSWORD=<password>
set EPF_SYS_PASSWORD=<sys_password>
bin\epf_purge.bat --config config\epf_purge.conf -y
```

### Linux Equivalents

All commands above work identically on Linux using `./bin/epf_purge.sh` instead of `bin\epf_purge.bat`. Use `export` for environment variables.

---

## Parameters Quick Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--tns NAME` | *(prompted)* | Oracle TNS name or connect string. |
| `--user NAME` | `oppayments` | Database username. |
| `--password PASS` | *(prompted)* | Database password. Prefer `EPF_PURGE_PASSWORD` env var. |
| `--sys-password PASS` | *(prompted)* | SYS password (for `--reclaim` / `--optimize-db`). Prefer `EPF_SYS_PASSWORD` env var. |
| `--retention N` | `30` | Keep data newer than N days. |
| `--depth SCOPE` | `ALL` | `ALL`, `PAYMENTS`, `LOGS`, or `BANK_STATEMENTS`. |
| `--batch-size N` | `1000` | Rows per commit. Larger = faster, more UNDO. |
| `--dry-run` | off | Preview only — no deletes. |
| `--reclaim` | off | Post-purge space reclamation. Needs SYS + maintenance window. |
| `--reclaim-only` | off | Skip purge, reclaim only. |
| `--optimize-db` | off | Pre-purge redo log and stats optimization. Needs SYS. |
| `--drop-pkg` | off | Remove PL/SQL package after purge. |
| `--drop-logs` | off | Remove purge log tables after run. |
| `--truncate-logs` | off | Clear previous run history before starting. |
| `--config FILE` | *(none)* | Load settings from config file. CLI overrides config. |
| `-y` / `--assume-yes` | off | Skip all confirmation prompts. |

Use `--no-dry-run`, `--no-reclaim`, `--no-optimize-db`, `--no-drop-pkg`, `--no-truncate-logs` to explicitly disable a setting that a config file may have enabled.

### Environment Variables

| Variable | Purpose |
|----------|---------|
| `EPF_PURGE_PASSWORD` | OPPAYMENTS password. Overrides `--password` and config. |
| `EPF_SYS_PASSWORD` | SYS password. Overrides `--sys-password` and config. |

---

## Live Monitoring

The tool automatically opens a live progress monitor:

- **Windows:** A separate "EPF Live Monitor" console window opens and displays real-time progress. It remains open after completion — close it manually.
- **Linux:** The monitor runs in the background. Watch progress from another terminal:
  ```bash
  tail -f logs/epf_purge_<timestamp>.log
  ```

Both wrapper output and monitor updates are merged chronologically into the log file (`logs/epf_purge_<timestamp>.log`).

---

## Checking Results

### After-Run Summary

The tool prints a summary automatically. You can also query the audit table:

```sql
-- Latest run summary
SELECT module, status, SUM(rows_affected) AS total_rows,
       ROUND(SUM(NVL(elapsed_seconds, 0)), 1) AS elapsed_sec
FROM oppayments.epf_purge_log
WHERE run_id = (
    SELECT run_id FROM oppayments.epf_purge_log
    WHERE operation = 'RUN_START'
    ORDER BY log_timestamp DESC FETCH FIRST 1 ROW ONLY
)
GROUP BY module, status
ORDER BY module, status;
```

### Check for Errors

```sql
SELECT log_timestamp, module, table_name, error_code, error_message
FROM oppayments.epf_purge_log
WHERE status = 'ERROR'
ORDER BY log_timestamp DESC;
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `SQL*Plus not found` | Install Oracle Client (Instant Client is sufficient) and add `sqlplus` to `PATH`. |
| Connection failed | Verify TNS name (`tnsping <TNS_NAME>`), credentials, and `tnsnames.ora`. |
| ORA-30036 (UNDO full) | Reduce `--batch-size` to 500 or smaller. |
| Disk space not freed after purge | Normal — use `--reclaim` to shrink the datafiles. |
| Purge seems stuck | `SELECT module, action, client_info FROM v$session WHERE module = 'EPF_PURGE';` |
| Package compilation error | `SELECT line, text FROM user_errors WHERE name = 'EPF_PURGE_PKG';` |
| Reclaim crashed / indexes missing | Run `sql/utility/17_reclaim_recovery.sql` as SYS. It diagnoses the state, moves any stranded tables from EPF_SCRATCH back, recreates all missing indexes/constraints, and drops SCRATCH. Safe to re-run. |
| `EPF_SCRATCH` tablespace left behind | Run `sql/utility/17_reclaim_recovery.sql` — it handles table repatriation and SCRATCH cleanup automatically. |

---

## About Space Reclamation

After a purge, the deleted data frees logical space inside the Oracle tablespace — Oracle will reuse it for future inserts. However, the `.dbf` datafile on disk **does not shrink automatically**. This is normal Oracle behaviour.

To actually reduce the file size on disk, a **space reclamation** step is needed. This step is inherently **offline** — there is no way to shrink an Oracle datafile while the application is running. Oracle does not provide an online tablespace compaction mechanism; standard techniques such as `SHRINK SPACE`, `REBUILD ONLINE`, and in-place `MOVE` all fail to lower the datafile High Water Mark due to fundamental storage allocation behaviours (locality bias, dual-copy coexistence, extent bitmap reuse).

**You have two options:**

| Option | Description |
|--------|-------------|
| **Use the built-in `--reclaim`** | The script handles everything: drops indexes/constraints, drains tables to a temporary tablespace, refills them compactly, resizes the datafile, and recreates all indexes/constraints. Requires a **maintenance window** (application must be stopped). |
| **DBA-managed reclamation** | Your DBA performs the reclamation using their own methods (export/import, tablespace reorganisation, etc.). The purge tool only deletes the data; the DBA reclaims disk space at their discretion. |

If reclaim is interrupted (crash, disk full, session killed), a dedicated recovery script is provided:

```
sqlplus sys/<SYS_PASS>@<TNS> as sysdba @sql/utility/17_reclaim_recovery.sql
```

This script automatically diagnoses the current state, moves any stranded tables from the scratch tablespace back to the original tablespace, drops the scratch tablespace, recreates all missing indexes and constraints from the DDL backup, and verifies the result. It is safe to run at any time — if nothing is broken, it detects that and exits cleanly.

If neither option is taken, the purge is still effective — freed space is reused by the database for new data — but the datafile will remain at its current size on disk.

---

## Important Notes

- **The purge is online.** It runs while the application is live and does not block transactions.
- **Reclaim is offline.** It drops all indexes and constraints. The ePF application must be stopped. See the section above for why this cannot be done online.
- **Interruption is safe.** Committed batches are permanent. The current batch rolls back. Re-run to continue.
- **Re-running is safe.** The purge selects by date, not by state — running again picks up where it left off.
- **Always dry-run first** on any new environment or retention setting.

---

## File Structure

```
EPF_DATA_PURGE/
├── bin/
│   ├── epf_purge.bat            Windows wrapper
│   ├── epf_purge.sh             Linux wrapper
│   ├── epf_monitor.ps1          Live monitor (PowerShell)
│   └── epf_monitor.sh           Live monitor (Bash)
├── config/
│   └── epf_purge.conf.example   Configuration template
├── logs/                        Runtime log files (auto-created)
├── sql/
│   ├── 01–06, 12                Core SQL (auto-deployed by wrapper)
│   └── utility/                 Diagnostic, utility & recovery queries
├── EPF_PURGE_USER_GUIDE.md      This document
├── EPF_PURGE_REFERENCE.md       Full technical reference
├── README.md                    Quick-start guide
└── reclaim_workflow.md          Space reclamation deep-dive
```
