# EPF Data Purge — User Guide

> A simplified, user-friendly guide to the EPF Data Purge tool.
> For full technical details, see [EPF_PURGE_FULL_DOCUMENTATION.md](EPF_PURGE_FULL_DOCUMENTATION.md).

---

## What Is This Tool?

The **EPF Data Purge Tool** cleans up old data from the ePF (electronic Payment Factory) Oracle database. Over time, the database accumulates payment records, audit logs, and bank statement files that are no longer needed. Without cleanup, the database grows unbounded — slowing down queries, lengthening backups, and wasting disk space.

This tool **safely deletes** data older than a configurable number of days, logs everything it does, and optionally reclaims disk space.

---

## What Does It Do?

| Feature | Description |
|---------|-------------|
| **Purge old data** | Deletes payment records, audit logs, and bank statements older than N days (default: 30). |
| **Configurable retention** | Choose how many days of data to keep. Different modules can use different retentions. |
| **Dry-run mode** | Preview exactly what would be deleted — without deleting anything. Always do this first. |
| **Full audit log** | Every operation is logged to a database table (`epf_purge_log`) with row counts, timing, and errors. |
| **Batch processing** | Deletes in configurable batches (default: 1,000 rows) to avoid long-running transactions. |
| **Space reclamation** | Optionally compacts the database and shrinks datafiles on disk after deleting data. |
| **Live monitoring** | A separate monitor shows real-time progress during long purges. |
| **Selective purge** | Choose to purge only payments, only logs, or only bank statements. |
| **Cross-platform** | Works on Windows (`.bat`) and Linux (`.sh`). |

---

## Requirements

| Requirement | Details |
|-------------|---------|
| **Oracle SQL*Plus** | Must be installed and on your system PATH. |
| **ORACLE_HOME** | Environment variable set to your Oracle client directory (recommended). |
| **Database user: `oppayments`** | Needs `DELETE` on all purged tables, `CREATE TABLE`, `CREATE PROCEDURE`, `CREATE TYPE`. |
| **DBA view grants** | For accurate space comparison: `GRANT SELECT ON sys.dba_segments TO oppayments; GRANT SELECT ON sys.dba_lobs TO oppayments;` (auto-applied when SYS password is provided). |
| **SYS / DBA user** | Only needed for space reclamation (`--reclaim`) or DB optimization (`--optimize-db`). |
| **Disk space** | The purge itself uses minimal disk. Space reclamation (`--reclaim`) is online and needs no extra space. |

---

## How to Run

### 1. Interactive Mode (Recommended for First Time)

Just run the script — it will prompt you for everything:

```cmd
:: Windows
cd C:\path\to\EPF_DATA_PURGE
bin\epf_purge.bat

:: Linux
cd /path/to/EPF_DATA_PURGE
chmod +x bin/epf_purge.sh
./bin/epf_purge.sh
```

### 2. Dry Run First (Always Recommended)

See how many rows would be deleted, without deleting anything:

```cmd
:: Windows
bin\epf_purge.bat --tns EPFPROD --user oppayments --retention 90 --dry-run

:: Linux
./bin/epf_purge.sh --tns EPFPROD --user oppayments --retention 90 --dry-run
```

### 3. Actual Purge

```cmd
bin\epf_purge.bat --tns EPFPROD --user oppayments --retention 90 --batch-size 5000
```

### 4. Purge + Space Reclamation

```cmd
bin\epf_purge.bat --tns EPFPROD --user oppayments --retention 90 --reclaim --sys-password MySysPass
```

### 5. Purge a Specific Module Only

```cmd
:: Only payments (90-day retention)
bin\epf_purge.bat --tns EPFPROD --user oppayments --depth PAYMENTS --retention 90

:: Only logs (30-day retention)
bin\epf_purge.bat --tns EPFPROD --user oppayments --depth LOGS --retention 30

:: Only bank statements (60-day retention)
bin\epf_purge.bat --tns EPFPROD --user oppayments --depth BANK_STATEMENTS --retention 60
```

---

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--tns NAME` | *(required)* | Oracle TNS name or connect string (e.g., `EPFPROD`, `localhost:1521/orcl`). |
| `--user NAME` | `oppayments` | Database username. |
| `--password PASS` | *(prompted)* | Database password. Prefer using the `EPF_PURGE_PASSWORD` environment variable instead. |
| `--retention N` | `30` | Delete data older than N days. |
| `--depth DEPTH` | `ALL` | Which module to purge: `ALL`, `PAYMENTS`, `LOGS`, or `BANK_STATEMENTS`. |
| `--batch-size N` | `1000` | Number of rows per batch. Larger = faster but uses more UNDO space. |
| `--dry-run` | off | Count affected rows without deleting. Always run this first. |
| `--optimize-db` | off | Pre-purge optimization: enlarge redo logs to 1 GB and gather optimizer stats. Needs SYS. |
| `--reclaim` | off | Post-purge space reclamation: SHRINK segments + squeeze + resize datafiles. Needs SYS. |
| `--reclaim-only` | off | Skip the purge, only reclaim space. Useful if you already purged earlier. |
| `--no-stall-check` | off | Disable stall detection during space reclamation. |
| `--drop-pkg` | off | Remove the PL/SQL package from the database after the purge finishes. |
| `--drop-logs` | off | Remove the audit log tables (`epf_purge_log`, `epf_purge_space_snapshot`). They will be recreated on next run. |
| `--sys-password PASS` | *(prompted)* | Password for SYS (only needed for `--optimize-db` and `--reclaim`). |
| `--assume-yes` / `-y` | off | Skip all confirmation prompts. For automated/scheduled runs. |
| `--config FILE` | *(none)* | Load settings from a config file instead of command-line arguments. |

### Purge Depth Options

| Depth | What Gets Purged |
|-------|-----------------|
| `ALL` | Everything below — payments, logs, and bank statements. |
| `PAYMENTS` | Bulk payments + 20 child tables + file integrations. |
| `LOGS` | Audit trail + audit archive + technical logs. |
| `BANK_STATEMENTS` | File dispatching + directory dispatching records. |

---

## What Gets Purged — All 27 Tables

The tool deletes data from **27 tables** across two schemas (`oppayments` and `op`), organized into three modules. Tables are deleted in a specific order to respect foreign key constraints: **children are always deleted before their parents**.

### PAYMENTS Module — 22 tables

The root table is `bulk_payment`. Everything else is a child or grandchild. The date filter is `bulk_payment.value_date < cutoff`.

```
bulk_payment  ← ROOT (filtered by value_date)
│
├── bulk_payment_additional_info      (direct child)
├── bulk_signature                    (direct child)
├── mandatory_signers                 (direct child)
├── oidc_request_token                (direct child)
├── payment_audit (by bulk_payment_id)(direct child)
├── transmission_execution_audit  [*] (direct child — contains CLOB: message)
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


**Tables with CLOB columns:**

| Table | CLOB Column | Impact |
|-------|------------|--------|
| `transmission_execution_audit` | `message` | Stores XML/text message content. Deleting rows generates extra UNDO for LOB data. |

### LOGS Module — 3 tables

| Table | Schema | Date Filter | Relationship |
|-------|--------|-------------|-------------|
| `audit_archive` | `oppayments` | FK from `audit_trail` | Child — deleted first |
| `audit_trail` | `oppayments` | `audit_timestamp < cutoff` | Parent |
| `spec_trt_log` | **`op`** | `dtlog < cutoff` | Standalone (no FK) |

### BANK_STATEMENTS Module — 2 tables

| Table | Schema | Date Filter | Relationship | CLOB Column |
|-------|--------|-------------|-------------|-------------|
| `directory_dispatching` | `oppayments` | FK from `file_dispatching` | Child — deleted first | `breakdown_content` |
| `file_dispatching` | `oppayments` | `date_reception < cutoff` | Parent | `file_content` |

Both tables contain **CLOB columns** that store file contents and breakdown data. These generate more UNDO per row during deletion than regular tables.

---

## General Execution Flow

When you run the tool, here is what happens step by step:

```
1. PARSE         Parse command-line arguments / load config file / interactive prompts
                           │
2. CHECK         Verify prerequisites: sqlplus available, ORACLE_HOME set, DB connectivity
                           │
3. OPTIMIZE      (Optional, --optimize-db) Enlarge redo logs, gather stats
                           │
4. DEPLOY        Run SQL scripts to create the log table and PL/SQL package
                 01_create_purge_log_table.sql → 02_epf_purge_pkg_spec.sql → 03_epf_purge_pkg_body.sql
                           │
5. VERIFY        Check the package compiled without errors
                           │
6. TUNE UNDO     Lower undo_retention from 900s to 60s (prevents UNDO tablespace from growing to 25GB+)
                           │
7. MONITOR       Start the live progress monitor (background)
                           │
8. PURGE         Execute the purge:
                   • Snapshot segment sizes (BEFORE)
                   • Delete PAYMENTS (21 tables, batch by batch)
                   • Delete FILE_INTEGRATIONS (batch by batch)
                   • Delete AUDIT_LOGS (2 tables, batch by batch)
                   • Delete TECH_LOGS (op.spec_trt_log, batch by batch)
                   • Delete BANK_STATEMENTS (2 tables, batch by batch)
                   • Snapshot segment sizes (AFTER)
                   • Print run summary + space comparison
                           │
9. RESTORE       Stop monitor, restore undo_retention to 900s
                           │
10. RECLAIM      (Optional, --reclaim) Shrink segments + squeeze + resize datafiles
                           │
11. CLEANUP      (Optional) Drop package (--drop-pkg), drop logs (--drop-logs)
                           │
12. LOG          Write timestamped log file to logs/ directory
```

---

## Impact and Safety

### What You Can Expect

| Aspect | Detail |
|--------|--------|
| **Online operation** | The purge runs while the application is live. It uses row-level locks and batched commits — it does not block normal ePF transactions. |
| **Disk space** | After purge, Oracle marks freed space as "reusable" but the datafile stays the same size on disk. Use `--reclaim` to actually shrink the files. |
| **Duration** | Depends on data volume. Dry runs take seconds. Purging ~500K bulk_payments with all dependents takes roughly 1–3 hours. |
| **Redo logs** | The tool generates redo (write-ahead log entries). With `--optimize-db`, redo logs are enlarged to 1 GB to prevent `log file switch` waits. |
| **UNDO tablespace** | Batched commits keep per-transaction UNDO small. The wrapper script automatically tunes `undo_retention` to 60s during purge and restores it to 900s after. |
| **Interruption safety** | If the purge is interrupted (Ctrl+C, crash), already-committed batches are permanent. The current batch rolls back cleanly. Re-run the purge — it picks up where it left off (selects by date, not by state). |
| **Application impact** | Minimal. The purge deletes old data (older than retention period). Active transactions on current data are not affected. Prefer off-hours for very large purges to minimize I/O contention. |

### Safety Features

| Feature | What It Does |
|---------|-------------|
| **Dry run** (`--dry-run`) | Counts rows that *would* be deleted. Deletes nothing. Always run this first. |
| **Batch commits** | Commits every N rows (default 1,000). Keeps transactions small. |
| **UNDO tuning** | Automatically lowers `undo_retention` to prevent UNDO tablespace from growing to 25+ GB. |
| **Per-batch error handling** | If one batch fails, it rolls back that batch and continues with the next. One bad record doesn't abort the entire purge. |
| **Full audit trail** | Every DELETE, every count, every error is logged to `epf_purge_log` with timestamps and row counts. |
| **Autonomous logging** | Log entries use Oracle autonomous transactions — they survive even if the main transaction rolls back. |
| **Cursor cleanup** | All database cursors are explicitly closed on error (prevents resource leaks). |
| **Confirmation prompts** | The wrapper script asks for confirmation before executing. Use `-y` to skip in automated runs. |
| **Privilege separation** | Normal purge runs as `oppayments` (limited privileges). SYS is only used for reclaim/optimization, and only when you explicitly ask for it. |

---

## Checking Results

### Quick Summary After Purge

The tool automatically prints a summary after each run. You can also query it:

```sql
-- Summary of the most recent run
SELECT module, status, SUM(rows_affected) AS total_rows,
       ROUND(SUM(NVL(elapsed_seconds, 0)), 1) AS elapsed_sec
FROM oppayments.epf_purge_log
WHERE run_id = (
    SELECT run_id FROM oppayments.epf_purge_log
    WHERE operation = 'RUN_START'
    ORDER BY log_timestamp DESC
    FETCH FIRST 1 ROW ONLY
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
| **"SQL*Plus not found"** | Install Oracle Client (Instant Client is enough) and add `sqlplus` to your PATH. |
| **Connection failed** | Check TNS name (`tnsping EPFPROD`), credentials, and `tnsnames.ora`. |
| **ORA-30036 (undo full)** | Reduce `--batch-size` to 500 or smaller. Or ask DBA to extend the UNDO tablespace. |
| **Disk space not freed** | Expected — use `--reclaim` to actually shrink the datafiles. See section below. |
| **Purge seems stuck** | Check progress: `SELECT module, action, client_info FROM v$session WHERE module = 'EPF_PURGE';` |
| **Package compilation error** | Check: `SELECT line, text FROM user_errors WHERE name = 'EPF_PURGE_PKG';` — usually a missing privilege. |

---

## How Space Reclamation Works (`--reclaim`)

### The Problem: Why Deleting Data Doesn't Free Disk Space

When you `DELETE` rows in Oracle, the data is removed from the table — but **the file on disk stays the same size**. This surprises everyone the first time.

Think of it like a book where you erase text from pages. The pages are now blank, but the book doesn't get thinner. Oracle marks those blank pages as "reusable" — new data can be written there — but the physical `.dbf` datafile doesn't shrink.

This is because of the **High Water Mark (HWM)** — an internal marker that tracks the *highest block ever used* in the tablespace. Oracle can only resize its datafile down to the HWM. If a single table (or index, or LOB segment) is still using a block near the top, the entire datafile stays that size.

```
BEFORE PURGE — 40 GB datafile, data scattered throughout:

    Block 0          ███░░███░░███████░░░██████░░░░████████      Block 40GB
                     ↑ data      ↑ gap     ↑ data    ↑ HWM (40GB)

AFTER PURGE — Many rows deleted, but HWM unchanged:

    Block 0          ██░░░░░░░░░██░░░░░░░░░░░░░░░░░░░░░████      Block 40GB
                     ↑ data     ↑ gap (lots of free space)  ↑ HWM still 40GB!
                                                               (one segment pinning it)
```

The datafile can't shrink because that small segment at the top (block 40GB) is still there. The `--reclaim` option fixes this.

### The Solution: 3-Phase Reclaim

#### Phase 1 — SHRINK SPACE (compact each table)

For every table in the tablespace, Oracle compacts the rows into fewer blocks and releases unused blocks:

```sql
-- For each table:
ALTER TABLE oppayments.payment ENABLE ROW MOVEMENT;     -- required for shrink
ALTER TABLE oppayments.payment SHRINK SPACE CASCADE;     -- compact table + indexes + LOB segments
```

`CASCADE` means it also shrinks the table's indexes and LOB segments (CLOBs).

**Result:** Each table uses fewer blocks. "Used space" drops significantly. But the HWM doesn't necessarily drop — the data just got compacted within its existing blocks, and the top segment may still be pinning the HWM.

```
AFTER PHASE 1 — Data compacted, but HWM still pinned:

    Block 0          ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░████      Block 40GB
                     ↑ data (compacted)          still pinned ↑ HWM
```

#### Phase 2 — SQUEEZE (lower the HWM by relocating segments)

This is the clever part. The script runs a loop (up to 2,000 iterations):

1. **Find** whichever segment (table, index, or LOB) has extents closest to the top of the tablespace — this is the one "pinning" the HWM.
2. **Squeeze the ceiling** — resize the datafile down to just above the HWM, so Oracle can't place new extents at the top.
3. **Move/Rebuild** that segment so Oracle allocates it lower in the tablespace:
   - Tables: `ALTER TABLE ... MOVE ONLINE TABLESPACE ...`
   - Indexes: `ALTER INDEX ... REBUILD ONLINE`
   - LOB segments: `ALTER TABLE ... MOVE LOB (column) STORE AS (TABLESPACE ...)`
4. **Check** if the HWM dropped. If so, the segment successfully moved lower.
5. **Repeat** with the next top segment.

**A single iteration looks like this (example):**

```sql
-- The PAYMENT_AUDIT index is the top segment at 39.8 GB
-- Squeeze datafile ceiling to just above HWM:
ALTER DATABASE DATAFILE '/path/data01.dbf' RESIZE 40960M;   -- tight ceiling

-- Rebuild the index — Oracle places it in the lower free space:
ALTER INDEX oppayments.PAY_AUDIT_IDX REBUILD ONLINE;

-- HWM drops from 39.8 GB to 38.2 GB (next segment)
-- Next iteration: handle whatever is now at the top (38.2 GB)
```

After many iterations, all segments are packed toward the bottom and the HWM is close to the actual used space:

```
AFTER PHASE 2 — All segments relocated down:

    Block 0          ████████████████░░░░░░░░░░░░░░░░░░░░░░      Block 40GB
                     ↑ everything packed   ↑ HWM now ~15GB     (wasted space)
```

#### Phase 3 — RESIZE (shrink the file on disk)

With the HWM lowered, Oracle can finally shrink the datafile:

```sql
-- HWM is at ~15 GB. Resize to HWM + 50 MB safety margin:
ALTER DATABASE DATAFILE '/path/data01.dbf' RESIZE 15410M;
-- Datafile shrinks from 40 GB to ~15 GB on disk. OS space freed!
```

```
AFTER PHASE 3 — Datafile actually shrunk:

    Block 0          ████████████████|     (end of file)
                     ↑ data          ↑ HWM (15 GB) = file size
                     
                     Freed: 25 GB returned to OS ✓
```

#### Phase 4 — UNDO/TEMP cleanup

Finally, the script tries to shrink the UNDO and TEMP tablespaces too, which may have bloated during the purge. If a simple `RESIZE` fails (scattered extents), it does a full tablespace swap: creates a new UNDO tablespace → switches Oracle to use it → drops the old one.

### Key Points

| Aspect | Detail |
|--------|--------|
| **Online?** | Yes — all operations use `MOVE ONLINE` / `REBUILD ONLINE`. Row-level locks only. Application stays running. |
| **Duration** | Depends on tablespace size and fragmentation. Typically 30 minutes to several hours. |
| **Stall detection** | Every 100 iterations, the script checks if the HWM actually dropped. If it stalls 3 times in a row, it exits gracefully (some segments can't be moved). |
| **LOB handling** | CLOB/LOB segments are detected and moved explicitly via `ALTER TABLE ... MOVE LOB (column)`. |
| **Safety** | If any individual MOVE fails, the script logs the error and continues with the next segment. No data is lost. |
