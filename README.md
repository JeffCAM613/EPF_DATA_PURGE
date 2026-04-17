# EPF Data Purge Tool

Automated purge tool for the ePF (electronic Payment Factory) Oracle database. Removes expired data from the `oppayments` schema based on configurable retention policies, with full audit logging and optional space reclamation.

## Prerequisites

- **Oracle SQL*Plus** installed and on PATH
- **ORACLE_HOME** environment variable set (recommended)
- **Database user** with the following privileges:
  - `DELETE` on all `oppayments` tables listed below
  - `CREATE TABLE` (for the audit log table)
  - `CREATE PROCEDURE` (for the PL/SQL package)
  - `DBA` role (only if using datafile resize in space reclamation)

## Quick Start

### Windows
```cmd
bin\epf_purge.bat
```

### Linux
```bash
chmod +x bin/epf_purge.sh
./bin/epf_purge.sh
```

The wrapper script will prompt you for all required inputs with explanations.

### Dry Run (Recommended First Step)

Always run a dry run first to see how many rows would be deleted without actually deleting anything:

```bash
# Linux
./bin/epf_purge.sh --tns EPFPROD --user oppayments --retention 90 --dry-run

# Windows
bin\epf_purge.bat --tns EPFPROD --user oppayments --retention 90 --dry-run
```

## Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--tns NAME` | *(required)* | Oracle TNS name or connect string (e.g., `EPFPROD`, `localhost:1521/orcl`) |
| `--user NAME` | `oppayments` | Database username |
| `--password PASS` | *(prompted)* | Database password (prefer `EPF_PURGE_PASSWORD` env var) |
| `--retention N` | `30` | Purge data older than N days |
| `--depth DEPTH` | `ALL` | Which modules to purge (see below) |
| `--batch-size N` | `1000` | Rows per batch commit. Larger = faster, more undo space |
| `--dry-run` | off | Count rows only, no actual deletes |
| `--reclaim` | off | Reclaim space after purge (SHRINK + COALESCE + RESIZE if DBA) |
| `--drop-pkg` | off | Drop the PL/SQL package after execution |
| `--config FILE` | *(none)* | Load settings from config file |

### Purge Depth Options

| Depth | Modules Purged |
|-------|---------------|
| `ALL` | Payments + Logs + Bank Statements (everything) |
| `PAYMENTS` | Bulk payments, individual payments, and file integrations |
| `LOGS` | Audit trails, audit archives, and technical logs |
| `BANK_STATEMENTS` | Bank statement file and directory dispatching |

## What Gets Purged

### PAYMENTS Module (14 tables)

Deletes bulk payments and all dependent child records based on `bulk_payment.value_date`:

| Table | Relationship |
|-------|-------------|
| `bulk_payment` | Root table |
| `bulk_payment_additional_info` | Direct child of bulk_payment |
| `payment` | Direct child of bulk_payment |
| `payment_additional_info` | Child of payment |
| `payment_audit` | Child of both bulk_payment and payment |
| `import_audit` | Direct child of bulk_payment |
| `transmission_execution` | Direct child of bulk_payment |
| `transmission_execution_audit` | Direct child of bulk_payment |
| `transmission_exception` | Direct child of bulk_payment |
| `notification_execution` | Direct child of bulk_payment |
| `workflow_execution` | Child of payment |
| `approbation_execution` | Child of workflow_execution |
| `file_integration` | Based on `integration_date` |

### LOGS Module (3 tables)

| Table | Date Column |
|-------|------------|
| `audit_trail` | `audit_timestamp` |
| `audit_archive` | Via `audit_trail.audit_archive_id` |
| `op.spec_trt_log` | `dtlog` |

### BANK_STATEMENTS Module (2 tables)

| Table | Date Column |
|-------|------------|
| `file_dispatching` | `date_reception` |
| `directory_dispatching` | Via `file_dispatching` join |

## Purge Log (Audit Trail)

All purge operations are recorded in `oppayments.epf_purge_log`. This table is created automatically on first run and is **not** dropped when the package is removed.

### Querying the Log

```sql
-- Summary of the most recent run
SELECT module, status, SUM(rows_affected) AS total_rows,
       COUNT(*) AS operations,
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

-- Detailed view of a specific run
SELECT log_timestamp, module, operation, table_name,
       rows_affected, status, message
FROM oppayments.epf_purge_log
WHERE run_id = HEXTORAW('your_run_id_here')
ORDER BY log_timestamp;

-- Error history
SELECT log_timestamp, module, table_name, error_code, error_message
FROM oppayments.epf_purge_log
WHERE status = 'ERROR'
ORDER BY log_timestamp DESC;
```

### Log Table Columns

| Column | Description |
|--------|------------|
| `log_id` | Auto-increment primary key |
| `run_id` | GUID linking all entries from one execution |
| `log_timestamp` | When the entry was recorded |
| `module` | PAYMENTS, FILE_INTEGRATION, AUDIT_LOGS, TECH_LOGS, BANK_STATEMENTS, SPACE_RECLAIM |
| `operation` | DELETE, DRY_RUN_COUNT, SHRINK_SPACE, COALESCE, RESIZE, RUN_START, RUN_END |
| `table_name` | Fully qualified table name affected |
| `rows_affected` | Number of rows deleted in this operation |
| `batch_number` | Batch sequence number within a module |
| `retention_days` | Retention parameter used for this run |
| `status` | SUCCESS, ERROR, WARNING, INFO |
| `message` | Descriptive text |
| `error_code` | Oracle SQLCODE on errors |
| `error_message` | Oracle SQLERRM on errors |
| `elapsed_seconds` | Duration of this operation |

### Space Snapshot Table: `oppayments.epf_purge_space_snapshot`

Automatically captures segment sizes before and after each purge run.

| Column | Description |
|--------|------------|
| `snapshot_id` | Auto-increment primary key |
| `run_id` | Links to the purge run |
| `snapshot_phase` | BEFORE or AFTER |
| `snapshot_timestamp` | When the snapshot was taken |
| `owner` | Schema owner |
| `segment_name` | Oracle segment name (table, index, or LOB segment) |
| `segment_type` | TABLE, INDEX, LOBSEGMENT, etc. |
| `parent_table` | For LOB segments, the parent table name; otherwise same as segment_name |
| `size_bytes` | Segment size in bytes |
| `size_mb` | Segment size in MB |

## Example Output

### Dry Run
```
============================================================
  EPF DATA PURGE
  Run ID:     A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4
  Depth:      ALL
  Retention:  90 days
  Cutoff:     2025-12-19
  Batch size: 1000
  Dry run:    YES
============================================================
[DRY RUN] bulk_payment: 4523, payment: 28450 (and dependents) would be deleted
[DRY RUN] file_integration: 1205 rows would be deleted
[DRY RUN] audit_trail: 156780, audit_archive: 156780 rows would be deleted
[DRY RUN] op.spec_trt_log: 89432 rows would be deleted
[DRY RUN] directory_dispatching: 3400, file_dispatching: 1700 rows would be deleted

============================================================
  EPF PURGE RUN SUMMARY
  Run ID: A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4
============================================================
  Started:  2026-03-19 14:30:00
  Finished: 2026-03-19 14:30:05
  Duration: 5.0s
------------------------------------------------------------
  AUDIT_LOGS            Rows:          0  Errors: 0  Warnings: 0  Time: 1.2s
  BANK_STATEMENTS       Rows:          0  Errors: 0  Warnings: 0  Time: 0.8s
  FILE_INTEGRATION      Rows:          0  Errors: 0  Warnings: 0  Time: 0.3s
  PAYMENTS              Rows:          0  Errors: 0  Warnings: 0  Time: 2.1s
  TECH_LOGS             Rows:          0  Errors: 0  Warnings: 0  Time: 0.6s
------------------------------------------------------------
  TOTAL ROWS DELETED: 0
============================================================
```

### Actual Purge
```
============================================================
  EPF DATA PURGE
  Run ID:     F7E8D9C0B1A2F7E8D9C0B1A2F7E8D9C0
  Depth:      ALL
  Retention:  90 days
  Cutoff:     2025-12-19
  Batch size: 1000
  Dry run:    NO
============================================================
=== Bulk Payments Purge Summary ===
  bulk_payment:                    4,523
  bulk_payment_additional_info:    4,523
  payment:                         28,450
  payment_additional_info:         28,450
  payment_audit (by bp_id):        4,523
  payment_audit (by payment_id):   28,450
  import_audit:                    4,523
  transmission_execution_audit:    4,523
  transmission_execution:          4,523
  transmission_exception:          312
  notification_execution:          4,523
  workflow_execution:              28,450
  approbation_execution:           56,900
file_integration: 1,205 rows deleted
audit_trail: 156,780, audit_archive: 156,780 rows deleted
op.spec_trt_log: 89,432 rows deleted
directory_dispatching: 3,400, file_dispatching: 1,700 rows deleted

============================================================
  SPACE USAGE COMPARISON (Before vs After Purge)
  Run ID: F7E8D9C0B1A2F7E8D9C0B1A2F7E8D9C0
============================================================
Segment / Table                        Before(MB)   After(MB)   Freed(MB) Freed%
----------------------------------------------------------------------------------
SYS_LOB0000076469C00004$$                7,987.20    6,102.40    1,884.80  23.6%
SYS_LOB0000076465C00004$$                4,218.88    3,450.11      768.77  18.2%
PAYMENT_ADDITIONAL_INFO                  3,778.56    2,100.22    1,678.34  44.4%
DIRECTORY_DISPATCHING                    2,631.68    1,800.50      831.18  31.6%
PAY_ADD_PAY_KEY_INDX                     2,314.24    1,500.00      814.24  35.2%
PAYMENT                                  2,109.44    1,200.33      909.11  43.1%
PAYMENT_AUDIT                            1,474.56      820.10      654.46  44.4%
...
----------------------------------------------------------------------------------
TOTAL (OPPAYMENTS)                      34,910.27   24,150.80   10,759.47  30.8%
============================================================
NOTE: Segment sizes reflect allocated space, not actual data.
Space freed by DELETE is reusable by Oracle but does not
reduce OS disk usage. Use --reclaim or SHRINK SPACE to
compact segments, then RESIZE datafiles to free OS disk.
For full disk reclamation, use the tablespace reclaim tool.
============================================================
```

All purge output (including DBMS_OUTPUT from the PL/SQL package) streams live to the console in real time, so you can monitor progress without querying the database separately.

The space comparison is automatic — it snapshots all OPPAYMENTS segments (tables, indexes, and **LOB segments resolved to their parent table**) before and after purge. LOB segments like `SYS_LOB...` are mapped back to their parent table name via `dba_lobs`, so you can see the full picture including LOB storage impact.

The snapshot data is also stored in `oppayments.epf_purge_space_snapshot` for querying:

```sql
-- Compare before/after for a specific run
SELECT b.parent_table,
       b.total_mb AS before_mb,
       a.total_mb AS after_mb,
       b.total_mb - a.total_mb AS freed_mb
FROM (
    SELECT parent_table, SUM(size_mb) AS total_mb
    FROM oppayments.epf_purge_space_snapshot
    WHERE run_id = HEXTORAW('your_run_id') AND snapshot_phase = 'BEFORE'
    GROUP BY parent_table
) b
LEFT JOIN (
    SELECT parent_table, SUM(size_mb) AS total_mb
    FROM oppayments.epf_purge_space_snapshot
    WHERE run_id = HEXTORAW('your_run_id') AND snapshot_phase = 'AFTER'
    GROUP BY parent_table
) a ON b.parent_table = a.parent_table
ORDER BY freed_mb DESC NULLS LAST;
```

## Configuration File

For automated/scheduled runs, copy the example config:

```bash
cp config/epf_purge.conf.example config/epf_purge.conf
# Edit config/epf_purge.conf with your settings
```

Then run with:
```bash
./bin/epf_purge.sh --config config/epf_purge.conf
```

Set the password via environment variable (never store passwords in config files):
```bash
export EPF_PURGE_PASSWORD='your_password'
```

See [config/epf_purge.conf.example](config/epf_purge.conf.example) for all available settings.

## Space Reclamation

### Lightweight Reclamation (--reclaim flag)

After purging, Oracle marks freed space as reusable but does **not** shrink the datafiles. The `--reclaim` flag handles this automatically by running all three tiers in sequence:

1. **SHRINK SPACE CASCADE** — Compacts data within each table segment (including LOBs and indexes). Online, no downtime.
2. **COALESCE** — Merges adjacent free extents in the tablespace.
3. **RESIZE datafiles** — Actually shrinks the `.dbf` files on disk, returning space to the OS. **Requires DBA privileges** — if the user doesn't have them, this step is skipped gracefully with a warning (Tiers 1-2 still complete).

```bash
# Purge + lightweight space reclamation
./bin/epf_purge.sh --tns SONEPAR_ANM --user oppayments --retention 90 --reclaim
```

### Full Tablespace Reclaim (Recommended for Disk Pressure)

When lightweight reclamation is insufficient, the **tablespace reclaim tool** performs a full export/drop/recreate/import cycle. This guarantees full disk space recovery and restructures the tablespace with modern settings (BIGFILE, AUTOEXTEND, ASSM).

This is a **standalone tool** -- it does not require running the purge first. You can use it independently whenever you need to reclaim disk space, even without deleting any data.

**What it does:**
1. Discovers the tablespace used by OPPAYMENTS (typically DATA)
2. Finds **all schemas** sharing that tablespace
3. Detects PDB/non-PDB environment and whether the tablespace is the database default
4. Detects the Data Pump directory (PDB_DATA_PUMP_DIR or DATA_PUMP_DIR)
5. Exports all schemas via `expdp`
6. Creates a temporary **holding tablespace** for safe user reassignment
7. Switches the database/PDB default tablespace to the holding TS (if applicable)
8. Reassigns all users to the holding tablespace
9. Drops the original tablespace (`INCLUDING CONTENTS AND DATAFILES`)
10. Recreates it as a `BIGFILE` tablespace with `AUTOEXTEND ON`
11. Restores the database/PDB default and reassigns all users back
12. Imports all schemas via `impdp`
13. Verifies all objects, recompiles invalid ones, drops the holding tablespace

```bash
# Linux
./bin/epf_tablespace_reclaim.sh --tns EPFPROD --sys-password MyPassword

# Windows
bin\epf_tablespace_reclaim.bat --tns EPFPROD --sys-password MyPassword

# Or run interactively (prompts for everything)
./bin/epf_tablespace_reclaim.sh
```

**Requirements:**
- Application **downtime** required
- SYS or DBA-privileged credentials
- `expdp` / `impdp` on PATH
- Sufficient disk space for the dump file

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--tns NAME` | *(required)* | Oracle TNS name or connect string |
| `--dba-user NAME` | `sys` | DBA username |
| `--dba-password PASS` | *(prompted)* | DBA password (prefer `EPF_DBA_PASSWORD` env var) |
| `--sys-password PASS` | — | Shortcut: sets `--dba-user=sys` + password |
| `--oppayments-user NAME` | `oppayments` | OPPAYMENTS schema name |
| `--datafile-path PATH` | *(auto-detected)* | Path for the new datafile |
| `--datafile-size SIZE` | `10G` | Initial datafile size |
| `--autoextend-next SIZE` | `1G` | Autoextend increment |
| `--autoextend-max SIZE` | `UNLIMITED` | Maximum datafile size |

See [docs/space_reclamation_guide.md](docs/space_reclamation_guide.md) for detailed background on how Oracle manages space.

## Project Structure

```
EPF_DATA_PURGE/
├── README.md                              # This file
├── .gitignore
├── sql/
│   ├── 01_create_purge_log_table.sql      # Audit log table DDL
│   ├── 02_epf_purge_pkg_spec.sql          # PL/SQL package specification
│   ├── 03_epf_purge_pkg_body.sql          # PL/SQL package body (all purge logic)
│   ├── 04_drop_epf_purge_pkg.sql          # Package cleanup script
│   └── legacy/                            # Original French scripts (reference only)
├── bin/
│   ├── epf_purge.sh                       # Linux/Unix purge wrapper script
│   ├── epf_purge.bat                      # Windows purge wrapper script
│   ├── epf_tablespace_reclaim.sh          # Linux/Unix tablespace reclaim script
│   └── epf_tablespace_reclaim.bat         # Windows tablespace reclaim script
├── config/
│   └── epf_purge.conf.example             # Example configuration file
├── logs/                                  # Runtime log files (gitignored)
└── docs/
    └── space_reclamation_guide.md         # Tablespace management guide
```

## Troubleshooting

### ORA-01555: snapshot too old
Reduce `--batch-size` (e.g., 500 or 100). This error occurs when the undo tablespace is too small for the batch.

### ORA-10631: SHRINK SPACE on non-ASSM tablespace
Your tablespace uses Manual Segment Space Management. SHRINK SPACE only works with ASSM. Check with:
```sql
SELECT tablespace_name, segment_space_management
FROM dba_tablespaces WHERE tablespace_name LIKE 'OPPAYMENTS%';
```

### Package compilation errors
Check `USER_ERRORS`:
```sql
SELECT line, text FROM user_errors WHERE name = 'EPF_PURGE_PKG' ORDER BY sequence;
```

### Connection issues
- Verify TNS name: `tnsping EPFPROD`
- Check listener: `lsnrctl status`
- Test manually: `sqlplus oppayments/password@EPFPROD`

### Slow purge performance
- Increase `--batch-size` (try 5000 or 10000)
- Ensure indexes exist on foreign key columns
- Run during off-peak hours
- Check for locks: `SELECT * FROM v$lock WHERE type = 'TX';`

## Direct PL/SQL Usage

If you prefer to manage the package manually without the wrapper scripts:

```sql
-- Deploy (run these in order via sqlplus)
@sql/01_create_purge_log_table.sql
@sql/02_epf_purge_pkg_spec.sql
@sql/03_epf_purge_pkg_body.sql

-- Execute
SET SERVEROUTPUT ON SIZE UNLIMITED
BEGIN
    oppayments.epf_purge_pkg.run_purge(
        p_retention_days => 90,
        p_purge_depth    => 'ALL',
        p_batch_size     => 1000,
        p_reclaim_space  => FALSE,
        p_dry_run        => FALSE
    );
END;
/

-- Or run individual modules
DECLARE
    l_run_id RAW(16) := SYS_GUID();
BEGIN
    oppayments.epf_purge_pkg.purge_bulk_payments(
        p_run_id      => l_run_id,
        p_cutoff_date => TRUNC(SYSDATE - 90),
        p_batch_size  => 1000,
        p_dry_run     => FALSE
    );
    oppayments.epf_purge_pkg.print_run_summary(l_run_id);
END;
/

-- Cleanup
@sql/04_drop_epf_purge_pkg.sql
```
