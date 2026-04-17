# Space Reclamation Guide

## The Problem

After deleting rows from Oracle tables, disk usage does **not** decrease. This is by design: Oracle marks the space as "reusable" within the tablespace, but the underlying `.dbf` datafiles remain the same size.

This means:
- `SELECT COUNT(*)` shows fewer rows
- The OS reports the same disk usage
- New inserts can reuse the freed space, but it doesn't help if your disk is full

## Understanding How Oracle Uses Space

```
Datafile (.dbf)
├── Allocated extents (contain your data)
├── Free extents (space freed by DELETE, reusable by Oracle)
└── Unused space (never allocated, above the High Water Mark)
```

The **High Water Mark (HWM)** is the highest point in the datafile that has ever been used. Even after deleting rows, the HWM stays where it is. Oracle can only resize a datafile down to the HWM.

## Reclamation Methods

### Tier 1: SHRINK SPACE (Lightweight)

**What it does:** Compacts data within segments, moving rows to fill gaps, then lowers the HWM. Space is returned to the tablespace (not the OS).

**When to use:** After routine purges. Online, no downtime required.

```sql
-- Enable row movement (required for SHRINK)
ALTER TABLE oppayments.bulk_payment ENABLE ROW MOVEMENT;

-- Shrink the table and all dependent segments (indexes, LOBs)
ALTER TABLE oppayments.bulk_payment SHRINK SPACE CASCADE;
```

**Requirements:**
- Tablespace must use ASSM (Automatic Segment Space Management)
- Table cannot have function-based indexes on ROWID
- Brief row-level locks during compaction (minimal impact)

**Using the package:**
```sql
-- The package handles this automatically
BEGIN
    oppayments.epf_purge_pkg.run_purge(
        p_retention_days => 90,
        p_reclaim_space  => TRUE
    );
END;
```

### Tier 2: COALESCE Tablespace

**What it does:** Merges adjacent free extents into larger contiguous blocks. Useful for dictionary-managed tablespaces (for locally-managed tablespaces, Oracle does this automatically).

```sql
ALTER TABLESPACE OPPAYMENTS_DATA COALESCE;
```

**When to use:** Only if you're on dictionary-managed tablespaces (rare in modern Oracle).

### Tier 3: RESIZE Datafiles (Returns Disk Space to OS)

**What it does:** Shrinks the actual `.dbf` file on the OS filesystem. This is the **only method that reduces disk usage** as reported by the OS (short of dropping the tablespace).

```sql
-- Check current size vs high water mark
SELECT file_name,
       ROUND(bytes / 1048576) AS current_mb,
       ROUND((bytes - NVL(free_bytes, 0)) / 1048576) AS used_mb
FROM dba_data_files df
LEFT JOIN (
    SELECT file_id, SUM(bytes) AS free_bytes
    FROM dba_free_space
    GROUP BY file_id
) fs ON fs.file_id = df.file_id
WHERE df.tablespace_name = 'OPPAYMENTS_DATA';

-- Resize to a specific size (must be above HWM)
ALTER DATABASE DATAFILE '/path/to/oppayments01.dbf' RESIZE 500M;
```

**Requirements:**
- DBA privileges
- Target size must be above the current HWM
- Run SHRINK SPACE first to lower the HWM

**Using the package:**
```sql
BEGIN
    oppayments.epf_purge_pkg.reclaim_space(
        p_run_id           => SYS_GUID(),
        p_shrink_tables    => TRUE,   -- Tier 1: lower the HWM first
        p_coalesce_ts      => FALSE,
        p_resize_datafiles => TRUE    -- Tier 3: then shrink the files
    );
END;
```

### Tier 4: Tablespace Reclaim via Export/Import (Recommended for Full Reclamation)

**What it does:** Exports all schemas sharing the tablespace, drops the entire tablespace, recreates it as a bigfile autoextending tablespace, and reimports all data. This gives you a perfectly compacted tablespace with modern best-practice configuration.

**When to use:** When Tiers 1-3 are insufficient to reclaim enough space, or when you want to restructure the tablespace (e.g., convert to BIGFILE, enable AUTOEXTEND). This is the **recommended approach** for significant space reclamation.

**How it works:**

1. **Discover** the default tablespace used by OPPAYMENTS
2. **Find all schemas** sharing that tablespace (usually DATA tablespace)
3. **Detect PDB/non-PDB** environment (checks `CON_ID`) and whether the tablespace is the database/PDB default
4. **Detect Data Pump directory** (PDB_DATA_PUMP_DIR for PDB, DATA_PUMP_DIR for non-PDB)
5. **Export** all affected schemas via `expdp`
6. **Create holding tablespace** (`DATA_HOLD` or `<TABLESPACE>_HOLD`) -- temporary tablespace to safely hold user assignments during the drop
7. **Switch database/PDB default** -- if the tablespace is the default, runs `ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE <HOLD>` (PDB) or `ALTER DATABASE DEFAULT TABLESPACE <HOLD>` (non-PDB)
8. **Reassign all users** to the holding tablespace
9. **Drop** the original tablespace (`INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS`)
10. **Recreate** the tablespace as `BIGFILE` with `AUTOEXTEND ON`
11. **Restore database/PDB default** tablespace assignment
12. **Reassign all users** back to the recreated tablespace
13. **Import** all schemas via `impdp`
14. **Verify** all objects are valid and recompile if needed
15. **Drop holding tablespace** -- cleanup

**Using the tool:**

```bash
# Linux
./bin/epf_tablespace_reclaim.sh --tns EPFPROD --sys-password MyPassword

# Windows
bin\epf_tablespace_reclaim.bat --tns EPFPROD --sys-password MyPassword
```

Or run interactively (prompts for all inputs):

```bash
./bin/epf_tablespace_reclaim.sh
```

**Requirements:**
- Application **downtime** is required
- SYS or DBA-privileged user credentials
- `expdp` and `impdp` (Oracle Data Pump) on PATH
- Sufficient disk space for the export dump file
- Data Pump directory configured (auto-detected; see below)

**Data Pump directory detection:**

The tool automatically checks for Data Pump directories in this order:
1. `PDB_DATA_PUMP_DIR` -- present in PDB (Pluggable Database) environments
2. `DATA_PUMP_DIR` -- present in non-PDB / CDB root environments
3. If neither exists, the tool prompts for a filesystem path and creates `DATA_PUMP_DIR`

**New tablespace properties:**

| Property | Value |
|----------|-------|
| Type | BIGFILE (single large datafile, simplifies management) |
| Initial size | 10G (configurable via `--datafile-size`) |
| Autoextend | ON |
| Autoextend increment | 1G (configurable via `--autoextend-next`) |
| Max size | UNLIMITED (configurable via `--autoextend-max`) |
| Extent management | LOCAL |
| Segment space management | AUTO (ASSM) |

**PDB vs non-PDB handling:**

The tool automatically detects whether it is running in a PDB (Pluggable Database) or standalone/CDB root environment by checking `SYS_CONTEXT('USERENV', 'CON_ID')`. If the tablespace being reclaimed is the **database/PDB default tablespace** (common with the DATA tablespace), the tool uses a **holding tablespace** approach:

1. Creates a temporary tablespace (`<TABLESPACE>_HOLD`, e.g., `DATA_HOLD`)
2. In PDB: runs `ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE DATA_HOLD`
3. In non-PDB: runs `ALTER DATABASE DEFAULT TABLESPACE DATA_HOLD`
4. Reassigns all users to the holding tablespace
5. Drops, recreates, and restores the original tablespace
6. Drops the holding tablespace

This is necessary because Oracle will not allow dropping a tablespace that is the database default while users still reference it.

**Advantages over Tiers 1-3:**
- Guaranteed full space reclamation to OS
- Tablespace is restructured with modern settings (BIGFILE, AUTOEXTEND, ASSM)
- Clean segment layout with no fragmentation
- All objects reimported with fresh storage allocation

**Downsides:**
- Requires application downtime
- Requires DBA/SYSDBA privileges
- Dump file requires temporary disk space (keep until verified)
- Longer execution time for large datasets

**Safety notes:**
- The tool confirms the plan before making any changes
- Export is performed **before** any destructive operations
- If the import fails, data can be recovered from the dump file
- The tool verifies all objects after import and recompiles invalid ones
- The holding tablespace ensures no user assignment conflicts during the drop

## Recommended Approach

For **routine purges** (weekly/monthly):
1. Run the purge package
2. Enable `p_reclaim_space => TRUE` (Tier 1 SHRINK)
3. Space is freed within Oracle for reuse

For **disk pressure situations** (Tiers 1-3):
1. Run the purge package
2. Run SHRINK SPACE (Tier 1)
3. Run RESIZE on datafiles (Tier 3) -- requires DBA
4. If still not enough, use Tier 4 (Tablespace Reclaim)

For **full space reclamation** (Tier 4 -- recommended):
1. Run the purge package first to remove expired data
2. Schedule a maintenance window (application downtime required)
3. Run the tablespace reclaim tool:
   ```bash
   ./bin/epf_tablespace_reclaim.sh --tns EPFPROD --sys-password MyPassword
   ```
4. Verify application functionality after import
5. Keep the dump file until fully verified

## Checking Space Usage

### Before/After Comparison

```sql
-- Table-level space usage
SELECT segment_name,
       ROUND(SUM(bytes) / 1048576, 2) AS size_mb
FROM user_segments
WHERE segment_name IN (
    'BULK_PAYMENT', 'PAYMENT', 'PAYMENT_AUDIT',
    'AUDIT_TRAIL', 'AUDIT_ARCHIVE', 'FILE_DISPATCHING'
)
GROUP BY segment_name
ORDER BY size_mb DESC;

-- Tablespace-level free space
SELECT tablespace_name,
       ROUND(SUM(bytes) / 1048576, 2) AS free_mb
FROM user_free_space
GROUP BY tablespace_name;

-- Datafile sizes (DBA only)
SELECT file_name,
       ROUND(bytes / 1048576, 2) AS size_mb
FROM dba_data_files
WHERE tablespace_name LIKE 'OPPAYMENTS%';
```

## FAQ

**Q: Why doesn't DELETE free disk space?**
A: Oracle keeps the allocated space for future inserts. This avoids expensive file operations on every delete and improves performance for databases that frequently insert/delete.

**Q: Is SHRINK SPACE safe to run in production?**
A: Yes, but it acquires brief row-level locks during compaction. Run during low-traffic periods. It's fully online -- no downtime required.

**Q: What if SHRINK SPACE fails with ORA-10631?**
A: Your tablespace uses MSSM (Manual Segment Space Management) instead of ASSM. Use the Tablespace Reclaim tool (Tier 4) which recreates the tablespace with ASSM enabled.

**Q: How do I know if my tablespace uses ASSM?**
```sql
SELECT tablespace_name, segment_space_management
FROM dba_tablespaces
WHERE tablespace_name LIKE 'OPPAYMENTS%';
-- Should show 'AUTO' for ASSM
```

**Q: Why does the tablespace reclaim tool export ALL schemas on the tablespace, not just OPPAYMENTS?**
A: Because dropping a tablespace removes ALL data stored in it. Any schema whose default tablespace is the one being dropped would lose its data. The tool discovers and exports all affected schemas to ensure nothing is lost.

**Q: What if the import fails partway through?**
A: The dump file is your rollback. You can re-run `impdp` manually against the dump file. The tablespace reclaim tool uses `TABLE_EXISTS_ACTION=REPLACE` so partial imports can be safely rerun.

**Q: What is a BIGFILE tablespace?**
A: A tablespace backed by a single large datafile (up to 128TB with 32K block size). It simplifies datafile management -- no need to add multiple datafiles as the database grows. Combined with AUTOEXTEND, the tablespace grows automatically as needed.
