# EPF Data Purge — Improvement Plan

Working document for the review/improvement pass started 2026-04-25.
Update statuses inline as phases land. Test-checklist answers go directly under each checklist item.

---

## Decisions confirmed

| # | Decision | Choice |
|---|----------|--------|
| 1 | Per-table comparison rows | All 27 purged tables, **always**, grouped by module (PAYMENTS, LOGS, BANK_STATEMENTS), then sorted by table name ASC inside each group |
| 2 | Filter table list by depth | If `--depth` ≠ ALL, only show that module's tables. If ALL, show all 3 modules. |
| 3 | Sub-totals in comparison | One sub-total per shown module + grand total for the shown modules + tablespace-wide total at the bottom |
| 4 | SHRINK_PROGRESS cadence | Every 10 tables OR every 60s elapsed, whichever comes first |
| 5 | Bash monitor split | Split into `bin/epf_monitor.sh` mirroring `bin/epf_monitor.ps1` so .sh and .bat behave the same way |
| 6 | Phase 4 polishes | All 4 items approved (config grouping, drop duplicate header, `EPF_SYS_PASSWORD`, post-fail comparison warning) |
| 7 | `op.spec_trt_log` | Explicitly include in snapshot capture (it lives in `op` schema and may be in a different tablespace than OPPAYMENTS) |

---

## Phase 1 — Reclaim live logging reliability (Linux/parity)  — **DONE**

**Goal:** make Linux runs stream reclaim progress live, the same way Windows already does, with no silent failure modes.

**Files changed:**
- new [bin/epf_monitor.sh](bin/epf_monitor.sh) — port of [bin/epf_monitor.ps1](bin/epf_monitor.ps1), positional args `CONN_STR POLL_SEC MAX_WAIT_MIN LOG_FILE`
- [bin/epf_purge.sh](bin/epf_purge.sh) `start_monitor`/`stop_monitor` — invoke external script instead of inline subshell
- [bin/epf_purge.sh](bin/epf_purge.sh) `restore_undo_post_purge` — capture sqlplus output, verify `undo_retention=900`, warn on failure
- [bin/epf_purge.bat](bin/epf_purge.bat) post-purge undo restore — same fix
- [bin/epf_monitor.ps1](bin/epf_monitor.ps1) — added `SHRINK_PROGRESS` operation, distinguished `RUN_END`/`RECLAIM_END` ERROR vs SUCCESS
- [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql) Phase 1 loop — emit `SHRINK_PROGRESS` every 10 tables OR 60s; removed buffered per-table `SHRINK OK` print
- [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql) — top-level `EXCEPTION WHEN OTHERS` emits `RECLAIM_END status=ERROR` then re-raises

### Changes

1. **Split bash monitor into [bin/epf_monitor.sh](bin/epf_monitor.sh)** mirroring the contract of [bin/epf_monitor.ps1](bin/epf_monitor.ps1):
   - Same args: `ConnStr`, `PollSec`, `MaxWaitMin`, `LogFile`.
   - Same exit conditions: `RECLAIM_END`, top-level `ORCHESTRATOR ERROR`, idle timeout. Does **not** exit on `RUN_END` — keeps polling for reclaim entries.
   - Same formatter cases (RUN_START, RUN_END, RECLAIM_START, SHRINK_DONE, SHRINK_PROGRESS [new], SQUEEZE_PROGRESS, RECLAIM_END, DELETE batch, DELETE per-table total, ERROR, INFO/INIT/DRY_RUN_COUNT).
   - Same "newer run_id detected → switch" logic.
2. **Update [bin/epf_purge.sh](bin/epf_purge.sh)**'s `start_monitor` / `stop_monitor`:
   - Launch `epf_monitor.sh` as a background process; mirror the PowerShell launch shape in [bin/epf_purge.bat](bin/epf_purge.bat).
   - On exit/teardown, wait up to 60s for graceful exit, then kill.
3. **Add `SHRINK_PROGRESS` log emission in [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql)**:
   - During Phase 1's table loop, call `reclaim_log('SHRINK_PROGRESS', 'INFO', 'Shrunk N/M tables...', elapsed)` every 10 tables OR 60s elapsed (whichever first).
   - Track wallclock + counter inside the loop; pick the first trigger to fire.
4. **Top-level reclaim error handler**:
   - Wrap the main BEGIN/END block in [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql) so any unhandled exception emits `RECLAIM_END` with `status='ERROR'` before re-raising. Currently the monitor can hang at idle-timeout if reclaim crashes before hitting the success path.
5. **Fix `restore_undo_post_purge` silent failure** ([bin/epf_purge.sh:723-727](bin/epf_purge.sh#L723-L727)): drop `>/dev/null 2>&1`, capture and log the result, warn loudly if `undo_retention` was not restored. Mirror in `.bat`.

### Test checklist (Phase 1)

| # | Check | Observation |
|---|-------|-------------|
| 1.1 | On Linux, run `--reclaim` end-to-end. Watch live output: see RECLAIM_START, periodic SHRINK_PROGRESS, periodic SQUEEZE_PROGRESS, RECLAIM_END. No silent gap >120s during a long reclaim. | _to fill_ |
| 1.2 | On Windows, same test. Output should match Linux line-for-line (modulo timestamps). | _to fill_ |
| 1.3 | Compare the wrapper's stdout vs the contents of `logs/epf_purge_<ts>.log` — they should be identical (same lines, same order). | _to fill_ |
| 1.4 | Force reclaim to fail (e.g., revoke `ALTER DATABASE` mid-run, or run with a tablespace that doesn't exist). Confirm: monitor shows `RECLAIM_END ERROR` and exits within one poll interval; wrapper exits cleanly; log captures the error. | _to fill_ |
| 1.5 | Run a 2-minute purge with `--reclaim`. Confirm `undo_retention` is back to 900 after. (`SELECT value FROM v$parameter WHERE name = 'undo_retention';`) | _to fill_ |
| 1.6 | Kill the wrapper mid-purge with Ctrl+C. Confirm: monitor process dies (no orphans). | _to fill_ |
| 1.7 | Run `--reclaim-only` (no purge). Confirm: monitor still attaches to the new RECLAIM_START and shows live progress; exits on RECLAIM_END. | _to fill_ |

---

## Phase 2 — Affected-tables-only space comparison

**Goal:** Replace the noisy full-tablespace dump with a focused per-module breakdown that matches the depth the user actually purged. Always show overall tablespace impact at the bottom.

### Changes

1. **[sql/03_epf_purge_pkg_body.sql](sql/03_epf_purge_pkg_body.sql) `capture_space_snapshot`**:
   - Keep capturing all OPPAYMENTS-tablespace segments (useful for diagnostics + tablespace total).
   - **Add** explicit capture for `op.spec_trt_log` (and any of its LOB/index segments) regardless of tablespace, since LOGS depth includes it.
   - Add a column or marker to identify "purged-table" rows vs "other" — simplest: add a `module` column (`'PAYMENTS'|'LOGS'|'BANK_STATEMENTS'|'OTHER'`) populated using the existing `get_purged_tables()` mapping. Populated at insert time.

   *Schema impact:* one new nullable column on `epf_purge_space_snapshot`. Migration step in [sql/01_create_purge_log_table.sql](sql/01_create_purge_log_table.sql) (idempotent ALTER TABLE … ADD COLUMN IF NOT EXISTS) so existing installs upgrade cleanly.

2. **[sql/03_epf_purge_pkg_body.sql](sql/03_epf_purge_pkg_body.sql) `print_space_comparison`**:
   - New signature: `print_space_comparison(p_run_id RAW, p_depth VARCHAR2)`.
   - Output structure (depth-aware):

     ```
     ============================================================
       SPACE USAGE COMPARISON (Before vs After Purge + Reclaim)
       Run ID: <hex>      Depth: <depth>
     ============================================================
     Owner.Table                              Before(MB)    After(MB)    Freed(MB)  Freed%
     ------------------------------------------------------------------------------------
     [PAYMENTS]
       OPPAYMENTS.BULK_PAYMENT                 ...          ...          ...        ...%
       OPPAYMENTS.BULK_PAYMENT_ADDITIONAL_INFO ...          ...          ...        ...%
       ... (all PAYMENTS tables, sorted by name ASC)
       -- subtotal ----------------------------------------------------------------
       PAYMENTS subtotal                       ...          ...          ...        ...%

     [LOGS]
       OP.SPEC_TRT_LOG                         ...          ...          ...        ...%
       OPPAYMENTS.AUDIT_ARCHIVE                ...          ...          ...        ...%
       OPPAYMENTS.AUDIT_TRAIL                  ...          ...          ...        ...%
       LOGS subtotal                           ...          ...          ...        ...%

     [BANK_STATEMENTS]
       OPPAYMENTS.DIRECTORY_DISPATCHING        ...          ...          ...        ...%
       OPPAYMENTS.FILE_DISPATCHING             ...          ...          ...        ...%
       BANK_STATEMENTS subtotal                ...          ...          ...        ...%
     ====================================================================================
       PURGED TABLES TOTAL                     ...          ...          ...        ...%
       TABLESPACE TOTAL                        ...          ...          ...        ...%
       (includes non-purged segments)
     ====================================================================================
     ```

   - If `p_depth = 'PAYMENTS'`, only the `[PAYMENTS]` block + `PAYMENTS subtotal` + `PURGED TABLES TOTAL` (== subtotal in this case) + `TABLESPACE TOTAL`. Same pattern for LOGS / BANK_STATEMENTS.
   - If `p_depth = 'ALL'`, show all three blocks.
   - Sorting: blocks in fixed order PAYMENTS → LOGS → BANK_STATEMENTS; within each block sort by `owner || '.' || parent_table` ASC.
   - Always show all rows for the displayed modules even if Freed = 0 (per your decision).

3. **[bin/epf_purge.sh](bin/epf_purge.sh) / [bin/epf_purge.bat](bin/epf_purge.bat) `capture_space_comparison`**: pass the actual purge depth into `print_space_comparison`. Read it from the latest `RUN_START` log entry (or from the wrapper's known `$PURGE_DEPTH`).

### Test checklist (Phase 2)

| # | Check | Observation |
|---|-------|-------------|
| 2.1 | Run `--depth PAYMENTS --reclaim`. Comparison shows ONLY the [PAYMENTS] block + PURGED TOTAL + TABLESPACE TOTAL. No LOGS/BANK_STATEMENTS rows. | _to fill_ |
| 2.2 | Run `--depth LOGS --reclaim`. Comparison shows `OP.SPEC_TRT_LOG`, `OPPAYMENTS.AUDIT_ARCHIVE`, `OPPAYMENTS.AUDIT_TRAIL` (sorted), correct subtotal. | _to fill_ |
| 2.3 | Run `--depth ALL --reclaim`. All 27 tables visible, grouped by module, alphabetical inside each module. | _to fill_ |
| 2.4 | "TABLESPACE TOTAL" row matches the reclaim script's "Used space" before/after numbers (sanity check). | _to fill_ |
| 2.5 | A table with zero rows purged still appears with Freed=0.00 / 0.0%. | _to fill_ |
| 2.6 | If DBA grants are missing and we fall back to user_segments, the comparison still works (just with a warning that TABLESPACE TOTAL is incomplete). | _to fill_ |
| 2.7 | Existing installs (with old `epf_purge_space_snapshot` schema): the additive ALTER TABLE runs cleanly on first new run; old snapshots still readable. | _to fill_ |

---

## Phase 3 — Auto-show sizes integrated into depth prompt

**Goal:** Drop `--show-sizes` as a manual step. Always compute module sizes before the depth prompt and embed them inline.

### Changes

1. **New tiny SQL helper** (or refactor [sql/11_show_module_sizes.sql](sql/11_show_module_sizes.sql)):
   - Output 4 pipe-delimited values for the wrapper to capture: `PAYMENTS_GB|LOGS_GB|BANK_STATEMENTS_GB|TOTAL_GB`.
   - Keep the existing pretty-print version as `11b_show_module_sizes_pretty.sql` if needed for ad-hoc use (or just delete — the wrapper now formats it).
2. **[bin/epf_purge.sh](bin/epf_purge.sh) / [bin/epf_purge.bat](bin/epf_purge.bat)**:
   - Before the "Purge Depth" prompt (interactive) **and** before the configuration summary (non-interactive), call the size helper and capture the four numbers into local variables. Skip silently if DB connection isn't established yet (fall back to no-numbers prompt).
   - Format the depth prompt with sizes inline:
     ```
       Purge Depth
       Controls which data modules are purged:
         ALL              [~11.67 GB]  Purge all modules (payments, logs, bank statements)
         PAYMENTS         [~ 5.40 GB]  Purge bulk payments and file integrations only
         LOGS             [~ 0.42 GB]  Purge audit trail + technical logs only
         BANK_STATEMENTS  [~ 5.85 GB]  Purge bank statement dispatching only
       Enter depth [ALL]:
     ```
   - LOGS sizes will not include `op.spec_trt_log` unless we cross-schema query. Add it (separate small query as `oppayments` user requires `SELECT` on `op.spec_trt_log` segment metadata via `dba_segments`, or via `SELECT FROM op.spec_trt_log` to count bytes). We'll query `dba_segments` if grant is present, fall back to `user_segments` (which excludes spec_trt_log → footnote in prompt).
3. **Deprecate `--show-sizes`**: keep as a no-op (still parsed, prints a deprecation note, then continues). Drop the interactive "show sizes? Y/N" prompt entirely. Update `--help`.

### Test checklist (Phase 3)

| # | Check | Observation |
|---|-------|-------------|
| 3.1 | Interactive run: depth prompt shows GB next to each option. Numbers look reasonable. | _to fill_ |
| 3.2 | Non-interactive run with `--depth ALL`: configuration summary shows the same module sizes (or before-summary line). | _to fill_ |
| 3.3 | Without DBA grants on dba_segments: the prompt still shows numbers via user_segments, with a footnote that spec_trt_log isn't included. | _to fill_ |
| 3.4 | `--show-sizes` flag: still accepted, prints deprecation note, behavior unchanged. | _to fill_ |
| 3.5 | The "show sizes? Y/N" interactive prompt is gone. | _to fill_ |
| 3.6 | If DB connection fails, the prompt falls back gracefully (no numbers, no crash). | _to fill_ |

---

## Phase 4 — Polish

**Goal:** Small UX/reliability improvements that don't fit the other phases.

### Changes

1. **Group Configuration Summary** ([bin/epf_purge.sh](bin/epf_purge.sh) `main()` and `.bat`):
   ```
   ============================================================
     Configuration Summary
   ============================================================
   [Connection]
     TNS Name:       EPFPROD
     Username:       oppayments
   [Purge]
     Retention:      90 days
     Depth:          ALL
     Batch Size:     5000
     Dry Run:        N
   [Maintenance]
     Optimize DB:    N
     Reclaim Space:  Y
     Skip Stall:     N
     Drop Package:   N
     Truncate Logs:  N
     Drop Logs:      N
   ============================================================
   ```
2. **Drop duplicate "EPF DATA PURGE" header** in [sql/03_epf_purge_pkg_body.sql:1675-1683](sql/03_epf_purge_pkg_body.sql#L1675-L1683). The wrapper already framed the run; the package can just emit a single "[ORCHESTRATOR] Run started: id=…" line via `log_entry` (which the monitor will format).
3. **`EPF_SYS_PASSWORD` env var**:
   - In [bin/epf_purge.sh](bin/epf_purge.sh) `load_config` and arg-parse: if `SYS_PASSWORD` is empty after args+config, check `$EPF_SYS_PASSWORD`. Same in `.bat`.
   - Document in `--help` and in [config/epf_purge.conf.example](config/epf_purge.conf.example).
4. **Post-fail comparison warning** in [bin/epf_purge.sh](bin/epf_purge.sh) `capture_space_comparison`:
   - Before printing comparison, query: `SELECT status FROM oppayments.epf_purge_log WHERE operation = 'RECLAIM_END' AND run_id = <latest> ORDER BY log_id DESC FETCH FIRST 1 ROW ONLY;`
   - If status = 'ERROR' (or no RECLAIM_END row at all when `--reclaim` was requested), print a banner: `WARNING: Reclaim ended with errors — AFTER snapshot may not reflect the intended final state. See epf_purge_log for details.` Print comparison anyway.
   - Mirror in `.bat`.

### Test checklist (Phase 4)

| # | Check | Observation |
|---|-------|-------------|
| 4.1 | Configuration summary now uses [Connection]/[Purge]/[Maintenance] groups. | _to fill_ |
| 4.2 | Only one "purge started" header line appears (from wrapper's monitor formatting), no duplicate banner from PL/SQL. | _to fill_ |
| 4.3 | Run unattended on Linux: `EPF_PURGE_PASSWORD=… EPF_SYS_PASSWORD=… ./bin/epf_purge.sh --tns X --reclaim --assume-yes`. No prompts. | _to fill_ |
| 4.4 | Same on Windows with `set EPF_PURGE_PASSWORD=…` and `set EPF_SYS_PASSWORD=…`. | _to fill_ |
| 4.5 | Force reclaim failure → `capture_space_comparison` prints the WARNING banner before the comparison table. | _to fill_ |

---

## Cross-cutting test checklist (run after all phases)

| # | Check | Observation |
|---|-------|-------------|
| X.1 | Linux full run: `bin/epf_purge.sh --tns X --user oppayments --retention 90 --reclaim --sys-password Y --assume-yes`. End-to-end clean log, all expected sections present, exit code 0. | _to fill_ |
| X.2 | Windows full run with the same args (using `bin\epf_purge.bat`). Compare logs side-by-side: should be near-identical. | _to fill_ |
| X.3 | Diff `logs/epf_purge_*.log` from a Linux run vs a Windows run on the same DB. List any structural differences. | _to fill_ |
| X.4 | Cancel mid-purge (Ctrl+C). Confirm: no leftover monitor process, undo_retention restored, partial state safely re-runnable. | _to fill_ |
| X.5 | Re-run after cancel. Confirm: picks up where it left off (purge is date-filtered, so this should just continue). | _to fill_ |
| X.6 | Update [README.md](README.md) and [CLAUDE.md](../CLAUDE.md) to reflect the auto-sizes behavior, removed `--show-sizes` requirement, and new env var. | _to fill_ |

---

## Out of scope (deferred or rejected)

- Replacing the polling-based monitor with DBMS_PIPE / DBMS_OUTPUT streaming — not worth the complexity, polling works.
- Changing the batched-commit / autonomous-transaction architecture — works, don't touch.
- Adding automated tests — no test harness exists; would need a containerized Oracle setup. Discuss separately.
- Migrating wrappers to a single language (e.g. Python) — out of scope; .sh/.bat parity is the brief.

---

## How we'll work this plan

1. I'll execute one phase per round, mark its todos complete, and hand you the test checklist for that phase.
2. You run the checks against your environment and fill in the "Observation" column.
3. We adjust based on what you find before moving to the next phase.
4. Final cross-cutting checklist runs after all phases land.
