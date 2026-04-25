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

## Phase 1 — Reclaim live logging reliability (Linux/parity)  — **DONE + follow-ups**

**Goal:** make Linux runs stream reclaim progress live, the same way Windows already does, with no silent failure modes.

**Initial round files changed:**
- new [bin/epf_monitor.sh](bin/epf_monitor.sh) — port of [bin/epf_monitor.ps1](bin/epf_monitor.ps1), positional args `CONN_STR POLL_SEC MAX_WAIT_MIN LOG_FILE`
- [bin/epf_purge.sh](bin/epf_purge.sh) `start_monitor`/`stop_monitor` — invoke external script instead of inline subshell
- [bin/epf_purge.sh](bin/epf_purge.sh) `restore_undo_post_purge` — capture sqlplus output, verify `undo_retention=900`, warn on failure
- [bin/epf_purge.bat](bin/epf_purge.bat) post-purge undo restore — same fix
- [bin/epf_monitor.ps1](bin/epf_monitor.ps1) — added `SHRINK_PROGRESS` operation, distinguished `RUN_END`/`RECLAIM_END` ERROR vs SUCCESS
- [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql) Phase 1 loop — emit `SHRINK_PROGRESS` every 10 tables OR 60s; removed buffered per-table `SHRINK OK` print
- [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql) — top-level `EXCEPTION WHEN OTHERS` emits `RECLAIM_END status=ERROR` then re-raises

**Phase 1 follow-up round 3 (after third user test):**
- Round 2's raw `StreamWriter` made things worse — monitor showed nothing at all. Root cause: spawning the monitor via `Start-Process -PassThru -NoNewWindow` from one PowerShell into another corrupts handle inheritance such that `[Console]::OpenStandardOutput()` doesn't connect to the visible console. Decision: stop trying to share the parent's console. Move the monitor to a **separate console window**.
- [bin/epf_purge.bat](bin/epf_purge.bat) `start_monitor` — writes a small launcher .bat to `%TEMP%`, then spawns it via `Start-Process cmd` (no `-NoNewWindow`) which opens a new console window. The launcher runs the monitor and ends with `pause` so the window stays open after the monitor exits, letting the operator read the final output. Captures the cmd PID for cleanup.
- [bin/epf_purge.bat](bin/epf_purge.bat) `stop_monitor` — does **not** force-kill the monitor window. The launcher's `pause` keeps it open until the operator closes it manually. Cleanup deletes the temp launcher.bat.
- [bin/epf_purge.sh](bin/epf_purge.sh) `start_monitor` — backgrounds the monitor with stdout/stderr redirected to `/dev/null` so live updates don't interleave with main wrapper output. Monitor still appends every line to `$LOG_FILE`. Wrapper prints a `tail -f "$LOG_FILE"` instruction so the operator can watch live in another terminal.
- [bin/epf_monitor.ps1](bin/epf_monitor.ps1) — reverted `Write-Log` to plain `Write-Host`. The raw `StreamWriter` was only needed to fight the inherited-handle buffering, which the new architecture sidesteps. Removed the `$script:rawStdout` cleanup.
- Layout invariant (both platforms): main console = summary lines only; live updates appear in a separate window (Windows) or via `tail -f LOG_FILE` (Linux); LOG_FILE contains both, in chronological append order.

**Phase 1 follow-up round 2 (after second user test):**
- [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql) — SHRINK_PROGRESS reverted to **every 10 tables, no wallclock floor**. The 60s floor turned into "log every iter" because individual SHRINK ops can exceed 60s, so the throttle fired every loop body. Per-N-tables cadence is the only useful knob.
- [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql) — SQUEEZE_PROGRESS reverted to **every 25 iters, no wallclock floor** (user's explicit preference). Max log volume now ~80 lines for a 2000-iter cap.
- [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql) — termination conditions now go through `reclaim_log` (was DBMS_OUTPUT, suppressed by `SET SERVEROUTPUT OFF`): "target reached", "max iter cap hit" (with re-run hint), "stall exit" (with re-run hint).
- [bin/epf_monitor.ps1](bin/epf_monitor.ps1) — replaced `[Console]::Out.WriteLine + Flush()` with a raw `System.IO.StreamWriter` wrapped around `[Console]::OpenStandardOutput()` with `AutoFlush=$true`. Bypasses .NET's `Console` class buffering entirely. The earlier `Flush()` approach was insufficient — user still saw "stuck" gaps until Ctrl+C.

**Phase 1 follow-up round (after first user test):**
- [bin/epf_purge.bat](bin/epf_purge.bat) undo-restore — replaced cmd `type ... >> %LOG_FILE%` (which collides with the monitor's open write handle and prints `"The process cannot access the file because it is being used by another process."`) with the same PowerShell `FileStream(..., 'ReadWrite')` pattern used elsewhere in the .bat
- [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql) — `SET SERVEROUTPUT OFF` to suppress the buffered DBMS_OUTPUT flood that was dumping reclaim headers + per-iter lines after the block ended, garbling the monitor output
- [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql) — `SQUEEZE_PROGRESS` cadence improved: log iter 1 + every 10 iters + every 60s wallclock (was every 25 iters with no wallclock floor → 10-min silence on slow MOVEs)
- [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql) — explicit Phase 2 start log (`SQUEEZE_PROGRESS — Phase 2 squeeze starting...`) so the monitor draws a clear boundary between SHRINK and SQUEEZE
- [bin/epf_purge.sh](bin/epf_purge.sh) and [bin/epf_purge.bat](bin/epf_purge.bat) — drain delays (`sleep 15` / `Start-Sleep 15`) before reclaim header and after reclaim ends, so the monitor's 10s polls have time to catch up to RUN_END / RECLAIM_END before the wrapper races ahead
- [bin/epf_monitor.ps1](bin/epf_monitor.ps1) — `Write-Log` now uses `[Console]::Out.WriteLine` + explicit `Flush()` so output surfaces immediately when stdout is captured by parent cmd.exe (was buffered, only flushed on Ctrl+C)
- [bin/epf_monitor.ps1](bin/epf_monitor.ps1) — removed unused `$purgeEnded` variable

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

**First test round results** (Windows, single end-to-end run on a fresh DB import):

| # | Check | Observation |
|---|-------|-------------|
| 1.1 | On Linux, run `--reclaim` end-to-end. | **Skipped** (test was on Windows) |
| 1.2 | On Windows, run `--reclaim` end-to-end. Output should match Linux line-for-line. | **Partial pass / 4 issues found.** Reclaim ran. (a) `"The process cannot access the file because it is being used by another process."` printed during undo restore (.bat file-lock vs monitor) → **fixed** by switching that step to PowerShell `FileStream(..., 'ReadWrite')`. (b) Reclaim header printed before the last BANK_STATEMENTS batches and `** PURGE COMPLETED **` had drained from monitor → **fixed** by 15s drain before reclaim header. (c) ~10 min silence between `SHRINK done` and first SQUEEZE log because cadence was every-25-iters with no wallclock floor → **fixed** by iter 1 + every 10 iters + every 60s. (d) After last iter, `** RECLAIM COMPLETED **` was buffered and only surfaced on Ctrl+C; redundant DBMS_OUTPUT flood appeared garbled → **fixed** by `SET SERVEROUTPUT OFF`, `[Console]::Out.WriteLine + Flush()` in monitor, and 15s drain after reclaim ends. **Re-test required to confirm fixes.** |
| 1.3 | Wrapper stdout vs `logs/epf_purge_<ts>.log` identical. | **Skipped** |
| 1.4 | Force reclaim failure → `RECLAIM_END ERROR` shows live, wrapper exits cleanly. | **Skipped** |
| 1.5 | Verify `undo_retention=900` after run. | **Pass.** Wrapper printed `[OK] undo_retention restored to 900s`. (The "file is being used" warning was from the .bat log-append, not from the SQL itself — the SQL succeeded.) |
| 1.6 | Force undo restore failure (wrong SYS pw) → see WARN with sqlplus output. | **Skipped** |
| 1.7 | Ctrl+C mid-purge: no orphan monitor process. | **Skipped** (Ctrl+C was used at end-of-reclaim to surface buffered output, not a normal cancel test) |
| 1.8 | `--reclaim-only`: monitor attaches and runs through RECLAIM_END. | **Skipped** |
| 1.9 | SHRINK_PROGRESS cadence (every 10 tables OR 60s). | **Pass for shrink.** The pre-fix log showed `Shrunk 758 tables (skipped 9), last: OPPAYMENTS.WORKFLOW_EVENT` lines firing throughout Phase 1 with no >60s gaps. |
| 1.10 | No leftover inline-monitor variables in `epf_purge.sh`. | **Pass** (verified via grep). |

**Re-test items after follow-up round 1 fixes:**

| # | Check | Observation |
|---|-------|-------------|
| 1F.1 | Re-run end-to-end on Windows with `--reclaim`. Confirm: no `"file is being used"` warning during undo restore. | **Pass** (no warning seen this run). |
| 1F.2 | Confirm the `Online Tablespace Reclaim` header appears AFTER `** PURGE COMPLETED **` and after `Waiting for reclaim to start...`, with no interleaving of leftover purge batch lines. | _to verify in next run_ |
| 1F.3 | Confirm Phase 1 → Phase 2 transition is visible. | **Pass** for the boundary line itself ("Phase 2 squeeze starting" prints right after "Phase 1 SHRINK done"). |
| 1F.4 | Confirm `** RECLAIM COMPLETED **` surfaces in real time at the end (no Ctrl+C needed). | _to verify in next run_ |
| 1F.5 | Confirm the reclaim DBMS_OUTPUT flood is **gone** from both stdout and the logfile. | _to verify in next run_ |
| 1F.6 | Confirm `[OK] Online reclaim completed` appears at the very end and the wrapper exits cleanly without hang. | _to verify in next run_ |

**Issues found in second user test (drove round 2 fixes):**
- 1F-2.A — Cadence too verbose: "every 10/25 iters OR 60s wallclock" became "every iter" when iters took 90-300s each (LOB MOVE on retained data). User wanted original 25-iter behavior with no floor. → **Fixed** in round 2.
- 1F-2.B — `Console.Out.Flush()` did NOT fully resolve the buffering issue: user still saw `~8min stuck` gaps that only flushed on Ctrl+C. → **Fixed** in round 2 by switching to raw `StreamWriter` over `OpenStandardOutput()`.

**Re-test items after follow-up round 2 fixes:**

| # | Check | Observation |
|---|-------|-------------|
| 1G.1 — 1G.7 | (Round 2 cadence/termination/buffering checks) | **Superseded by round 3** — the buffering issue made all of these untestable in round 2 because the monitor displayed nothing. Re-test under the round-3 layout below. |

**Issues found in third user test (drove round 3 fixes):**
- 1G-3.A — Raw `StreamWriter` over `OpenStandardOutput()` made the situation worse: monitor appeared to start (PID captured) but displayed nothing on stdout. Root cause: `Start-Process -NoNewWindow` from outer-PowerShell-to-inner-PowerShell corrupts which console handle the inner process can write to. → **Fixed** in round 3 by moving monitor to a separate console window (Windows) or a `tail -f`-friendly background log (Linux).

**Re-test items after follow-up round 3 fixes (please run again):**

| # | Check | Observation |
|---|-------|-------------|
| 1H.1 | **Windows:** running `bin\epf_purge.bat ...` with `--reclaim` opens a separate console window titled "EPF Live Monitor". | _to fill_ |
| 1H.2 | **Windows:** the monitor window shows `[MONITOR] Tracking run_id: ...`, then live `RUN_START`, `DELETE batch ...`, `SHRINK_PROGRESS`, `SQUEEZE_PROGRESS`, `RECLAIM_END` lines as they happen — within ~10s of each DB write, no Ctrl+C needed. | _to fill_ |
| 1H.3 | **Windows:** the main wrapper console shows ONLY summary lines (`[INFO]/[OK]/[WARN]` from the wrapper itself, plus DB connection messages and the configuration summary). No purge batch lines, no SQUEEZE iter lines. | _to fill_ |
| 1H.4 | **Windows:** after monitor exits naturally (RECLAIM_END), the monitor window shows `[Monitor exited. Press any key to close this window.]` and stays open. Closing it manually completes the run cleanly. | _to fill_ |
| 1H.5 | **LOG_FILE contents:** `Get-Content` (Windows) or `cat` (Linux) on the log file shows wrapper summary lines and live monitor lines interleaved in chronological order. No content is missing from the log. | _to fill_ |
| 1H.6 | **Linux:** `bin/epf_purge.sh ...` with `--reclaim` prints `[INFO] Starting live progress monitor in background` and `tail -f "..."` instruction. Main terminal stays clean (summary only). Running `tail -f LOG_FILE` in another terminal shows live updates as they happen. | _to fill_ |
| 1H.7 | SQUEEZE_PROGRESS fires every 25 iters (no per-iter chatter). Max ~80 lines for 2000-iter cap. | _to fill_ |
| 1H.8 | SHRINK_PROGRESS fires every 10 tables (no wallclock chatter). | _to fill_ |
| 1H.9 | Squeeze "target reached" exit shows `Squeeze done at iter N: HWM=... <= target ...`. | _to fill_ |
| 1H.10 | Max-iter exit (1G.4) and stall exit (1G.5) — same checks as round 2, now testable. | _to fill_ |

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

4. **Add `--max-iterations N` flag + recommended value** (NEW — routed from Phase 1 follow-up):
   - When auto-computing module sizes, also compute the OPPAYMENTS-default tablespace **datafile size** and **segment count above the projected target HWM** (cheap query).
   - Recommendation heuristic (cheap and safe): `recommended_max_iter = max(2000, 50 * datafile_gb)`. So 40 GB → 2000, 120 GB → 6000, 250 GB → 12500. Cap at e.g. 20000 to avoid runaway. Show this in the reclaim prompt:
     ```
       Max Squeeze Iterations
       Each iteration relocates one segment near the high water mark.
       Larger tablespaces typically need more iterations.
       Tablespace size: 120 GB    -> recommended: 6000 iterations
       Enter max iterations [6000]:
     ```
   - Pass the chosen value into `05_reclaim_tablespace.sql` via a new SQL define (`max_iterations`, default 2000 if not supplied — back-compat).
   - Add `--max-iterations N` flag and `MAX_ITERATIONS=N` config key.

### Test checklist (Phase 3)

| # | Check | Observation |
|---|-------|-------------|
| 3.1 | Interactive run: depth prompt shows GB next to each option. Numbers look reasonable. | _to fill_ |
| 3.2 | Non-interactive run with `--depth ALL`: configuration summary shows the same module sizes (or before-summary line). | _to fill_ |
| 3.3 | Without DBA grants on dba_segments: the prompt still shows numbers via user_segments, with a footnote that spec_trt_log isn't included. | _to fill_ |
| 3.4 | `--show-sizes` flag: still accepted, prints deprecation note, behavior unchanged. | _to fill_ |
| 3.5 | The "show sizes? Y/N" interactive prompt is gone. | _to fill_ |
| 3.6 | If DB connection fails, the prompt falls back gracefully (no numbers, no crash). | _to fill_ |
| 3.7 | Reclaim prompt shows recommended `max_iterations` based on tablespace size. Both interactive and `--help` document the new flag. | _to fill_ |
| 3.8 | `--max-iterations 5000` overrides the recommendation. SQL receives the value via define. | _to fill_ |

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

5. **Post-reclaim "max iterations exhausted" recommendation banner** (NEW — routed from Phase 1 follow-up):
   - In [sql/05_reclaim_tablespace.sql](sql/05_reclaim_tablespace.sql), track an exit-reason variable (`TARGET_REACHED` / `MAX_ITER_HIT` / `STALL_EXIT`). Pass it through to the `RECLAIM_END` log message.
   - In the wrapper, after `capture_space_comparison`, if exit reason was `MAX_ITER_HIT` AND HWM > target, print a clearly-visible boxed banner:
     ```
     ============================================================
       NOTE: Reclaim hit the iteration cap (X/X) before reaching
       the target HWM. Some segments are still above target.
       To squeeze further, re-run the reclaim phase only:
         bin/epf_purge.sh --tns EPFPROD --user oppayments \
           --reclaim-only --sys-password ... \
           --max-iterations <larger N>
     ============================================================
     ```
   - Compose the printed command from the actual flags used in this run (TNS, user, sys-pw redacted as `...`, larger max-iter recommendation = `2 * current`).

### Test checklist (Phase 4)

| # | Check | Observation |
|---|-------|-------------|
| 4.1 | Configuration summary now uses [Connection]/[Purge]/[Maintenance] groups. | _to fill_ |
| 4.2 | Only one "purge started" header line appears (from wrapper's monitor formatting), no duplicate banner from PL/SQL. | _to fill_ |
| 4.3 | Run unattended on Linux: `EPF_PURGE_PASSWORD=… EPF_SYS_PASSWORD=… ./bin/epf_purge.sh --tns X --reclaim --assume-yes`. No prompts. | _to fill_ |
| 4.4 | Same on Windows with `set EPF_PURGE_PASSWORD=…` and `set EPF_SYS_PASSWORD=…`. | _to fill_ |
| 4.5 | Force reclaim failure → `capture_space_comparison` prints the WARNING banner before the comparison table. | _to fill_ |
| 4.6 | Run reclaim with a deliberately low `--max-iterations 50` on a fragmented tablespace. Confirm the banner with the suggested re-run command appears at the very end. | _to fill_ |
| 4.7 | Banner is NOT printed when reclaim exited via TARGET_REACHED. | _to fill_ |

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
