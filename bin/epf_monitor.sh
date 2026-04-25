#!/bin/bash
# ============================================================================
# EPF Data Purge - Live Progress Monitor (Bash)
# ============================================================================
# Runs alongside the purge in a separate process. Polls epf_purge_log via
# SQL*Plus every N seconds and prints new entries to stdout in real-time.
# Output is NOT buffered (each poll is a fresh SQL*Plus invocation).
#
# Mirror of bin/epf_monitor.ps1 so Linux and Windows runs produce identical
# log output.
#
# Automatically exits when it sees RECLAIM_END, a top-level ORCHESTRATOR
# ERROR, or when the wrapper kills it after the run completes.
#
# Usage (positional args):
#   epf_monitor.sh CONN_STR [POLL_SEC] [MAX_WAIT_MIN] [LOG_FILE]
#
# Args:
#   CONN_STR     - SQL*Plus connection string: user/pass@tns
#   POLL_SEC     - Poll interval in seconds (default 10)
#   MAX_WAIT_MIN - Max idle minutes before timeout exit (default 360)
#   LOG_FILE     - Optional file to also append output to
# ============================================================================

set -u

CONN_STR="${1:-}"
POLL_SEC="${2:-10}"
MAX_WAIT_MIN="${3:-360}"
LOG_FILE="${4:-}"

if [[ -z "$CONN_STR" ]]; then
    echo "[MONITOR] ERROR: No connection string provided" >&2
    echo "Usage: $0 CONN_STR [POLL_SEC] [MAX_WAIT_MIN] [LOG_FILE]" >&2
    exit 2
fi

# Multiple processes appending to LOG_FILE is safe under O_APPEND for line-sized
# writes (< PIPE_BUF). The wrapper also writes to LOG_FILE; both write atomically.
write_log() {
    echo "$1"
    if [[ -n "$LOG_FILE" ]]; then
        echo "$1" >> "$LOG_FILE" 2>/dev/null || true
    fi
}

write_log "============================================================"
write_log "  EPF PURGE - LIVE MONITOR"
write_log "  Poll interval: ${POLL_SEC}s   Max wait: ${MAX_WAIT_MIN} min"
write_log "  Started:       $(date '+%Y-%m-%d %H:%M:%S')"
write_log "============================================================"
write_log ""

last_log_id=0
run_id=""
found_run=0
done_flag=0
idle_since=$(date +%s)
wait_msg_count=0
max_idle_sec=$((MAX_WAIT_MIN * 60))
monitor_start_utc=$(date -u '+%Y-%m-%d %H:%M:%S')

while [[ $done_flag -eq 0 ]]; do
    # ------------------------------------------------------------------------
    # Discover latest run_id
    # ------------------------------------------------------------------------
    # Only pick up runs that are either still in progress or started after
    # this monitor was launched.  Prevents the monitor from locking onto a
    # completed old run, replaying its RECLAIM_END / ERROR, and exiting
    # before the current run even begins.
    if [[ $found_run -eq 0 ]]; then
        run_id=$(sqlplus -S "$CONN_STR" <<ENDSQL 2>/dev/null | tr -d '[:space:]'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 300 TRIMOUT ON TRIMSPOOL ON
SELECT RAWTOHEX(run_id) FROM (
    SELECT run_id FROM oppayments.epf_purge_log
    WHERE operation = 'RUN_START'
      AND (
          log_timestamp >= TO_TIMESTAMP('${monitor_start_utc}', 'YYYY-MM-DD HH24:MI:SS')
          OR
          NOT EXISTS (
              SELECT 1 FROM oppayments.epf_purge_log e2
              WHERE e2.run_id = oppayments.epf_purge_log.run_id
                AND (e2.operation = 'RECLAIM_END'
                     OR (e2.module = 'ORCHESTRATOR' AND e2.status = 'ERROR'))
          )
      )
    ORDER BY log_timestamp DESC
) WHERE ROWNUM = 1;
EXIT;
ENDSQL
        )
        if [[ -n "$run_id" && ${#run_id} -ge 16 ]]; then
            found_run=1
            idle_since=$(date +%s)
            write_log "[MONITOR] Tracking run_id: $run_id"
            write_log ""
        else
            wait_msg_count=$((wait_msg_count + 1))
            if (( wait_msg_count % 6 == 1 )); then
                write_log "[MONITOR] Waiting for purge to start..."
            fi
        fi
    fi

    # ------------------------------------------------------------------------
    # Fetch new log entries since last_log_id
    # ------------------------------------------------------------------------
    if [[ $found_run -eq 1 ]]; then
        output=$(sqlplus -S "$CONN_STR" <<ENDSQL 2>/dev/null
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 500 TRIMOUT ON TRIMSPOOL ON
SELECT log_id || '|' ||
       TO_CHAR(log_timestamp, 'HH24:MI:SS') || '|' ||
       module || '|' ||
       operation || '|' ||
       NVL(TO_CHAR(batch_number), '') || '|' ||
       NVL(TO_CHAR(rows_affected), '0') || '|' ||
       status || '|' ||
       NVL(REPLACE(message, '|', '/'), '') || '|' ||
       NVL(TO_CHAR(ROUND(elapsed_seconds, 1)), '') || '|' ||
       NVL(table_name, '')
FROM oppayments.epf_purge_log
WHERE run_id = HEXTORAW('${run_id}')
  AND log_id > ${last_log_id}
ORDER BY log_id;
EXIT;
ENDSQL
        )

        any_new=0
        while IFS='|' read -r log_id ts module operation batch_num rows_aff status message elapsed tbl_name; do
            log_id=$(echo "$log_id" | tr -d '[:space:]')
            [[ -z "$log_id" ]] && continue

            any_new=1
            last_log_id=$log_id
            idle_since=$(date +%s)

            ts=$(echo "$ts" | xargs)
            module=$(echo "$module" | xargs)
            operation=$(echo "$operation" | xargs)
            batch_num=$(echo "$batch_num" | xargs)
            rows_aff=$(echo "$rows_aff" | xargs)
            status=$(echo "$status" | xargs)
            message=$(echo "$message" | xargs)
            elapsed=$(echo "$elapsed" | xargs)
            tbl_name=$(echo "$tbl_name" | xargs)

            line=""
            case "$operation" in
                RUN_START)
                    line="[$ts] ** PURGE STARTED ** $message"
                    ;;
                RUN_END)
                    if [[ "$status" == "ERROR" ]]; then
                        write_log "[$ts] *** PURGE FAILED *** $message"
                        done_flag=1
                        break
                    else
                        write_log "[$ts] ** PURGE COMPLETED ** $message (total: ${elapsed}s)"
                        write_log "[$ts] Waiting for reclaim to start..."
                    fi
                    ;;
                RECLAIM_START)
                    write_log ""
                    line="[$ts] ** RECLAIM STARTED ** $message"
                    ;;
                RECLAIM_END)
                    if [[ "$status" == "ERROR" ]]; then
                        write_log "[$ts] *** RECLAIM FAILED *** $message"
                    else
                        write_log "[$ts] ** RECLAIM COMPLETED ** $message (total: ${elapsed}s)"
                    fi
                    done_flag=1
                    break
                    ;;
                SHRINK_DONE)
                    line="[$ts] $(printf '%-12s' RECLAIM) Phase 1 SHRINK done: $message (${elapsed}s)"
                    ;;
                SHRINK_PROGRESS|SQUEEZE_PROGRESS)
                    line="[$ts] $(printf '%-12s' RECLAIM) $message (${elapsed}s)"
                    ;;
                DELETE)
                    if [[ -n "$batch_num" ]]; then
                        line="[$ts] $(printf '%-12s' "$module") batch $(printf '%5s' "$batch_num")  $message  (${elapsed}s)"
                    elif [[ -n "$tbl_name" && "${rows_aff:-0}" -gt 0 ]]; then
                        line="[$ts] $(printf '%-12s' "$module") TOTAL  $(printf '%-48s' "$tbl_name") $(printf '%14s' "$rows_aff")"
                    fi
                    ;;
                DRY_RUN_COUNT|INFO|INIT)
                    line="[$ts] $(printf '%-12s' "$module") $message"
                    ;;
                *)
                    if [[ "$status" == "ERROR" ]]; then
                        write_log "[$ts] *** ERROR *** $module - $message"
                        done_flag=1
                        break
                    elif [[ -n "$message" ]]; then
                        line="[$ts] $(printf '%-12s' "$module") $message"
                    fi
                    ;;
            esac

            [[ -n "$line" ]] && write_log "$line"
        done <<< "$output"

        # --------------------------------------------------------------------
        # If no new activity, check for a newer run_id (handles cancel/restart)
        # or for a run that already completed while we were not looking.
        # --------------------------------------------------------------------
        if [[ $any_new -eq 0 && $last_log_id -gt 0 && $done_flag -eq 0 ]]; then
            latest_id=$(sqlplus -S "$CONN_STR" <<'ENDSQL' 2>/dev/null | tr -d '[:space:]'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 300 TRIMOUT ON TRIMSPOOL ON
SELECT RAWTOHEX(run_id) FROM (
    SELECT run_id FROM oppayments.epf_purge_log
    WHERE operation = 'RUN_START'
    ORDER BY log_timestamp DESC
) WHERE ROWNUM = 1;
EXIT;
ENDSQL
            )
            if [[ -n "$latest_id" && ${#latest_id} -ge 16 && "$latest_id" != "$run_id" ]]; then
                write_log ""
                write_log "[MONITOR] Newer run detected: $latest_id (was $run_id)"
                write_log "[MONITOR] Switching to new run_id..."
                write_log ""
                run_id="$latest_id"
                last_log_id=0
                idle_since=$(date +%s)
            else
                # Same run -- did we miss completion?
                end_count=$(sqlplus -S "$CONN_STR" <<ENDSQL 2>/dev/null | tr -d '[:space:]'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 50 TRIMOUT ON TRIMSPOOL ON
SELECT COUNT(*) FROM oppayments.epf_purge_log
WHERE run_id = HEXTORAW('${run_id}')
  AND (operation = 'RECLAIM_END' OR (module = 'ORCHESTRATOR' AND status = 'ERROR'));
EXIT;
ENDSQL
                )
                if [[ -n "$end_count" && "$end_count" -gt 0 ]]; then
                    done_flag=1
                fi
            fi
        fi
    fi

    [[ $done_flag -eq 1 ]] && break

    # ------------------------------------------------------------------------
    # Idle timeout
    # ------------------------------------------------------------------------
    now=$(date +%s)
    idle=$(( now - idle_since ))
    if (( idle > max_idle_sec )); then
        write_log "[MONITOR] Timeout: no activity for $((idle / 60)) minutes. Exiting."
        break
    fi

    sleep "$POLL_SEC"
done

write_log ""
write_log "[MONITOR] Monitor stopped at $(date '+%Y-%m-%d %H:%M:%S')"
write_log "============================================================"
