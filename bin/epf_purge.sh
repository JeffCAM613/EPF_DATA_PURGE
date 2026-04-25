#!/bin/bash
# ============================================================================
# EPF Data Purge - Linux/Unix Wrapper Script
# ============================================================================
# Deploys and executes the EPF purge PL/SQL package against an Oracle database.
#
# Usage:
#   Interactive:   ./epf_purge.sh
#   With config:   ./epf_purge.sh --config ../config/epf_purge.conf
#   With args:     ./epf_purge.sh --tns EPFPROD --user oppayments --retention 90
#
# Prerequisites:
#   - Oracle SQL*Plus installed and on PATH
#   - ORACLE_HOME environment variable set
#   - Database user with DELETE on oppayments.*, CREATE TABLE, CREATE PROCEDURE
# ============================================================================

set -euo pipefail

# ============================================================================
# Defaults
# ============================================================================
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
SQL_DIR="$PROJECT_DIR/sql"
LOG_DIR="$PROJECT_DIR/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/epf_purge_${TIMESTAMP}.log"

TNS_NAME=""
USERNAME="oppayments"
PASSWORD=""
RETENTION_DAYS=30
PURGE_DEPTH="ALL"
BATCH_SIZE=1000
DRY_RUN="N"
RECLAIM_SPACE="N"
RECLAIM_ONLY="N"
SKIP_STALL_CHECKS="N"
OPTIMIZE_DB="N"
SYS_PASSWORD=""
ASSUME_YES="N"
DROP_PACKAGE_AFTER="N"
DROP_LOGS="N"
TRUNCATE_LOGS="N"
SHOW_SIZES="N"
CONFIG_FILE=""

# ============================================================================
# Color output helpers
# ============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info()    { echo -e "${BLUE}[INFO]${NC}  $1" | tee -a "$LOG_FILE"; }
log_ok()      { echo -e "${GREEN}[OK]${NC}    $1" | tee -a "$LOG_FILE"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1" | tee -a "$LOG_FILE"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"; }
log_header()  { echo -e "\n${BLUE}============================================================${NC}" | tee -a "$LOG_FILE"
                echo -e "${BLUE}  $1${NC}" | tee -a "$LOG_FILE"
                echo -e "${BLUE}============================================================${NC}" | tee -a "$LOG_FILE"; }

# ============================================================================
# Parse command-line arguments
# ============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --config)       CONFIG_FILE="$2"; shift 2 ;;
            --tns)          TNS_NAME="$2"; shift 2 ;;
            --user)         USERNAME="$2"; shift 2 ;;
            --password)     PASSWORD="$2"; shift 2 ;;
            --retention)    RETENTION_DAYS="$2"; shift 2 ;;
            --depth)        PURGE_DEPTH="$2"; shift 2 ;;
            --batch-size)   BATCH_SIZE="$2"; shift 2 ;;
            --dry-run)      DRY_RUN="Y"; shift ;;
            --reclaim)      RECLAIM_SPACE="Y"; shift ;;
            --reclaim-only) RECLAIM_ONLY="Y"; RECLAIM_SPACE="Y"; shift ;;
            --reclaim-online)      RECLAIM_SPACE="Y"; shift ;;  # legacy alias
            --reclaim-online-only) RECLAIM_ONLY="Y"; RECLAIM_SPACE="Y"; shift ;;  # legacy alias
            --no-stall-check) SKIP_STALL_CHECKS="Y"; shift ;;
            --optimize-db)  OPTIMIZE_DB="Y"; shift ;;
            --sys-password) SYS_PASSWORD="$2"; shift 2 ;;
            --assume-yes|-y) ASSUME_YES="Y"; shift ;;
            --drop-pkg)     DROP_PACKAGE_AFTER="Y"; shift ;;
            --drop-logs)    DROP_LOGS="Y"; shift ;;
            --truncate-logs) TRUNCATE_LOGS="Y"; shift ;;
            --show-sizes)   SHOW_SIZES="Y"; shift ;;
            --help|-h)      show_help; exit 0 ;;
            *)              log_error "Unknown argument: $1"; show_help; exit 1 ;;
        esac
    done
}

# ============================================================================
# Load configuration file
# ============================================================================
load_config() {
    if [[ -n "$CONFIG_FILE" ]]; then
        if [[ ! -f "$CONFIG_FILE" ]]; then
            log_error "Config file not found: $CONFIG_FILE"
            exit 1
        fi
        log_info "Loading configuration from: $CONFIG_FILE"
        while IFS='=' read -r key value; do
            # Skip comments and empty lines
            [[ "$key" =~ ^#.*$ || -z "$key" ]] && continue
            # Trim whitespace
            key=$(echo "$key" | xargs)
            value=$(echo "$value" | xargs)
            case "$key" in
                TNS_NAME)           TNS_NAME="$value" ;;
                USERNAME)           USERNAME="$value" ;;
                PASSWORD)           PASSWORD="$value" ;;
                RETENTION_DAYS)     RETENTION_DAYS="$value" ;;
                PURGE_DEPTH)        PURGE_DEPTH="$value" ;;
                BATCH_SIZE)         BATCH_SIZE="$value" ;;
                DRY_RUN)            DRY_RUN="$value" ;;
                RECLAIM_SPACE)      RECLAIM_SPACE="$value" ;;
                SKIP_STALL_CHECKS)  SKIP_STALL_CHECKS="$value" ;;
                DROP_PACKAGE_AFTER) DROP_PACKAGE_AFTER="$value" ;;
            esac
        done < "$CONFIG_FILE"
    fi

    # Environment variable overrides config file for password
    if [[ -n "${EPF_PURGE_PASSWORD:-}" ]]; then
        PASSWORD="$EPF_PURGE_PASSWORD"
    fi
}

# ============================================================================
# Show help
# ============================================================================
show_help() {
    cat << 'HELPEOF'
EPF Data Purge Tool
===================

Usage:
  epf_purge.sh [OPTIONS]

Options:
  --config FILE     Load settings from config file
  --tns NAME        Oracle TNS name or connect string (e.g., EPFPROD)
  --user NAME       Database username (default: oppayments)
  --password PASS   Database password (prefer EPF_PURGE_PASSWORD env var)
  --retention N     Purge data older than N days (default: 30)
  --depth DEPTH     Purge scope: ALL, PAYMENTS, LOGS, BANK_STATEMENTS (default: ALL)
  --batch-size N    Rows per batch commit (default: 1000)
  --dry-run         Count rows only, do not delete anything
  --optimize-db     Run DB optimization before purge (enlarge redo logs, gather stats)
                    Needs DBA/SYS creds. ~4 GB temp disk space. Idempotent.
  --reclaim         After purge, run online space reclaim (SHRINK + squeeze + resize)
                    No downtime required. Needs DBA/SYS creds.
  --reclaim-only    Skip purge entirely, run online reclaim only
  --no-stall-check  Disable stall detection during reclaim (always run all iterations)
  --drop-pkg        Drop the PL/SQL package after execution
  --drop-logs       Drop purge log tables (epf_purge_log, epf_purge_space_snapshot)
  --truncate-logs   Clear all purge run history before starting (keeps tables)
  --show-sizes      Show data sizes per module to help choose purge depth
  --help, -h        Show this help message

Environment Variables:
  EPF_PURGE_PASSWORD   Database password (overrides config file and --password)

Examples:
  # Interactive mode (prompts for all inputs)
  ./epf_purge.sh

  # Dry run with 90-day retention
  ./epf_purge.sh --tns EPFPROD --user oppayments --retention 90 --dry-run

  # Using config file (for scheduled execution)
  ./epf_purge.sh --config ../config/epf_purge.conf

  # Purge only payment data with space reclamation
  ./epf_purge.sh --tns EPFPROD --user oppayments --depth PAYMENTS --reclaim --sys-password XXX

  # Reclaim only (after a previous purge)
  ./epf_purge.sh --tns EPFPROD --reclaim-only --sys-password XXX
HELPEOF
}

# ============================================================================
# Interactive prompts (used when arguments/config are not provided)
# ============================================================================
interactive_prompts() {
    log_header "EPF Data Purge - Configuration"

    if [[ -z "$TNS_NAME" ]]; then
        echo ""
        echo "  TNS Name / Connect String"
        echo "  This is the Oracle service name or TNS alias used to connect"
        echo "  to the database. Example: EPFPROD, localhost:1521/orcl"
        read -rp "  Enter TNS name: " TNS_NAME
    fi

    if [[ -z "$PASSWORD" ]]; then
        echo ""
        echo "  Database User: $USERNAME"
        echo "  Enter the password for this database user."
        read -rsp "  Password: " PASSWORD
        echo ""
    fi

    echo ""
    echo "  Retention Period"
    echo "  Data older than this many days will be purged."
    echo "  Current value: $RETENTION_DAYS days"
    read -rp "  Retention days [$RETENTION_DAYS]: " input
    RETENTION_DAYS="${input:-$RETENTION_DAYS}"

    echo ""
    echo "  Show Module Data Sizes (--show-sizes)"
    echo "  Queries the database to show data sizes per purge module"
    echo "  to help you choose the appropriate purge depth."
    read -rp "  Show data sizes? (Y/N) [$SHOW_SIZES]: " input
    SHOW_SIZES="${input:-$SHOW_SIZES}"

    if [[ "${SHOW_SIZES^^}" == "Y" ]]; then
        echo ""
        echo "  [INFO]  Querying data sizes..."
        sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" @"${SQL_DIR}/11_show_module_sizes.sql" 2>/dev/null
    fi

    echo ""
    echo "  Purge Depth"
    echo "  Controls which data modules are purged:"
    echo "    ALL             - Purge all modules (payments, logs, bank statements)"
    echo "    PAYMENTS        - Purge bulk payments and file integrations only"
    echo "    LOGS            - Purge audit trails and technical logs only"
    echo "    BANK_STATEMENTS - Purge bank statement dispatching only"
    read -rp "  Purge depth [$PURGE_DEPTH]: " input
    PURGE_DEPTH="${input:-$PURGE_DEPTH}"

    echo ""
    echo "  Batch Size"
    echo "  Number of parent records processed per commit. Larger = faster"
    echo "  but uses more undo/redo space. Recommended: 500-5000."
    read -rp "  Batch size [$BATCH_SIZE]: " input
    BATCH_SIZE="${input:-$BATCH_SIZE}"

    echo ""
    echo "  Dry Run"
    echo "  If yes, the tool will count how many rows would be deleted"
    echo "  without actually deleting anything. Good for a first test."
    read -rp "  Dry run? (Y/N) [$DRY_RUN]: " input
    DRY_RUN="${input:-$DRY_RUN}"

    echo ""
    echo "  Drop Package After Execution"
    echo "  If yes, the PL/SQL package will be removed from the database"
    echo "  after the purge completes. The log table is preserved."
    read -rp "  Drop package after? (Y/N) [$DROP_PACKAGE_AFTER]: " input
    DROP_PACKAGE_AFTER="${input:-$DROP_PACKAGE_AFTER}"

    echo ""
    echo "  Truncate Purge Logs (--truncate-logs)"
    echo "  Clears all previous purge run history from the log tables."
    echo "  Useful when re-running after a failed or test purge."
    read -rp "  Truncate logs? (Y/N) [$TRUNCATE_LOGS]: " input
    TRUNCATE_LOGS="${input:-$TRUNCATE_LOGS}"

    echo ""
    echo "  Pre-Purge Database Optimization (--optimize-db)"
    echo "  Enlarges redo logs to 1 GB and gathers optimizer statistics."
    echo "  Recommended for first-time purge on databases with small redo logs."
    echo "  Requires SYS/DBA credentials. Idempotent and auto-reverts on failure."
    echo "  >> Extra disk space: ~4 GB temporary (new redo logs before old ones deleted)"
    read -rp "  Optimize DB? (Y/N) [$OPTIMIZE_DB]: " input
    OPTIMIZE_DB="${input:-$OPTIMIZE_DB}"

    echo ""
    echo "  Post-Purge Space Reclaim (--reclaim)"
    echo "  After purge, shrinks and squeezes the tablespace to free OS disk space."
    echo "  Online operation (no downtime). Requires SYS/DBA credentials."
    echo "  >> No extra disk space needed (MOVE uses existing free space in tablespace)"
    read -rp "  Reclaim space? (Y/N) [$RECLAIM_SPACE]: " input
    RECLAIM_SPACE="${input:-$RECLAIM_SPACE}"

    if [[ "${RECLAIM_SPACE^^}" == "Y" ]]; then
        echo ""
        echo "  Skip Stall Checks (--no-stall-check)"
        echo "  When enabled, reclaim always runs all 2000 iterations without"
        echo "  stopping early on zero-progress checkpoints."
        read -rp "  Skip stall checks? (Y/N) [$SKIP_STALL_CHECKS]: " input
        SKIP_STALL_CHECKS="${input:-$SKIP_STALL_CHECKS}"
    fi

    # Prompt for SYS password now if optimize-db or reclaim enabled
    if [[ "${OPTIMIZE_DB^^}" == "Y" || "${RECLAIM_SPACE^^}" == "Y" ]]; then
        if [[ -z "$SYS_PASSWORD" ]]; then
            echo ""
            echo "  SYS/DBA password (needed for optimize-db and/or reclaim)"
            read -rsp "  SYS password: " SYS_PASSWORD
            echo ""
        fi
    fi
}

# ============================================================================
# Validate prerequisites
# ============================================================================
check_prerequisites() {
    log_header "Checking Prerequisites"
    local errors=0

    # Check sqlplus
    if command -v sqlplus &> /dev/null; then
        log_ok "SQL*Plus found: $(command -v sqlplus)"
    else
        log_error "SQL*Plus not found on PATH. Install Oracle Client and add sqlplus to PATH."
        errors=$((errors + 1))
    fi

    # Check ORACLE_HOME
    if [[ -n "${ORACLE_HOME:-}" ]]; then
        log_ok "ORACLE_HOME set: $ORACLE_HOME"
    else
        log_warn "ORACLE_HOME not set. SQL*Plus may still work if TNS is configured."
    fi

    # Check TNS_NAME
    if [[ -z "$TNS_NAME" ]]; then
        log_error "TNS name not specified. Use --tns or set TNS_NAME in config."
        errors=$((errors + 1))
    fi

    # Check password
    if [[ -z "$PASSWORD" ]]; then
        log_error "Password not provided. Use --password, EPF_PURGE_PASSWORD env var, or config file."
        errors=$((errors + 1))
    fi

    # Test database connectivity
    if [[ $errors -eq 0 ]]; then
        log_info "Testing database connectivity..."
        local test_result
        test_result=$(sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0
SELECT 'CONNECTION_OK' FROM DUAL;
EXIT;
SQLEOF
        )
        if echo "$test_result" | grep -q "CONNECTION_OK"; then
            log_ok "Database connection successful"
        else
            log_error "Database connection failed. Check credentials and TNS name."
            echo "$test_result" >> "$LOG_FILE"
            errors=$((errors + 1))
        fi
    fi

    # Check SQL files exist
    for sql_file in 01_create_purge_log_table.sql 02_epf_purge_pkg_spec.sql 03_epf_purge_pkg_body.sql; do
        if [[ -f "$SQL_DIR/$sql_file" ]]; then
            log_ok "Found: sql/$sql_file"
        else
            log_error "Missing: sql/$sql_file"
            errors=$((errors + 1))
        fi
    done

    if [[ $errors -gt 0 ]]; then
        log_error "$errors prerequisite(s) failed. Cannot proceed."
        exit 1
    fi

    log_ok "All prerequisites passed"
}

# ============================================================================
# Truncate purge log tables (clear old run history)
# ============================================================================
truncate_logs() {
    [[ "${TRUNCATE_LOGS^^}" != "Y" ]] && return

    log_info "Truncating purge log tables..."
    sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF' >/dev/null 2>&1
TRUNCATE TABLE oppayments.epf_purge_log;
TRUNCATE TABLE oppayments.epf_purge_space_snapshot;
EXIT;
SQLEOF
    log_ok "Purge logs truncated"
}

# ============================================================================
# Deploy PL/SQL package
# ============================================================================
deploy_package() {
    log_header "Deploying PL/SQL Package"

    local deploy_errors=0

    for sql_file in 01_create_purge_log_table.sql 02_epf_purge_pkg_spec.sql 03_epf_purge_pkg_body.sql; do
        log_info "Running: $sql_file"
        local output
        output=$(sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<SQLEOF
SET SERVEROUTPUT ON SIZE UNLIMITED
SET ECHO OFF FEEDBACK ON
@${SQL_DIR}/${sql_file}
EXIT;
SQLEOF
        )
        echo "$output" >> "$LOG_FILE"

        if echo "$output" | grep -qi "ORA-\|SP2-\|PLS-"; then
            log_error "Errors in $sql_file (check log for details)"
            deploy_errors=$((deploy_errors + 1))
        else
            log_ok "$sql_file executed successfully"
        fi
    done

    # Check for compilation errors
    log_info "Checking for package compilation errors..."
    local comp_errors
    comp_errors=$(sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 200
SELECT type || ': Line ' || line || ' - ' || text
FROM user_errors
WHERE name = 'EPF_PURGE_PKG'
ORDER BY type, sequence;
EXIT;
SQLEOF
    )

    if [[ -n "$(echo "$comp_errors" | xargs)" ]]; then
        log_error "Package has compilation errors:"
        echo "$comp_errors" | tee -a "$LOG_FILE"
        deploy_errors=$((deploy_errors + 1))
    else
        log_ok "Package compiled without errors"
    fi

    if [[ $deploy_errors -gt 0 ]]; then
        log_error "Deployment failed with $deploy_errors error(s). Aborting."
        exit 1
    fi
}

# ============================================================================
# Grant DBA view access for space snapshots (needs SYS)
# ============================================================================
# The space comparison needs dba_segments to match reclaim report numbers.
# Grants are idempotent and only run when SYS password is available.
grant_dba_views() {
    [[ -z "$SYS_PASSWORD" ]] && return

    log_info "Granting DBA view access to ${USERNAME} for space snapshots..."
    sqlplus -S "sys/${SYS_PASSWORD}@${TNS_NAME} AS SYSDBA" <<SQLEOF >> "$LOG_FILE" 2>&1
SET HEADING OFF FEEDBACK OFF
GRANT SELECT ON sys.dba_segments TO ${USERNAME};
GRANT SELECT ON sys.dba_lobs TO ${USERNAME};
EXIT;
SQLEOF
    log_ok "DBA view grants applied"
}

# ============================================================================
# Start background progress monitor
# ============================================================================
# Launches bin/epf_monitor.sh as a background process. Mirrors the Windows
# wrapper which launches bin/epf_monitor.ps1. The monitor polls epf_purge_log
# via SQL*Plus every 10s; each poll is a fresh invocation so output is NEVER
# buffered (unlike DBMS_OUTPUT which only flushes at block end).
#
# The monitor exits on RECLAIM_END, top-level ORCHESTRATOR ERROR, idle
# timeout, or when stop_monitor terminates it.
MONITOR_PID=""
MONITOR_SCRIPT="${SCRIPT_DIR}/epf_monitor.sh"
start_monitor() {
    # Verify sqlplus is available (should be, since check_prerequisites passed)
    if ! command -v sqlplus &>/dev/null; then
        log_warn "Monitor: sqlplus not found on PATH. Skipping live monitor."
        log_warn "Purge will continue without live progress. Check epf_purge_log table manually."
        return 0
    fi

    if [[ ! -f "$MONITOR_SCRIPT" ]]; then
        log_warn "Monitor script not found: $MONITOR_SCRIPT"
        log_warn "Purge will continue without live progress."
        return 0
    fi

    # Ensure executable (script may arrive without +x via git on Windows)
    [[ -x "$MONITOR_SCRIPT" ]] || chmod +x "$MONITOR_SCRIPT" 2>/dev/null || true

    log_info "Starting live progress monitor (polls epf_purge_log every 10s)"

    bash "$MONITOR_SCRIPT" \
        "${USERNAME}/${PASSWORD}@${TNS_NAME}" \
        10 \
        360 \
        "$LOG_FILE" &
    MONITOR_PID=$!
    # Give the monitor a moment to connect
    sleep 2

    # Verify the background process is actually running
    if ! kill -0 "$MONITOR_PID" 2>/dev/null; then
        log_warn "Monitor process (PID $MONITOR_PID) exited immediately."
        log_warn "Possible causes: DB connection issue, epf_purge_log table missing, or sqlplus error."
        log_warn "Purge will continue without live progress. Check epf_purge_log table manually:"
        log_warn "  SELECT message FROM oppayments.epf_purge_log ORDER BY log_id DESC FETCH FIRST 5 ROWS ONLY;"
        MONITOR_PID=""
    else
        log_ok "Monitor started (PID $MONITOR_PID)"
    fi
}

stop_monitor() {
    # Safe to call even if monitor was never started
    if [[ -z "$MONITOR_PID" ]]; then
        return 0
    fi
    if ! kill -0 "$MONITOR_PID" 2>/dev/null; then
        # Already exited on its own (normal - saw RUN_END)
        wait "$MONITOR_PID" 2>/dev/null || true
        MONITOR_PID=""
        return 0
    fi
    # Give the monitor up to 60s to see RUN_END and exit gracefully
    log_info "Waiting for monitor to detect completion..."
    local waited=0
    while kill -0 "$MONITOR_PID" 2>/dev/null && [[ $waited -lt 60 ]]; do
        sleep 2
        waited=$((waited + 2))
    done
    # If still running, terminate
    if kill -0 "$MONITOR_PID" 2>/dev/null; then
        kill "$MONITOR_PID" 2>/dev/null || true
        wait "$MONITOR_PID" 2>/dev/null || true
    fi
    MONITOR_PID=""
}

# ============================================================================
# Pre-purge: Tune UNDO to prevent excessive tablespace growth
# ============================================================================
# Bulk deletes generate large amounts of undo data. By default Oracle keeps
# expired undo for undo_retention seconds (typically 900s = 15 min).
# During a multi-hour purge this causes the undo tablespace to grow unbounded.
# We lower undo_retention to 60s and cap the datafile autoextend max to limit
# growth.  The original values are restored after the purge.
tune_undo_pre_purge() {
    [[ -z "$SYS_PASSWORD" ]] && return
    [[ "${DRY_RUN^^}" == "Y" ]] && return

    log_info "Tuning UNDO for bulk delete (retention=60s, maxsize=8G)"

    local sys_connect
    if [[ "${SYS_USER:-sys}" == "sys" ]]; then
        sys_connect="sys/${SYS_PASSWORD}@${TNS_NAME} AS SYSDBA"
    else
        sys_connect="${SYS_USER}/${SYS_PASSWORD}@${TNS_NAME}"
    fi

    sqlplus -S "${sys_connect}" <<'SQLEOF' 2>&1 | tee -a "$LOG_FILE"
SET SERVEROUTPUT ON SIZE UNLIMITED
SET HEADING OFF FEEDBACK OFF VERIFY OFF
DECLARE
    v_ret NUMBER;
BEGIN
    SELECT value INTO v_ret FROM v$parameter WHERE name = 'undo_retention';
    DBMS_OUTPUT.PUT_LINE('UNDO_RETENTION_ORIGINAL=' || v_ret);
    IF v_ret > 60 THEN
        EXECUTE IMMEDIATE 'ALTER SYSTEM SET undo_retention = 60';
        DBMS_OUTPUT.PUT_LINE('Lowered undo_retention from ' || v_ret || 's to 60s');
    ELSE
        DBMS_OUTPUT.PUT_LINE('undo_retention already low: ' || v_ret || 's');
    END IF;
    FOR f IN (SELECT file_name, maxbytes
              FROM dba_data_files
              WHERE tablespace_name = (SELECT value FROM v$parameter WHERE name = 'undo_tablespace')
                AND (autoextensible = 'YES' AND (maxbytes = 0 OR maxbytes > 8589934592)))
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER DATABASE DATAFILE ''' || f.file_name || ''' AUTOEXTEND ON MAXSIZE 8G';
            DBMS_OUTPUT.PUT_LINE('Capped autoextend: ' || f.file_name || ' maxsize=8G');
        EXCEPTION WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('Could not cap ' || f.file_name || ': ' || SQLERRM);
        END;
    END LOOP;
END;
/
EXIT;
SQLEOF
}

# ============================================================================
# Post-purge: Restore undo_retention to Oracle default
# ============================================================================
restore_undo_post_purge() {
    [[ -z "$SYS_PASSWORD" ]] && return
    [[ "${DRY_RUN^^}" == "Y" ]] && return

    log_info "Restoring undo_retention to 900s"

    local sys_connect
    if [[ "${SYS_USER:-sys}" == "sys" ]]; then
        sys_connect="sys/${SYS_PASSWORD}@${TNS_NAME} AS SYSDBA"
    else
        sys_connect="${SYS_USER}/${SYS_PASSWORD}@${TNS_NAME}"
    fi

    # Capture output so we can detect (and surface) failures. Without this the
    # purge can finish "successfully" while undo_retention stays at 60s.
    local restore_out
    restore_out=$(sqlplus -S "${sys_connect}" <<'SQLEOF' 2>&1
WHENEVER SQLERROR EXIT FAILURE
SET HEADING OFF FEEDBACK OFF VERIFY OFF
ALTER SYSTEM SET undo_retention = 900;
SELECT 'undo_retention=' || value FROM v$parameter WHERE name = 'undo_retention';
EXIT;
SQLEOF
    )
    local rc=$?
    echo "$restore_out" >> "$LOG_FILE"

    if [[ $rc -ne 0 ]] || echo "$restore_out" | grep -qi "ORA-\|SP2-"; then
        log_warn "FAILED to restore undo_retention to 900s. Current value may still be 60s."
        log_warn "Manual fix (as SYS): ALTER SYSTEM SET undo_retention = 900;"
        log_warn "Last sqlplus output:"
        echo "$restore_out" | sed 's/^/    /' | tee -a "$LOG_FILE" >&2
    elif echo "$restore_out" | grep -q "undo_retention=900"; then
        log_ok "undo_retention restored to 900s"
    else
        log_warn "undo_retention restore command ran but verification did not confirm 900s."
        log_warn "Output: $(echo "$restore_out" | tr -d '\n')"
    fi
}

# ============================================================================
# Execute purge
# ============================================================================
execute_purge() {
    log_header "Executing Purge"

    # Convert Y/N to PL/SQL TRUE/FALSE
    local dry_run_bool="FALSE"
    [[ "${DRY_RUN^^}" == "Y" ]] && dry_run_bool="TRUE"

    log_info "Parameters: retention=$RETENTION_DAYS days, depth=$PURGE_DEPTH, batch=$BATCH_SIZE"
    log_info "Options: dry_run=$dry_run_bool"

    # Stream sqlplus output live to console and log file simultaneously
    # (using tee pipe instead of variable capture so output appears in real time)
    sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<SQLEOF 2>&1 | tee -a "$LOG_FILE"
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET TIMING ON
SET ECHO OFF FEEDBACK OFF

BEGIN
    oppayments.epf_purge_pkg.run_purge(
        p_retention_days => ${RETENTION_DAYS},
        p_purge_depth    => '${PURGE_DEPTH}',
        p_batch_size     => ${BATCH_SIZE},
        p_dry_run        => ${dry_run_bool}
    );
END;
/

EXIT;
SQLEOF
    local sqlplus_exit=${PIPESTATUS[0]}

    # Check for ORA errors in log
    if [[ $sqlplus_exit -ne 0 ]] || grep -qi "ORA-\|ERROR" "$LOG_FILE" 2>/dev/null; then
        log_warn "Errors detected in purge output. Check log and epf_purge_log table."
    else
        log_ok "Purge execution completed"
    fi
}

# ============================================================================
# Post-reclaim: Capture AFTER space snapshot and print comparison
# ============================================================================
# Space comparison is done here (after reclaim) instead of inside run_purge
# because DELETE alone does not change segment sizes - only SHRINK/MOVE does.
capture_space_comparison() {
    [[ "${DRY_RUN^^}" == "Y" ]] && return

    log_info "Capturing post-reclaim space snapshot and comparison..."

    sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF' 2>&1 | tee -a "$LOG_FILE"
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET HEADING OFF FEEDBACK OFF
DECLARE
    l_run_id RAW(16);
BEGIN
    SELECT run_id INTO l_run_id FROM (
        SELECT run_id FROM oppayments.epf_purge_log
        WHERE operation = 'RUN_END'
        ORDER BY log_timestamp DESC
    ) WHERE ROWNUM = 1;
    -- Delete stale AFTER snapshot (captured right after purge, before reclaim)
    DELETE FROM oppayments.epf_purge_space_snapshot
    WHERE run_id = l_run_id AND snapshot_phase = 'AFTER';
    COMMIT;
    -- Capture fresh AFTER snapshot (post-reclaim segment sizes)
    oppayments.epf_purge_pkg.capture_space_snapshot(l_run_id, 'AFTER');
    oppayments.epf_purge_pkg.print_space_comparison(l_run_id);
END;
/
EXIT;
SQLEOF
}

# ============================================================================
# Display summary from database
# ============================================================================
display_summary() {
    log_header "Purge Results (from epf_purge_log)"

    sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF' 2>&1 | tee -a "$LOG_FILE"
SET LINESIZE 200 PAGESIZE 100
SET HEADING ON FEEDBACK OFF

COLUMN module        FORMAT A20
COLUMN status        FORMAT A10
COLUMN total_rows    FORMAT 999,999,999
COLUMN operations    FORMAT 999
COLUMN elapsed_sec   FORMAT 999,990.0

SELECT module,
       status,
       SUM(rows_affected) AS total_rows,
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

EXIT;
SQLEOF
}

# ============================================================================
# Optionally drop package
# ============================================================================
cleanup_package() {
    if [[ "${DROP_PACKAGE_AFTER^^}" == "Y" ]]; then
        log_header "Dropping PL/SQL Package"
        local output
        output=$(sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<SQLEOF
SET SERVEROUTPUT ON SIZE UNLIMITED
@${SQL_DIR}/04_drop_epf_purge_pkg.sql
EXIT;
SQLEOF
        )
        echo "$output" | tee -a "$LOG_FILE"
        log_ok "Package dropped"
    fi
}

# ============================================================================
# Optionally drop purge log tables
# ============================================================================
cleanup_logs() {
    if [[ "${DROP_LOGS^^}" == "Y" ]]; then
        log_header "Dropping Purge Log Tables"
        local output
        output=$(sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF'
DROP TABLE oppayments.epf_purge_space_snapshot PURGE;
DROP TABLE oppayments.epf_purge_log PURGE;
EXIT;
SQLEOF
        )
        echo "$output" | tee -a "$LOG_FILE"
        log_ok "Purge log tables dropped"
    fi
}

# ============================================================================
# Execute online tablespace reclaim (SHRINK + squeeze + resize)
# ============================================================================
execute_reclaim_online() {
    log_header "Online Tablespace Reclaim"

    log_info "Running: 05_reclaim_tablespace.sql (SHRINK + squeeze + resize)"
    log_info "This requires DBA/SYS credentials for ALTER DATABASE and DBA views."

    local sys_connect
    if [[ "${SYS_USER:-sys}" == "sys" ]]; then
        sys_connect="sys/${SYS_PASSWORD}@${TNS_NAME} AS SYSDBA"
    else
        sys_connect="${SYS_USER}/${SYS_PASSWORD}@${TNS_NAME}"
    fi

    sqlplus -S "${sys_connect}" <<SQLEOF 2>&1 | tee -a "$LOG_FILE"
DEFINE skip_stall_checks = ${SKIP_STALL_CHECKS}
@${SQL_DIR}/05_reclaim_tablespace.sql
EXIT;
SQLEOF
    local sqlplus_exit=${PIPESTATUS[0]}

    if [[ $sqlplus_exit -ne 0 ]]; then
        log_warn "Online reclaim returned non-zero exit code."
    else
        log_ok "Online reclaim completed"
    fi
}

# ============================================================================
# Main
# ============================================================================
main() {
    # Ensure log directory exists
    mkdir -p "$LOG_DIR"

    echo "EPF Data Purge Tool" | tee "$LOG_FILE"
    echo "Log file: $LOG_FILE" | tee -a "$LOG_FILE"
    echo "Started: $(date)" | tee -a "$LOG_FILE"

    # Parse arguments and load config
    parse_args "$@"
    load_config

    # --reclaim-only short-circuit: skip purge entirely
    if [[ "${RECLAIM_ONLY^^}" == "Y" ]]; then
        if [[ -z "$TNS_NAME" ]]; then
            echo ""
            echo "   ============================================================"
            echo "   EPF Space Reclaim (RECLAIM-ONLY MODE - no purge)"
            echo "   ============================================================"
            read -rp "  Enter TNS name: " TNS_NAME
        fi
        if [[ -z "$SYS_PASSWORD" ]]; then
            read -rsp "  SYS password: " SYS_PASSWORD
            echo ""
        fi
        log_info "Skipping purge. Running online reclaim only."
        execute_reclaim_online
        echo "" | tee -a "$LOG_FILE"
        log_ok "Online reclaim completed. Log: $LOG_FILE"
        echo "Finished: $(date)" >> "$LOG_FILE"
        exit 0
    fi

    # If key params missing, go interactive
    local _was_interactive="N"
    if [[ -z "$TNS_NAME" || -z "$PASSWORD" ]]; then
        _was_interactive="Y"
        interactive_prompts
    fi

    # Confirm settings
    log_header "Configuration Summary"
    log_info "TNS Name:       $TNS_NAME"
    log_info "Username:       $USERNAME"
    log_info "Retention:      $RETENTION_DAYS days"
    log_info "Purge Depth:    $PURGE_DEPTH"
    log_info "Batch Size:     $BATCH_SIZE"
    log_info "Dry Run:        $DRY_RUN"
    log_info "Optimize DB:    $OPTIMIZE_DB"
    log_info "Reclaim Space:  $RECLAIM_SPACE"
    if [[ "${RECLAIM_SPACE^^}" == "Y" ]]; then
        log_info "Skip Stall:     $SKIP_STALL_CHECKS"
    fi
    log_info "Drop Package:   $DROP_PACKAGE_AFTER"
    log_info "Truncate Logs:  $TRUNCATE_LOGS"
    log_info "Drop Logs:      $DROP_LOGS"

    # Disk space estimate
    echo "" | tee -a "$LOG_FILE"
    log_info "--- Approximate Disk Space Requirements ---"
    local est_total_gb=0
    log_info "  Purge:        ~2-5 GB temporary (UNDO growth, auto-recovered after retention)"
    est_total_gb=5
    if [[ "${OPTIMIZE_DB^^}" == "Y" ]]; then
        log_info "  Optimize DB:  ~4 GB temporary (4x1GB redo logs, old ones deleted after)"
        est_total_gb=$((est_total_gb + 4))
    fi
    if [[ "${RECLAIM_SPACE^^}" == "Y" ]]; then
        log_info "  Reclaim:      No extra space (uses existing free space in tablespace)"
    fi
    log_warn "  PEAK TOTAL:   ~${est_total_gb} GB of temporary free disk space required"
    log_info "  The purge itself frees space; this is only the temporary overhead during execution."

    # Execute steps
    check_prerequisites

    # Show module sizes if requested (non-interactive only; interactive already handled in prompts)
    if [[ "${SHOW_SIZES^^}" == "Y" && "$_was_interactive" != "Y" ]]; then
        echo ""
        log_info "Querying data sizes per module..."
        sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" @"${SQL_DIR}/11_show_module_sizes.sql" 2>/dev/null
    fi

    # --optimize-db: enlarge redo logs + gather stats (needs SYS)
    if [[ "${OPTIMIZE_DB^^}" == "Y" ]]; then
        if [[ -z "$SYS_PASSWORD" ]]; then
            echo ""
            echo "  DB optimization requires DBA/SYS credentials."
            read -rsp "  SYS password: " SYS_PASSWORD
            echo ""
        fi
        log_header "Pre-Purge Database Optimization"
        local sys_connect
        if [[ "${SYS_USER:-sys}" == "sys" ]]; then
            sys_connect="sys/${SYS_PASSWORD}@${TNS_NAME} AS SYSDBA"
        else
            sys_connect="${SYS_USER}/${SYS_PASSWORD}@${TNS_NAME}"
        fi
        sqlplus -S "${sys_connect}" <<SQLEOF 2>&1 | tee -a "$LOG_FILE"
@${SQL_DIR}/06_optimize_db.sql
EXIT;
SQLEOF
        local opt_exit=${PIPESTATUS[0]}
        if [[ $opt_exit -ne 0 ]]; then
            log_warn "DB optimization returned non-zero exit code."
        else
            log_ok "DB optimization completed"
        fi
    fi

    deploy_package
    grant_dba_views
    truncate_logs

    # Create temporary FK indexes for purge performance (with --optimize-db)
    if [[ "${OPTIMIZE_DB^^}" == "Y" && "${DRY_RUN^^}" != "Y" ]]; then
        log_info "Creating temporary FK indexes for purge performance..."
        sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF' >> "$LOG_FILE" 2>&1
SET SERVEROUTPUT ON SIZE UNLIMITED
@sql/06b_create_purge_indexes.sql
SQLEOF
        log_ok "Temporary FK indexes created"
    fi

    start_monitor
    tune_undo_pre_purge
    execute_purge
    restore_undo_post_purge
    display_summary

    # Drop temporary FK indexes (before reclaim to avoid extra segments)
    if [[ "${OPTIMIZE_DB^^}" == "Y" && "${DRY_RUN^^}" != "Y" ]]; then
        log_info "Dropping temporary FK indexes..."
        sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF' >> "$LOG_FILE" 2>&1
SET SERVEROUTPUT ON SIZE UNLIMITED
@sql/06c_drop_purge_indexes.sql
SQLEOF
        log_ok "Temporary FK indexes dropped"
    fi

    # --reclaim runs online space reclaim (SHRINK + squeeze + resize)
    if [[ "${RECLAIM_SPACE^^}" == "Y" ]]; then
        if [[ "${DRY_RUN^^}" == "Y" ]]; then
            log_info "Skipping space reclaim (dry run)"
        else
            if [[ -z "$SYS_PASSWORD" ]]; then
                echo ""
                echo "  Space reclaim requires DBA/SYS credentials."
                read -rsp "  SYS password: " SYS_PASSWORD
                echo ""
            fi
            # Drain delay: monitor polls every 10s, so the last few purge
            # batch lines may not have surfaced yet. Without this, the reclaim
            # header below interleaves with leftover BANK_STATEMENTS lines and
            # the "** PURGE COMPLETED **" marker.
            sleep 15
            execute_reclaim_online
            # Drain delay so the monitor has a chance to fetch RECLAIM_END
            # before stop_monitor terminates it (otherwise the final
            # "** RECLAIM COMPLETED **" line may never surface).
            sleep 15
        fi
    fi

    capture_space_comparison
    stop_monitor
    cleanup_package
    cleanup_logs

    echo "" | tee -a "$LOG_FILE"
    log_ok "EPF Data Purge completed. Log: $LOG_FILE"
    echo "Finished: $(date)" >> "$LOG_FILE"
}

main "$@"
