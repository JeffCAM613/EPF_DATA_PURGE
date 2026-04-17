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
SYS_PASSWORD=""
ASSUME_YES="N"
DROP_PACKAGE_AFTER="N"
DROP_LOGS="N"
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
            --sys-password) SYS_PASSWORD="$2"; shift 2 ;;
            --assume-yes|-y) ASSUME_YES="Y"; shift ;;
            --drop-pkg)     DROP_PACKAGE_AFTER="Y"; shift ;;
            --drop-logs)    DROP_LOGS="Y"; shift ;;
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
  --reclaim         After purge, invoke epf_tablespace_reclaim.sh
                    (export/import/recreate-as-BIGFILE; needs DBA creds)
  --reclaim-only    Skip purge entirely, run reclaim tool only
  --drop-pkg        Drop the PL/SQL package after execution
  --drop-logs       Drop purge log tables (epf_purge_log, epf_purge_space_snapshot)
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
  ./epf_purge.sh --tns EPFPROD --user oppayments --depth PAYMENTS --reclaim
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
    echo "  Space Reclamation"
    echo "  After purging, attempt to reclaim space within Oracle tablespaces"
    echo "  using SHRINK SPACE. This makes space reusable by Oracle but does"
    echo "  NOT reduce the OS-level file size."
    read -rp "  Reclaim space? (Y/N) [$RECLAIM_SPACE]: " input
    RECLAIM_SPACE="${input:-$RECLAIM_SPACE}"

    # Collect SYS password upfront so the reclaim step runs unattended.
    if [[ "${RECLAIM_SPACE^^}" == "Y" && -z "$SYS_PASSWORD" ]]; then
        echo ""
        echo "  Reclaim requires DBA/SYS credentials to drop and recreate"
        echo "  the tablespace. Enter the SYS password now so the reclaim"
        echo "  step runs unattended after the purge."
        read -rsp "  SYS password: " SYS_PASSWORD
        echo ""
    fi

    echo ""
    echo "  Drop Package After Execution"
    echo "  If yes, the PL/SQL package will be removed from the database"
    echo "  after the purge completes. The log table is preserved."
    read -rp "  Drop package after? (Y/N) [$DROP_PACKAGE_AFTER]: " input
    DROP_PACKAGE_AFTER="${input:-$DROP_PACKAGE_AFTER}"
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
            echo "   EPF Tablespace Reclaim (RECLAIM-ONLY MODE - no purge)"
            echo "   ============================================================"
            read -rp "  Enter TNS name: " TNS_NAME
        fi
        log_info "Skipping purge. Delegating to epf_tablespace_reclaim.sh"
        local -a reclaim_args=(--tns "$TNS_NAME")
        [[ -n "$SYS_PASSWORD" ]] && reclaim_args+=(--sys-password "$SYS_PASSWORD")
        [[ "${ASSUME_YES^^}" == "Y" ]] && reclaim_args+=(--assume-yes)
        exec "${SCRIPT_DIR}/epf_tablespace_reclaim.sh" "${reclaim_args[@]}"
    fi

    # If key params missing, go interactive
    if [[ -z "$TNS_NAME" || -z "$PASSWORD" ]]; then
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
    log_info "Reclaim Space:  $RECLAIM_SPACE"
    log_info "Drop Package:   $DROP_PACKAGE_AFTER"
    log_info "Drop Logs:      $DROP_LOGS"

    # Execute steps
    check_prerequisites
    deploy_package
    execute_purge
    display_summary

    # --reclaim delegates to the standalone tablespace reclaim tool
    # (export/import/recreate-as-BIGFILE). DBA credentials are prompted there.
    if [[ "${RECLAIM_SPACE^^}" == "Y" ]]; then
        if [[ "${DRY_RUN^^}" == "Y" ]]; then
            log_info "Skipping tablespace reclaim (dry run)"
        else
            log_header "Invoking Tablespace Reclaim Tool"
            log_info "--reclaim now delegates to epf_tablespace_reclaim.sh"
            log_info "(export/import/recreate-as-BIGFILE). DBA credentials required."
            "${SCRIPT_DIR}/epf_tablespace_reclaim.sh" --tns "${TNS_NAME}" \
                --sys-password "${SYS_PASSWORD}" --assume-yes \
                || log_warn "Tablespace reclaim did not complete successfully."
        fi
    fi

    cleanup_package
    cleanup_logs

    echo "" | tee -a "$LOG_FILE"
    log_ok "EPF Data Purge completed. Log: $LOG_FILE"
    echo "Finished: $(date)" >> "$LOG_FILE"
}

main "$@"
