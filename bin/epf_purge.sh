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
MONITOR_LOG_FILE="$LOG_DIR/epf_purge_${TIMESTAMP}_monitor.log"

TNS_NAME=""
USERNAME="oppayments"
PASSWORD=""
RETENTION_DAYS=30
PURGE_DEPTH="ALL"
PURGE_MODE="FULL"
BATCH_SIZE=1000
DRY_RUN="N"
RECLAIM_SPACE="N"
RECLAIM_ONLY="N"
SKIP_STALL_CHECKS="N"
ALLOW_OFFLINE_IDX="N"
OPTIMIZE_DB="N"
SYS_PASSWORD=""
ASSUME_YES="N"
DROP_PACKAGE_AFTER="N"
DROP_LOGS="N"
TRUNCATE_LOGS="N"
SHOW_SIZES="N"     # deprecated: sizes are now shown automatically
MAX_ITERATIONS=""  # empty = use recommendation based on tablespace size
CONFIG_FILE=""

# Auto-computed module sizes (populated by capture_module_sizes once DB is reachable)
EPF_PAY_GB=""
EPF_LOG_GB=""
EPF_BST_GB=""
EPF_TOTAL_GB=""
EPF_DATAFILE_GB=""
EPF_RECOMMENDED_MAX_ITER=""

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
# Per-param "_SUPPLIED=Y" markers let interactive_prompts skip ANY prompt
# whose value was already provided on the command line. Net effect:
# supplying a flag means you never see its prompt, regardless of which
# OTHER prompts are still triggered (e.g. you can pass --retention/--depth
# and only get prompted for password if no --password is given). When
# TNS+PASSWORD are both supplied the entire prompt block is skipped --
# this is the existing "single-command unattended run" path.
RETENTION_SUPPLIED="N"
DEPTH_SUPPLIED="N"
BATCH_SUPPLIED="N"
DRY_RUN_SUPPLIED="N"
DROP_PKG_SUPPLIED="N"
TRUNCATE_LOGS_SUPPLIED="N"
DROP_LOGS_SUPPLIED="N"
OPTIMIZE_DB_SUPPLIED="N"
RECLAIM_SUPPLIED="N"

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --config)       CONFIG_FILE="$2"; shift 2 ;;
            --tns)          TNS_NAME="$2"; shift 2 ;;
            --user)         USERNAME="$2"; shift 2 ;;
            --password)     PASSWORD="$2"; shift 2 ;;
            --retention)    RETENTION_DAYS="$2"; RETENTION_SUPPLIED="Y"; shift 2 ;;
            --depth)        PURGE_DEPTH="$2"; DEPTH_SUPPLIED="Y"; shift 2 ;;
            --mode)         PURGE_MODE="$2"; MODE_SUPPLIED="Y"; shift 2 ;;
            --batch-size)   BATCH_SIZE="$2"; BATCH_SUPPLIED="Y"; shift 2 ;;
            --dry-run)      DRY_RUN="Y"; DRY_RUN_SUPPLIED="Y"; shift ;;
            --no-dry-run)   DRY_RUN="N"; DRY_RUN_SUPPLIED="Y"; shift ;;
            --reclaim)      RECLAIM_SPACE="Y"; RECLAIM_SUPPLIED="Y"; shift ;;
            --no-reclaim)   RECLAIM_SPACE="N"; RECLAIM_SUPPLIED="Y"; shift ;;
            --reclaim-only) RECLAIM_ONLY="Y"; RECLAIM_SPACE="Y"; RECLAIM_SUPPLIED="Y"; shift ;;
            --reclaim-online)      RECLAIM_SPACE="Y"; RECLAIM_SUPPLIED="Y"; shift ;;  # legacy alias
            --reclaim-online-only) RECLAIM_ONLY="Y"; RECLAIM_SPACE="Y"; RECLAIM_SUPPLIED="Y"; shift ;;  # legacy alias
            --no-stall-check) SKIP_STALL_CHECKS="Y"; shift ;;
            --allow-offline-index-rebuild) ALLOW_OFFLINE_IDX="Y"; shift ;;
            --optimize-db)  OPTIMIZE_DB="Y"; OPTIMIZE_DB_SUPPLIED="Y"; shift ;;
            --no-optimize-db) OPTIMIZE_DB="N"; OPTIMIZE_DB_SUPPLIED="Y"; shift ;;
            --sys-password) SYS_PASSWORD="$2"; shift 2 ;;
            --assume-yes|-y) ASSUME_YES="Y"; shift ;;
            --drop-pkg)     DROP_PACKAGE_AFTER="Y"; DROP_PKG_SUPPLIED="Y"; shift ;;
            --no-drop-pkg)  DROP_PACKAGE_AFTER="N"; DROP_PKG_SUPPLIED="Y"; shift ;;
            --drop-logs)    DROP_LOGS="Y"; DROP_LOGS_SUPPLIED="Y"; shift ;;
            --truncate-logs) TRUNCATE_LOGS="Y"; TRUNCATE_LOGS_SUPPLIED="Y"; shift ;;
            --no-truncate-logs) TRUNCATE_LOGS="N"; TRUNCATE_LOGS_SUPPLIED="Y"; shift ;;
            --show-sizes)
                # Deprecated: module sizes are now always shown in the depth
                # prompt and configuration summary. Flag accepted for back-
                # compat; emits a one-time deprecation note later.
                SHOW_SIZES="Y"; shift ;;
            --max-iterations) MAX_ITERATIONS="$2"; shift 2 ;;
            --help|-h)      show_help; exit 0 ;;
            *)              log_error "Unknown argument: $1"; show_help; exit 1 ;;
        esac
    done
}

# ============================================================================
# Load configuration file
# ============================================================================
# Config file values are applied as DEFAULTS -- CLI arguments take precedence.
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
            # Only apply config values that were NOT already set via CLI
            case "$key" in
                TNS_NAME)           [[ -z "$TNS_NAME" ]] && TNS_NAME="$value" ;;
                USERNAME)           [[ "$USERNAME" == "oppayments" ]] && USERNAME="$value" ;;
                PASSWORD)           [[ -z "$PASSWORD" ]] && PASSWORD="$value" ;;
                SYS_PASSWORD)       [[ -z "$SYS_PASSWORD" ]] && SYS_PASSWORD="$value" ;;
                RETENTION_DAYS)     [[ "$RETENTION_SUPPLIED" == "N" ]] && RETENTION_DAYS="$value" ;;
                PURGE_DEPTH)        [[ "$DEPTH_SUPPLIED" == "N" ]] && PURGE_DEPTH="$value" ;;
                PURGE_MODE)         [[ "${MODE_SUPPLIED:-N}" == "N" ]] && PURGE_MODE="$value" ;;
                BATCH_SIZE)         [[ "$BATCH_SUPPLIED" == "N" ]] && BATCH_SIZE="$value" ;;
                DRY_RUN)            [[ "$DRY_RUN_SUPPLIED" == "N" ]] && DRY_RUN="$value" ;;
                RECLAIM_SPACE)      [[ "$RECLAIM_SUPPLIED" == "N" ]] && RECLAIM_SPACE="$value" ;;
                OPTIMIZE_DB)        [[ "$OPTIMIZE_DB_SUPPLIED" == "N" ]] && OPTIMIZE_DB="$value" ;;
                DROP_PACKAGE_AFTER) [[ "$DROP_PKG_SUPPLIED" == "N" ]] && DROP_PACKAGE_AFTER="$value" ;;
                TRUNCATE_LOGS)      [[ "$TRUNCATE_LOGS_SUPPLIED" == "N" ]] && TRUNCATE_LOGS="$value" ;;
                ASSUME_YES)         [[ "$ASSUME_YES" == "N" ]] && ASSUME_YES="$value" ;;
                DROP_LOGS)          [[ "${DROP_LOGS:-N}" == "N" ]] && DROP_LOGS="$value" ;;
            esac
        done < "$CONFIG_FILE"
    fi

    # Environment variable overrides config file for passwords
    if [[ -n "${EPF_PURGE_PASSWORD:-}" ]]; then
        PASSWORD="$EPF_PURGE_PASSWORD"
    fi
    # Parallel env var for SYS / DBA password so unattended runs that need
    # --reclaim or --optimize-db don't have to type the password interactively.
    if [[ -n "${EPF_SYS_PASSWORD:-}" ]]; then
        SYS_PASSWORD="$EPF_SYS_PASSWORD"
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
  --no-dry-run      Force dry-run off (overrides config file / earlier flag)
  --optimize-db     Run DB optimization before purge (enlarge redo logs, gather stats)
                    Needs DBA/SYS creds. ~4 GB temp disk space. Idempotent.
  --no-optimize-db  Force optimize-db off
  --reclaim         After purge, drop all OPPAYMENTS+OP indexes/constraints,
                    compact, resize datafiles, then recreate. App MUST be
                    quiesced (no writes) for the duration. Needs DBA/SYS creds.
  --no-reclaim      Force reclaim off
  --reclaim-only    Skip purge entirely, run reclaim only
  --max-iterations N  IGNORED by current reclaim path (kept for back-compat).
  --no-stall-check    IGNORED by current reclaim path (kept for back-compat).
  --allow-offline-index-rebuild
                      IGNORED by current reclaim path (always drops + recreates).
  --drop-pkg        Drop the PL/SQL package after execution
  --no-drop-pkg     Force drop-pkg off
  --drop-logs       Drop purge log tables (epf_purge_log, epf_purge_space_snapshot)
  --truncate-logs   Clear all purge run history before starting (keeps tables)
  --no-truncate-logs Force truncate-logs off
  --show-sizes      DEPRECATED: module sizes are now always shown automatically.
                    Flag accepted for back-compat; does nothing.
  --help, -h        Show this help message

Notes:
  * Any flag passed on the command line skips its corresponding interactive
    prompt. Pass --tns + --password (or set EPF_PURGE_PASSWORD) to skip ALL
    prompts and run unattended in one command.

Environment Variables:
  EPF_PURGE_PASSWORD   Database password (overrides config file and --password)
  EPF_SYS_PASSWORD     SYS / DBA password (overrides config file and --sys-password).
                       Use this for unattended runs with --reclaim or --optimize-db.

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
# Capture module sizes (DB connectivity required)
# ============================================================================
# Populates EPF_PAY_GB / EPF_LOG_GB / EPF_BST_GB / EPF_TOTAL_GB / EPF_DATAFILE_GB
# and EPF_EST_PAY / EPF_EST_LOG / EPF_EST_BST / EPF_EST_ALL / EPF_OTHER_GB / EPF_OTHER_PCT
# from sql/12_capture_module_sizes.sql. On failure, sets EPF_SIZE_ERR with a
# diagnostic hint (if available) and returns 1.
capture_module_sizes() {
    [[ -z "$TNS_NAME" || -z "$PASSWORD" ]] && return 1
    EPF_SIZE_ERR=""
    local raw_out out
    raw_out=$(sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" \
              @"${SQL_DIR}/12_capture_module_sizes.sql" "${RETENTION_DAYS}" 2>&1)
    out=$(echo "$raw_out" | grep '^EPF_SIZES|' | head -1)
    if [[ -z "$out" ]]; then
        # Check for diagnostic line from the SQL's outer exception handler
        local err_line
        err_line=$(echo "$raw_out" | grep '^EPF_ERROR|' | head -1)
        if [[ -n "$err_line" ]]; then
            EPF_SIZE_ERR="${err_line#EPF_ERROR|}"
        else
            # Fall back to first ORA-/SP2-/ERROR line from sqlplus itself
            EPF_SIZE_ERR=$(echo "$raw_out" | grep -i 'ORA-\|SP2-\|ERROR' | head -1)
        fi
        return 1
    fi
    IFS='|' read -r _ EPF_PAY_GB EPF_LOG_GB EPF_BST_GB EPF_TOTAL_GB EPF_DATAFILE_GB \
                      EPF_EST_PAY EPF_EST_LOG EPF_EST_BST EPF_EST_ALL EPF_OTHER_GB EPF_OTHER_PCT EPF_COVERAGE_GB \
                      EPF_CLOB_PAY_GB EPF_CLOB_LOG_GB EPF_CLOB_BST_GB EPF_CLOB_TOTAL_GB EPF_CLOB_PCT <<< "$out"
    # Parse per-table CLOB detail line (legacy 3-field format)
    local clob_detail
    clob_detail=$(echo "$raw_out" | grep '^EPF_CLOB_DETAIL|' | head -1)
    if [[ -n "$clob_detail" ]]; then
        IFS='|' read -r _ EPF_CLOB_DIR_DISP EPF_CLOB_FILE_DISP EPF_CLOB_TX_AUD <<< "$clob_detail"
    fi
    # Parse dynamic per-table CLOB lines (one per purge-covered table with LOBs)
    EPF_CLOB_TABLES=()
    EPF_CLOB_TABLE_COUNT=0
    while IFS='|' read -r _ label gb cols; do
        EPF_CLOB_TABLES+=("${label} (${gb} GB) [${cols}]")
        (( EPF_CLOB_TABLE_COUNT++ ))
    done < <(echo "$raw_out" | grep '^EPF_CLOB_TABLE|')
    return 0
}

# Recommend a max-iter value based on tablespace datafile size.
# Heuristic: max(2000, 50 * datafile_gb), capped at 20000.
# Falls back to 2000 if datafile size is unknown (no DBA grants).
compute_recommended_max_iter() {
    if [[ -z "$EPF_DATAFILE_GB" ]] || [[ "$EPF_DATAFILE_GB" == "0" ]] || [[ "$EPF_DATAFILE_GB" == "0.00" ]]; then
        EPF_RECOMMENDED_MAX_ITER=2000
        return
    fi
    local df_int
    df_int=$(printf '%.0f' "$EPF_DATAFILE_GB")
    local rec=$(( 50 * df_int ))
    (( rec < 2000  )) && rec=2000
    (( rec > 20000 )) && rec=20000
    EPF_RECOMMENDED_MAX_ITER=$rec
}

# ============================================================================
# Normalize PURGE_DEPTH: uppercase, strip spaces, ALL overrides everything
# ============================================================================
# Accepts comma-separated values like "PAYMENTS,LOGS" or single values.
normalize_depth() {
    PURGE_DEPTH="${PURGE_DEPTH// /}"
    PURGE_DEPTH="${PURGE_DEPTH^^}"
    # If ALL appears anywhere, collapse to ALL
    if [[ ",${PURGE_DEPTH}," == *",ALL,"* ]]; then
        PURGE_DEPTH="ALL"
    fi
}

# ============================================================================
# Normalize PURGE_MODE: uppercase, validate
# ============================================================================
normalize_mode() {
    PURGE_MODE="${PURGE_MODE^^}"
    if [[ "$PURGE_MODE" != "FULL" && "$PURGE_MODE" != "CLOB_ONLY" && "$PURGE_MODE" != "CLOB_N_LOGS" ]]; then
        log_warn "Invalid purge mode '$PURGE_MODE', defaulting to FULL"
        PURGE_MODE="FULL"
    fi
}

# ============================================================================
# Build scope summary from PURGE_DEPTH (supports comma-separated modules)
# ============================================================================
# Sets: EPF_SCOPE_GB, EPF_SCOPE_EST, EPF_SCOPE_TABLES
build_scope_summary() {
    EPF_SCOPE_GB="0"
    EPF_SCOPE_EST="0"
    EPF_SCOPE_TABLES=0
    if [[ "$PURGE_DEPTH" == "ALL" ]]; then
        EPF_SCOPE_GB="${EPF_COVERAGE_GB:-0.00}"
        EPF_SCOPE_EST="${EPF_EST_ALL:-0}"
        EPF_SCOPE_TABLES=27
        return
    fi
    local gb=0 est=0 tbl=0
    if [[ ",${PURGE_DEPTH}," == *",PAYMENTS,"* ]]; then
        gb=$(echo "$gb + ${EPF_PAY_GB:-0}" | bc 2>/dev/null || echo "$gb")
        est=$(echo "$est + ${EPF_EST_PAY:-0}" | bc 2>/dev/null || echo "$est")
        tbl=$(( tbl + 22 ))
    fi
    if [[ ",${PURGE_DEPTH}," == *",LOGS,"* ]]; then
        gb=$(echo "$gb + ${EPF_LOG_GB:-0}" | bc 2>/dev/null || echo "$gb")
        est=$(echo "$est + ${EPF_EST_LOG:-0}" | bc 2>/dev/null || echo "$est")
        tbl=$(( tbl + 3 ))
    fi
    if [[ ",${PURGE_DEPTH}," == *",BANK_STATEMENTS,"* ]]; then
        gb=$(echo "$gb + ${EPF_BST_GB:-0}" | bc 2>/dev/null || echo "$gb")
        est=$(echo "$est + ${EPF_EST_BST:-0}" | bc 2>/dev/null || echo "$est")
        tbl=$(( tbl + 2 ))
    fi
    EPF_SCOPE_GB=$(printf '%.2f' "$gb")
    EPF_SCOPE_EST=$(printf '%.2f' "$est")
    EPF_SCOPE_TABLES=$tbl
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

    if [[ -z "$SYS_PASSWORD" ]]; then
        echo ""
        echo "  SYS/DBA Password (optional)"
        echo "  Enables accurate tablespace sizing and is required later for"
        echo "  optimize-db and space reclaim. Press Enter to skip."
        read -rsp "  SYS password: " SYS_PASSWORD
        echo ""
    fi

    # Apply DBA grants early so capture_module_sizes can query dba_data_files.
    if [[ -n "$SYS_PASSWORD" ]]; then
        echo ""
        echo "  [INFO]  Granting DBA view access to ${USERNAME}..."
        grant_dba_views
    fi

    echo ""
    echo "  Retention Period"
    echo "  Data older than this many days will be purged."
    echo "  Current value: $RETENTION_DAYS days"
    if [[ "$RETENTION_SUPPLIED" != "Y" ]]; then
        read -rp "  Retention days [$RETENTION_DAYS]: " input
        RETENTION_DAYS="${input:-$RETENTION_DAYS}"
    else
        echo "  Using --retention $RETENTION_DAYS"
    fi

    # Capture module sizes for the depth prompt + max-iter recommendation.
    # Silent on failure; falls back to depth prompt without size hints.
    echo ""
    echo "  [INFO]  Querying current data sizes..."
    if capture_module_sizes; then
        compute_recommended_max_iter
        if [[ "$EPF_DATAFILE_GB" == "0.00" || "$EPF_DATAFILE_GB" == "0" ]]; then
            log_ok "Schema usage: ${EPF_TOTAL_GB} GB"
        else
            log_ok "Schema usage: ${EPF_TOTAL_GB} GB | datafiles: ${EPF_DATAFILE_GB} GB"
        fi
        log_ok "Purge coverage: ${EPF_EST_ALL} GB est. of ${EPF_COVERAGE_GB} GB | Outside coverage: ${EPF_OTHER_GB} GB (${EPF_OTHER_PCT}%)"
        log_ok "Breakdown: PAYMENTS=${EPF_PAY_GB} GB  LOGS=${EPF_LOG_GB} GB  BANK_STATEMENTS=${EPF_BST_GB} GB"
        log_ok "CLOBs: ${EPF_CLOB_TOTAL_GB} GB total (${EPF_CLOB_PCT}%) | PAY=${EPF_CLOB_PAY_GB} GB  LOGS=${EPF_CLOB_LOG_GB} GB  BST=${EPF_CLOB_BST_GB} GB"
        log_ok "Retention ${RETENTION_DAYS} days -- estimated purge: ~${EPF_EST_ALL} GB"
    else
        log_warn "Could not query data sizes -- depth prompt will not show GB hints."
        [[ -n "$EPF_SIZE_ERR" ]] && log_warn "  Reason: $EPF_SIZE_ERR"
        compute_recommended_max_iter
    fi

    echo ""
    echo "  Purge Mode"
    echo "  Controls what happens to matched rows:"
    echo ""
    echo "    FULL         Delete entire rows older than retention (default)"
    echo ""
    echo "    CLOB_ONLY    Clear CLOB content only -- rows preserved, LOB space freed"
    echo "                 UPDATE SET col = EMPTY_CLOB() on tables with LOBs:"
    if [[ ${#EPF_CLOB_TABLES[@]} -gt 0 ]]; then
        for entry in "${EPF_CLOB_TABLES[@]}"; do
            echo "                   ${entry}"
        done
        echo "                 Total CLOB reclaimable: ~${EPF_CLOB_TOTAL_GB} GB"
    fi
    echo ""
    echo "    CLOB_N_LOGS  CLOB_ONLY + full DELETE on LOGS module"
    echo "                 Clears CLOBs on above tables AND deletes log rows"
    echo "                 (audit_trail, audit_archive, op.spec_trt_log)"
    echo ""
    if [[ "${MODE_SUPPLIED:-N}" != "Y" ]]; then
        read -rp "  Purge mode [$PURGE_MODE]: " input
        PURGE_MODE="${input:-$PURGE_MODE}"
    else
        echo "  Using --mode $PURGE_MODE"
    fi
    normalize_mode

    # Only show Purge Depth for FULL mode -- CLOB modes cover all LOB tables in selected depth
    if [[ "$PURGE_MODE" == "FULL" ]]; then
    echo ""
    echo "  Purge Depth"
    echo "  Controls which data modules are purged:"
    if [[ -n "$EPF_TOTAL_GB" ]]; then
        local _purge_gb="${EPF_COVERAGE_GB:-0.00}"
        echo ""
        printf "    %-16s [ret:%s | ~%s GB of %s GB]\n" "ALL" "$RETENTION_DAYS" "$EPF_EST_ALL" "$_purge_gb"
        echo "                       Purge all modules (payments, logs, bank statements)"
        echo "                       (27 tables: bulk_payment, payment, file_integration,"
        echo "                        bulk_payment_additional_info, bulk_signature,"
        echo "                        mandatory_signers, oidc_request_token, payment_audit,"
        echo "                        payment_additional_info, import_audit, import_audit_messages,"
        echo "                        transmission_execution, transmission_execution_audit,"
        echo "                        transmission_exception, notification_execution,"
        echo "                        approbation_execution, approbation_execution_opt,"
        echo "                        workflow_execution, workflow_execution_opt,"
        echo "                        bulkpayment_exception, invoice, invoice_additional_info,"
        echo "                        audit_trail, audit_archive, op.spec_trt_log,"
        echo "                        file_dispatching, directory_dispatching)"
        echo ""
        printf "    %-16s [ret:%s | ~%s GB of %s GB]\n" "PAYMENTS" "$RETENTION_DAYS" "$EPF_EST_PAY" "$EPF_PAY_GB"
        echo "                       Purge bulk payments and file integrations only"
        echo "                       (22 tables: bulk_payment, payment, file_integration,"
        echo "                        bulk_payment_additional_info, bulk_signature,"
        echo "                        mandatory_signers, oidc_request_token, payment_audit,"
        echo "                        payment_additional_info, import_audit, import_audit_messages,"
        echo "                        transmission_execution, transmission_execution_audit,"
        echo "                        transmission_exception, notification_execution,"
        echo "                        approbation_execution, approbation_execution_opt,"
        echo "                        workflow_execution, workflow_execution_opt,"
        echo "                        bulkpayment_exception, invoice, invoice_additional_info)"
        echo ""
        printf "    %-16s [ret:%s | ~%s GB of %s GB]\n" "LOGS" "$RETENTION_DAYS" "$EPF_EST_LOG" "$EPF_LOG_GB"
        echo "                       Purge audit trails and technical logs only"
        echo "                       (3 tables: audit_trail, audit_archive, op.spec_trt_log)"
        echo ""
        printf "    %-16s [ret:%s | ~%s GB of %s GB]\n" "BANK_STATEMENTS" "$RETENTION_DAYS" "$EPF_EST_BST" "$EPF_BST_GB"
        echo "                       Purge bank statement dispatching only"
        echo "                       (2 tables: file_dispatching, directory_dispatching)"
        echo ""
        echo "    * Purge estimate is ROUGH -- based on row ratio in root tables, actual may vary."
        echo "    * ~${EPF_OTHER_GB} GB (${EPF_OTHER_PCT}%) of schema comes from tables outside purge coverage."
    else
        echo "    ALL             - Purge all modules (payments, logs, bank statements)"
        echo "    PAYMENTS        - Purge bulk payments and file integrations only"
        echo "    LOGS            - Purge audit trails and technical logs only"
        echo "    BANK_STATEMENTS - Purge bank statement dispatching only"
    fi
    echo ""
    echo "    Combine modules with commas: PAYMENTS,LOGS  PAYMENTS,BANK_STATEMENTS"
    echo "    If ALL appears in the list it overrides everything else."
    if [[ "$DEPTH_SUPPLIED" != "Y" ]]; then
        read -rp "  Purge depth [$PURGE_DEPTH]: " input
        PURGE_DEPTH="${input:-$PURGE_DEPTH}"
    else
        echo "  Using --depth $PURGE_DEPTH"
    fi
    normalize_depth
    fi

    echo ""
    echo "  Batch Size"
    echo "  Number of parent records processed per commit. Larger = faster"
    echo "  but uses more undo/redo space. Recommended: 500-5000."
    if [[ "$BATCH_SUPPLIED" != "Y" ]]; then
        read -rp "  Batch size [$BATCH_SIZE]: " input
        BATCH_SIZE="${input:-$BATCH_SIZE}"
    else
        echo "  Using --batch-size $BATCH_SIZE"
    fi

    echo ""
    echo "  Dry Run"
    echo "  If yes, the tool will count how many rows would be deleted"
    echo "  without actually deleting anything. Good for a first test."
    if [[ "$DRY_RUN_SUPPLIED" != "Y" ]]; then
        read -rp "  Dry run? (Y/N) [$DRY_RUN]: " input
        DRY_RUN="${input:-$DRY_RUN}"
    else
        echo "  Using --dry-run=$DRY_RUN"
    fi

    echo ""
    echo "  Drop Package After Execution"
    echo "  If yes, the PL/SQL package will be removed from the database"
    echo "  after the purge completes. The log table is preserved."
    if [[ "$DROP_PKG_SUPPLIED" != "Y" ]]; then
        read -rp "  Drop package after? (Y/N) [$DROP_PACKAGE_AFTER]: " input
        DROP_PACKAGE_AFTER="${input:-$DROP_PACKAGE_AFTER}"
    else
        echo "  Using --drop-pkg=$DROP_PACKAGE_AFTER"
    fi

    echo ""
    echo "  Truncate Purge Logs (--truncate-logs)"
    echo "  Clears all previous purge run history from the log tables."
    echo "  Useful when re-running after a failed or test purge."
    if [[ "$TRUNCATE_LOGS_SUPPLIED" != "Y" ]]; then
        read -rp "  Truncate logs? (Y/N) [$TRUNCATE_LOGS]: " input
        TRUNCATE_LOGS="${input:-$TRUNCATE_LOGS}"
    else
        echo "  Using --truncate-logs=$TRUNCATE_LOGS"
    fi

    echo ""
    echo "  Pre-Purge Database Optimization (--optimize-db)"
    echo "  Enlarges redo logs to 1 GB and gathers optimizer statistics."
    echo "  Recommended for first-time purge on databases with small redo logs."
    echo "  Requires SYS/DBA credentials. Idempotent and auto-reverts on failure."
    echo "  >> Extra disk space: ~4 GB temporary (new redo logs before old ones deleted)"
    if [[ "$OPTIMIZE_DB_SUPPLIED" != "Y" ]]; then
        read -rp "  Optimize DB? (Y/N) [$OPTIMIZE_DB]: " input
        OPTIMIZE_DB="${input:-$OPTIMIZE_DB}"
    else
        echo "  Using --optimize-db=$OPTIMIZE_DB"
    fi

    echo ""
    echo "  Post-Purge Space Reclaim (--reclaim)"
    echo "  After purge, reclaims OS disk space by dropping all OPPAYMENTS+OP"
    echo "  indexes/constraints, compacting tables, resizing the data (and"
    echo "  index, if separate) tablespace, then recreating everything."
    echo "  Requires SYS/DBA credentials. Application MUST be quiesced"
    echo "  (no writes) for the duration -- PK uniqueness is not enforced"
    echo "  between drop and recreate. Auto-detects tablespaces; works for"
    echo "  any tablespace name and for both shared and split data/index"
    echo "  layouts. You will see a confirmation banner before it runs."
    if [[ "$RECLAIM_SUPPLIED" != "Y" ]]; then
        read -rp "  Reclaim space? (Y/N) [$RECLAIM_SPACE]: " input
        RECLAIM_SPACE="${input:-$RECLAIM_SPACE}"
    else
        echo "  Using --reclaim=$RECLAIM_SPACE"
    fi

    if [[ "${RECLAIM_SPACE^^}" == "Y" ]]; then
        echo ""
        echo "  Reclaim mode: drop indexes / SHRINK / recreate."
        echo "  You will be shown a confirmation banner with full details"
        echo "  right before the reclaim runs."
        echo "  (legacy --max-iterations / --no-stall-check /"
        echo "   --allow-offline-index-rebuild flags are accepted but ignored)"
    fi

    # Prompt for SYS password if optimize-db or reclaim enabled but not yet provided
    if [[ "${OPTIMIZE_DB^^}" == "Y" || "${RECLAIM_SPACE^^}" == "Y" ]]; then
        if [[ -z "$SYS_PASSWORD" ]]; then
            echo ""
            echo "  SYS/DBA password (required for optimize-db / reclaim)"
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
    log_info "Deploying PL/SQL package..."

    local deploy_errors=0

    for sql_file in 01_create_purge_log_table.sql 02_epf_purge_pkg_spec.sql 03_epf_purge_pkg_body.sql; do
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
        fi
    done

    # Check for compilation errors
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
    fi

    if [[ $deploy_errors -gt 0 ]]; then
        log_error "Deployment failed with $deploy_errors error(s). Aborting."
        exit 1
    fi
    log_ok "Package deployed (log table, spec, body)"
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
GRANT SELECT ON sys.dba_data_files TO ${USERNAME};
EXIT;
SQLEOF
    log_ok "DBA view grants applied"
}

# ============================================================================
# Start background progress monitor
# ============================================================================
# Backgrounds bin/epf_monitor.sh with stdout suppressed so live updates don't
# interleave with the main wrapper console (which is reserved for summary
# lines). The monitor appends every line it produces to $MONITOR_LOG_FILE.
#
# To watch live progress in another terminal: tail -f "$MONITOR_LOG_FILE"
#
# This mirrors the Windows wrapper, which spawns the monitor in a separate
# console window for the same reason. Layout:
#   - This terminal    : summary lines only ([INFO]/[OK]/[WARN] from wrapper)
#   - tail -f          : live updates polled from epf_purge_log every 10s
#   - LOG_FILE         : wrapper output (config, sqlplus, summary, run log replay)
#   - MONITOR_LOG_FILE : live monitor output (separate file, no interleaving)
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

    log_info "Starting live progress monitor in background"
    log_info "  Monitor log: $MONITOR_LOG_FILE"
    log_info "This terminal will keep showing summary lines only."
    log_info "To watch live updates in another terminal:  tail -f \"$MONITOR_LOG_FILE\""

    # stdout/stderr to /dev/null so monitor lines don't interleave with the
    # wrapper output. The monitor's write_log() also appends each line to
    # $LOG_FILE directly, so nothing is lost.
    bash "$MONITOR_SCRIPT" \
        "${USERNAME}/${PASSWORD}@${TNS_NAME}" \
        10 \
        360 \
        "$MONITOR_LOG_FILE" >/dev/null 2>&1 &
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
    log_info "Executing purge (monitor window shows live progress)..."

    # Convert Y/N to PL/SQL TRUE/FALSE
    local dry_run_bool="FALSE"
    [[ "${DRY_RUN^^}" == "Y" ]] && dry_run_bool="TRUE"

    # Purge output goes to log file only. Monitor window shows live progress.
    sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<SQLEOF >> "$LOG_FILE" 2>&1
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 200
SET TIMING ON
SET ECHO OFF FEEDBACK OFF

BEGIN
    oppayments.epf_purge_pkg.run_purge(
        p_retention_days => ${RETENTION_DAYS},
        p_purge_depth    => '${PURGE_DEPTH}',
        p_batch_size     => ${BATCH_SIZE},
        p_dry_run        => ${dry_run_bool},
        p_purge_mode     => '${PURGE_MODE}'
    );
END;
/

EXIT;
SQLEOF
    local sqlplus_exit=$?

    # Check for ORA errors in log
    if [[ $sqlplus_exit -ne 0 ]] || grep -qi "ORA-\|ERROR" "$LOG_FILE" 2>/dev/null; then
        log_warn "Errors detected in purge output. Check log and epf_purge_log table."
    else
        log_ok "Purge execution completed"
    fi
}

# ============================================================================
# Post-purge/reclaim: Capture AFTER space snapshot and print comparison
# ============================================================================
# Space comparison is done here (after shrink/reclaim) instead of inside
# run_purge. DELETE alone does not change segment sizes; SHRINK (always run
# post-purge) and/or the full reclaim make the change visible.
capture_space_comparison() {
    [[ "${DRY_RUN^^}" == "Y" ]] && return

    log_info "Capturing space snapshot and comparison..."

    # If --reclaim was requested, check whether RECLAIM_END landed and with
    # what status. Drives the WARN banner before the table and the
    # "max-iter exhausted" recommendation banner after.
    local reclaim_end_status=""
    local reclaim_end_msg=""
    if [[ "${RECLAIM_SPACE^^}" == "Y" ]]; then
        local probe
        probe=$(sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF' 2>/dev/null | tr -d '\r'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 4000 TRIMSPOOL ON
SELECT NVL(status, 'MISSING') || '|' || NVL(REPLACE(message, CHR(10), ' '), '')
FROM (
    SELECT status, message FROM oppayments.epf_purge_log
    WHERE operation = 'RECLAIM_END'
    ORDER BY log_timestamp DESC
) WHERE ROWNUM = 1;
EXIT;
SQLEOF
        )
        reclaim_end_status="${probe%%|*}"
        reclaim_end_status=$(echo "$reclaim_end_status" | xargs)
        reclaim_end_msg="${probe#*|}"
        reclaim_end_msg=$(echo "$reclaim_end_msg" | xargs)

        if [[ -z "$reclaim_end_status" || "$reclaim_end_status" == "MISSING" ]]; then
            echo "" | tee -a "$LOG_FILE"
            log_warn "============================================================"
            log_warn "  WARNING: Reclaim was requested but no RECLAIM_END row was"
            log_warn "  found in epf_purge_log. The reclaim may not have run, or"
            log_warn "  it may have been killed before completion. The AFTER"
            log_warn "  snapshot below may not reflect the intended final state."
            log_warn "  See logs/epf_purge_*.log for details."
            log_warn "============================================================"
        elif [[ "$reclaim_end_status" == "ERROR" ]]; then
            echo "" | tee -a "$LOG_FILE"
            log_warn "============================================================"
            log_warn "  WARNING: Reclaim ended with status=ERROR. The AFTER"
            log_warn "  snapshot below may not reflect the intended final state."
            log_warn "  Reclaim message: ${reclaim_end_msg}"
            log_warn "============================================================"
        fi
    fi

    # Pass the depth used for THIS run so the comparison only shows relevant
    # modules. Heredoc uses double-quoted form so $PURGE_DEPTH is expanded.
    sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<SQLEOF 2>&1 | tee -a "$LOG_FILE"
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
    oppayments.epf_purge_pkg.print_space_comparison(l_run_id, '${PURGE_DEPTH}');
END;
/
EXIT;
SQLEOF

    # ----------------------------------------------------------------------
    # Post-reclaim banner: surface RECREATE failures (the only failure mode
    # the new drop-and-recreate path can leave behind needing user action).
    # The SQL writes RECLAIM_END status='WARNING' when one or more index /
    # constraint recreates failed.
    # ----------------------------------------------------------------------
    if [[ "${RECLAIM_SPACE^^}" == "Y" ]]; then
        local hit
        hit=$(sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF' 2>/dev/null | tr -d '\r'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 4000 TRIMSPOOL ON
SELECT message FROM (
    SELECT message FROM oppayments.epf_purge_log
    WHERE operation = 'RECLAIM_END'
      AND status = 'WARNING'
    ORDER BY log_timestamp DESC
) WHERE ROWNUM = 1;
EXIT;
SQLEOF
        )
        hit=$(echo "$hit" | xargs)
        if [[ -n "$hit" ]]; then
            echo "" | tee -a "$LOG_FILE"
            log_warn "============================================================"
            log_warn "  RECLAIM COMPLETED WITH RECREATE ERRORS"
            log_warn "  ${hit}"
            log_warn "  --"
            log_warn "  One or more indexes / constraints failed to recreate."
            log_warn "  Inspect the [FAILED: ...] list in the message above and"
            log_warn "  recreate them manually, OR re-run the reclaim (it captures"
            log_warn "  fresh DDL each run and will retry the failed objects)."
            log_warn "============================================================"
        fi
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
# Execute standalone SHRINK SPACE (post-purge segment compaction)
# ============================================================================
# Runs SHRINK SPACE on all tables to make purge results visible in the space
# comparison. Without this, DELETE leaves empty blocks inside segments and
# dba_segments reports no change. This is independent from reclaim: it always
# runs after purge (unless dry run), and reclaim-only also does its own shrink
# as part of the full reclaim flow.
execute_shrink() {
    log_info "Running SHRINK SPACE (in-place compaction, no index drop)..."

    sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<SQLEOF >> "$LOG_FILE" 2>&1
SET SERVEROUTPUT ON SIZE UNLIMITED
@${SQL_DIR}/05a_shrink_tables.sql
EXIT;
SQLEOF
    local sqlplus_exit=$?

    if [[ $sqlplus_exit -ne 0 ]]; then
        log_warn "SHRINK SPACE returned non-zero exit code."
    else
        log_ok "SHRINK SPACE completed"
    fi
}

# ============================================================================
# Execute online tablespace reclaim (drop indexes / SHRINK / recreate)
# ============================================================================
execute_reclaim_online() {
    log_info "Running reclaim (monitor window shows live progress)..."

    local sys_connect
    if [[ "${SYS_USER:-sys}" == "sys" ]]; then
        sys_connect="sys/${SYS_PASSWORD}@${TNS_NAME} AS SYSDBA"
    else
        sys_connect="${SYS_USER}/${SYS_PASSWORD}@${TNS_NAME}"
    fi

    # Positional args: target_pct_free, max_iterations (ignored), skip_stall_checks (ignored), allow_offline_idx (ignored)
    local effective_max_iter="${MAX_ITERATIONS:-${EPF_RECOMMENDED_MAX_ITER:-2000}}"
    local tmp_reclaim_out="${LOG_FILE%.log}_reclaim_detail.tmp"
    # Full reclaim output goes to temp file (live detail is in monitor log).
    # Summary banner is extracted to main log below.
    sqlplus -S "${sys_connect}" <<SQLEOF > "$tmp_reclaim_out" 2>&1
@${SQL_DIR}/05_reclaim_tablespace.sql 10 ${effective_max_iter} ${SKIP_STALL_CHECKS} ${ALLOW_OFFLINE_IDX}
EXIT;
SQLEOF
    local sqlplus_exit=$?

    # Extract summary banner (RECLAIM COMPLETE/FAILED/ABORTED through closing ====)
    # and append to main log + console. Full output stays in temp for debugging.
    local in_banner=0
    local eq_count=0
    while IFS= read -r line; do
        if [[ "$line" =~ ^[[:space:]]*RECLAIM\ (COMPLETE|FAILED|ABORTED) ]]; then
            in_banner=1
            eq_count=0
            local eq_line="============================================================"
            echo "" | tee -a "$LOG_FILE"
            echo "$eq_line" | tee -a "$LOG_FILE"
            echo "$line" | tee -a "$LOG_FILE"
        elif [[ $in_banner -eq 1 ]]; then
            echo "$line" | tee -a "$LOG_FILE"
            if [[ "$line" =~ ^==== ]]; then
                eq_count=$((eq_count + 1))
                if [[ $eq_count -ge 2 ]]; then
                    in_banner=0
                fi
            fi
        fi
    done < "$tmp_reclaim_out"
    rm -f "$tmp_reclaim_out"

    if [[ $sqlplus_exit -ne 0 ]]; then
        log_warn "Online reclaim returned non-zero exit code."
    else
        log_ok "Online reclaim completed"
    fi
}

# ============================================================================
# Reclaim warning banner + confirmation prompt
# ============================================================================
# Sets RECLAIM_CONFIRMED=Y if the user confirms (or --assume-yes), else N.
reclaim_warning_banner() {
    RECLAIM_CONFIRMED="N"
    cat <<'BANNER'

  ============================================================
  TABLESPACE RECLAIM - drop indexes / compact / recreate
  ============================================================
  This reclaim path will, against the OPPAYMENTS + OP schemas:

    1. Capture DDL for every PK/UK/FK constraint and every non-LOB
       index (non-constraint indexes only at this step).
    2. DROP every FK constraint, then every PK/UK constraint
       (Oracle drops their backing index too), then the remaining
       non-constraint indexes.
    3. SHRINK SPACE CASCADE on every table.
    4. Resize the data tablespace datafile(s) down to actual HWM.
    5. If a separate INDEX tablespace is detected, resize it too.
    6. Recreate every index, PK/UK and FK from the captured DDL.
       Indexes go back to their original tablespace.
    7. Final resize to capture true post-recreate footprint.

  IMPORTANT:
    * Indexes and PK uniqueness are NOT enforced during the window.
      The application must be quiesced (no writes) for the duration.
    * If the run fails AFTER drops and BEFORE recreates complete, the
      schema is in a partial state. Recovery: re-run the reclaim
      (captured DDL is rebuilt fresh from the DB each run) or restore
      from backup.
    * Tablespaces are auto-detected from OPPAYMENTS + OP metadata.
      No need to name them; works for any tablespace name and for
      both shared and separated data/index layouts.
  ============================================================

BANNER
    if [[ "${ASSUME_YES:-N}" == "Y" ]]; then
        log_info "--assume-yes set; proceeding without prompt."
        RECLAIM_CONFIRMED="Y"
        return 0
    fi
    # No interactive confirmation. The banner above is informational only --
    # the operator already opted in by passing --reclaim or --reclaim-only,
    # so a second Y/N prompt was just friction.
    RECLAIM_CONFIRMED="Y"
}

# ============================================================================
# write_final_summary -- user-friendly end-of-run summary written to log
# ============================================================================
# Reads epf_purge_log for the latest run and writes a self-contained recap
# at the very bottom of the log file: configuration, per-module purge totals,
# and reclaim status if reclaim was requested. Mirrors the .bat subroutine.
write_final_summary() {
    log_info ""
    log_info "================================================================================"
    if [[ "${RECLAIM_ONLY^^}" == "Y" ]]; then
        log_info "  EPF SPACE RECLAIM - FINAL RUN SUMMARY"
    else
        log_info "  EPF DATA PURGE - FINAL RUN SUMMARY"
    fi
    log_info "================================================================================"
    log_info "  [Configuration]"
    log_info "    TNS Name:       $TNS_NAME"
    log_info "    Username:       $USERNAME"
    if [[ "${RECLAIM_ONLY^^}" != "Y" ]]; then
        log_info "    Retention:      $RETENTION_DAYS days"
        log_info "    Depth:          $PURGE_DEPTH"
        log_info "    Mode:           $PURGE_MODE"
        log_info "    Batch Size:     $BATCH_SIZE"
        log_info "    Dry Run:        $DRY_RUN"
        log_info "    Reclaim:        $RECLAIM_SPACE"
    else
        log_info "    Reclaim-Only:   Y"
    fi
    log_info "  --------------------------------------------------------------------------------"

    if [[ -z "$TNS_NAME" || -z "$PASSWORD" ]]; then
        log_info "  (Skipping result blocks: no DB credentials available.)"
        log_info "================================================================================"
        return 0
    fi

    sqlplus -S "$USERNAME/$PASSWORD@$TNS_NAME" <<'SQL' | tee -a "$LOG_FILE"
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 200 TRIMSPOOL ON SERVEROUTPUT ON SIZE UNLIMITED
DECLARE
    l_purge_run_id  RAW(16);
    l_reclaim_run_id RAW(16);
    l_total_rows    NUMBER := 0;
    l_total_errors  NUMBER := 0;
    l_purge_start   TIMESTAMP;
    l_purge_end     TIMESTAMP;
    l_purge_secs    NUMBER;
    l_reclaim_secs  NUMBER;
    l_total_secs    NUMBER;
    l_has_reclaim   BOOLEAN := FALSE;
    -- Space metrics
    l_reclaim_start_msg VARCHAR2(4000);
    l_reclaim_end_msg   VARCHAR2(4000);
    l_df_before     NUMBER;
    l_df_after      NUMBER;
    l_hwm_before    NUMBER;
    l_hwm_after     NUMBER;
    l_used_before   NUMBER;
    l_used_after    NUMBER;
    l_df_pct        NUMBER;
    l_used_pct      NUMBER;
BEGIN
    -- Find purge run_id (latest RUN_END)
    BEGIN
        SELECT run_id INTO l_purge_run_id FROM (
            SELECT run_id FROM oppayments.epf_purge_log
            WHERE operation = 'RUN_END'
            ORDER BY log_timestamp DESC
        ) WHERE ROWNUM = 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN l_purge_run_id := NULL;
    END;

    -- Find reclaim run_id (latest RECLAIM_END; may equal purge_run_id or differ)
    BEGIN
        SELECT run_id INTO l_reclaim_run_id FROM (
            SELECT run_id FROM oppayments.epf_purge_log
            WHERE operation = 'RECLAIM_END'
            ORDER BY log_timestamp DESC
        ) WHERE ROWNUM = 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN l_reclaim_run_id := NULL;
    END;

    IF l_purge_run_id IS NULL AND l_reclaim_run_id IS NULL THEN
        DBMS_OUTPUT.PUT_LINE('  [Result]');
        DBMS_OUTPUT.PUT_LINE('    No completed run found.');
        RETURN;
    END IF;

    -- Purge timing (use purge run_id)
    IF l_purge_run_id IS NOT NULL THEN
        BEGIN
            SELECT log_timestamp INTO l_purge_start FROM oppayments.epf_purge_log
            WHERE run_id = l_purge_run_id AND operation = 'RUN_START' AND ROWNUM = 1;
            SELECT log_timestamp INTO l_purge_end FROM oppayments.epf_purge_log
            WHERE run_id = l_purge_run_id AND operation = 'RUN_END' AND ROWNUM = 1;
            l_purge_secs := ROUND(EXTRACT(DAY FROM (l_purge_end - l_purge_start))*86400
                                + EXTRACT(HOUR FROM (l_purge_end - l_purge_start))*3600
                                + EXTRACT(MINUTE FROM (l_purge_end - l_purge_start))*60
                                + EXTRACT(SECOND FROM (l_purge_end - l_purge_start)), 0);
        EXCEPTION WHEN NO_DATA_FOUND THEN l_purge_secs := NULL;
        END;

        -- Total rows deleted
        SELECT NVL(SUM(CASE WHEN status='SUCCESS' THEN rows_affected ELSE 0 END), 0),
               NVL(SUM(CASE WHEN status='ERROR' THEN 1 ELSE 0 END), 0)
        INTO l_total_rows, l_total_errors
        FROM oppayments.epf_purge_log
        WHERE run_id = l_purge_run_id AND operation = 'DELETE'
          AND module IN ('PAYMENTS','AUDIT_LOGS','TECH_LOGS','BANK_STATEMENTS','FILE_INTEGRATION');
    END IF;

    -- Reclaim info (use reclaim run_id, fall back to purge run_id)
    BEGIN
        SELECT message INTO l_reclaim_start_msg FROM oppayments.epf_purge_log
        WHERE run_id = NVL(l_reclaim_run_id, l_purge_run_id) AND operation = 'RECLAIM_START' AND ROWNUM = 1;
        SELECT REPLACE(message, CHR(10), ' '), elapsed_seconds
        INTO l_reclaim_end_msg, l_reclaim_secs
        FROM (SELECT message, elapsed_seconds FROM oppayments.epf_purge_log
              WHERE run_id = NVL(l_reclaim_run_id, l_purge_run_id) AND operation = 'RECLAIM_END'
              ORDER BY log_timestamp DESC) WHERE ROWNUM = 1;
        l_reclaim_secs := ROUND(l_reclaim_secs, 0);
        l_has_reclaim := TRUE;
        -- Parse from RECLAIM_END: "HWM 39.78->16.90GB, datafile 41.76->16.90GB"
        l_hwm_before := TO_NUMBER(REGEXP_SUBSTR(l_reclaim_end_msg, 'HWM (\d+\.?\d*)', 1, 1, NULL, 1));
        l_hwm_after  := TO_NUMBER(REGEXP_SUBSTR(l_reclaim_end_msg, 'HWM \d+\.?\d*->(\d+\.?\d*)', 1, 1, NULL, 1));
        l_df_before  := TO_NUMBER(REGEXP_SUBSTR(l_reclaim_end_msg, 'datafile (\d+\.?\d*)', 1, 1, NULL, 1));
        l_df_after   := TO_NUMBER(REGEXP_SUBSTR(l_reclaim_end_msg, 'datafile \d+\.?\d*->(\d+\.?\d*)', 1, 1, NULL, 1));
        -- Parse from RECLAIM_START: "used=39.62GB"
        l_used_before := TO_NUMBER(REGEXP_SUBSTR(l_reclaim_start_msg, 'used=(\d+\.?\d*)', 1, 1, NULL, 1));
        -- Tablespace used after from space snapshot (bat captures with purge run_id)
        BEGIN
            SELECT ROUND(SUM(size_mb) / 1024, 2) INTO l_used_after
            FROM oppayments.epf_purge_space_snapshot
            WHERE run_id = NVL(l_purge_run_id, l_reclaim_run_id) AND snapshot_phase = 'AFTER';
        EXCEPTION WHEN OTHERS THEN l_used_after := NULL;
        END;
    EXCEPTION WHEN NO_DATA_FOUND THEN l_has_reclaim := FALSE;
    END;

    -- Total duration
    DECLARE l_final_ts TIMESTAMP;
        l_reclaim_start_ts TIMESTAMP;
    BEGIN
        SELECT MAX(log_timestamp) INTO l_final_ts
        FROM oppayments.epf_purge_log WHERE run_id IN (NVL(l_purge_run_id, l_reclaim_run_id), NVL(l_reclaim_run_id, l_purge_run_id));
        IF l_purge_start IS NOT NULL THEN
            l_total_secs := ROUND(EXTRACT(DAY FROM (l_final_ts - l_purge_start))*86400
                                + EXTRACT(HOUR FROM (l_final_ts - l_purge_start))*3600
                                + EXTRACT(MINUTE FROM (l_final_ts - l_purge_start))*60
                                + EXTRACT(SECOND FROM (l_final_ts - l_purge_start)), 0);
        ELSIF l_has_reclaim THEN
            BEGIN
                SELECT log_timestamp INTO l_reclaim_start_ts FROM oppayments.epf_purge_log
                WHERE run_id = NVL(l_reclaim_run_id, l_purge_run_id) AND operation = 'RECLAIM_START' AND ROWNUM = 1;
                l_total_secs := ROUND(EXTRACT(DAY FROM (l_final_ts - l_reclaim_start_ts))*86400
                                    + EXTRACT(HOUR FROM (l_final_ts - l_reclaim_start_ts))*3600
                                    + EXTRACT(MINUTE FROM (l_final_ts - l_reclaim_start_ts))*60
                                    + EXTRACT(SECOND FROM (l_final_ts - l_reclaim_start_ts)), 0);
            EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
            END;
        END IF;
    END;

    -- Print result
    IF l_purge_secs IS NOT NULL THEN
        DBMS_OUTPUT.PUT_LINE('  [Purge Result]');
        DBMS_OUTPUT.PUT_LINE('    Rows Deleted:   ' || TRIM(TO_CHAR(l_total_rows, '999,999,999,999')));
        IF l_total_errors > 0 THEN
            DBMS_OUTPUT.PUT_LINE('    Errors:         ' || l_total_errors);
        END IF;
    END IF;

    IF l_has_reclaim THEN
        DBMS_OUTPUT.PUT_LINE('  [Reclaim Result]');
        IF l_df_before IS NOT NULL AND l_df_after IS NOT NULL THEN
            l_df_pct := ROUND((l_df_before - l_df_after) / NULLIF(l_df_before, 0) * 100, 1);
            DBMS_OUTPUT.PUT_LINE('    Datafile:       ' || l_df_before || ' GB -> ' || l_df_after || ' GB (-' || l_df_pct || '%)');
        END IF;
        IF l_hwm_before IS NOT NULL AND l_hwm_after IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('    HWM:            ' || l_hwm_before || ' GB -> ' || l_hwm_after || ' GB');
        END IF;
        IF l_used_before IS NOT NULL AND l_used_after IS NOT NULL THEN
            l_used_pct := ROUND((l_used_before - l_used_after) / NULLIF(l_used_before, 0) * 100, 1);
            DBMS_OUTPUT.PUT_LINE('    Tablespace:     ' || l_used_before || ' GB -> ' || l_used_after || ' GB (-' || l_used_pct || '%)');
        END IF;
    END IF;

    DBMS_OUTPUT.PUT_LINE('  --------------------------------------------------------------------------------');
    IF l_purge_secs IS NOT NULL AND l_has_reclaim THEN
        IF l_total_secs IS NOT NULL THEN
            DBMS_OUTPUT.PUT_LINE('    Total Duration: ' || l_total_secs || 's');
        END IF;
        DBMS_OUTPUT.PUT_LINE('      Purge:        ' || l_purge_secs || 's');
        DBMS_OUTPUT.PUT_LINE('      Reclaim:      ' || l_reclaim_secs || 's');
    ELSIF l_purge_secs IS NOT NULL THEN
        DBMS_OUTPUT.PUT_LINE('    Purge Duration: ' || l_purge_secs || 's');
    ELSIF l_has_reclaim AND l_reclaim_secs IS NOT NULL THEN
        DBMS_OUTPUT.PUT_LINE('    Reclaim Duration: ' || l_reclaim_secs || 's');
    END IF;
    IF l_has_reclaim AND l_hwm_after IS NOT NULL AND l_used_after IS NOT NULL AND l_hwm_after - l_used_after > 1 THEN
        DBMS_OUTPUT.PUT_LINE('  ********************************************************************************');
        DBMS_OUTPUT.PUT_LINE('  *** HWM NOT FULLY RECLAIMED ***');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Current HWM:  ' || l_hwm_after || ' GB   |   Actual Used:  ' || l_used_after || ' GB   |   Gap: ~' || ROUND(l_hwm_after - l_used_after, 2) || ' GB');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  Some segments could not be relocated in this pass. Re-running reclaim');
        DBMS_OUTPUT.PUT_LINE('  may move them and further reduce the datafile.');
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('  To reclaim more space, run:');
        DBMS_OUTPUT.PUT_LINE('    ./epf_purge.sh --reclaim-only --tns ${TNS_NAME} -y');
        DBMS_OUTPUT.PUT_LINE('  ********************************************************************************');
    END IF;
END;
/
EXIT;
SQL

    log_info "================================================================================"
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

    # Normalize PURGE_DEPTH early (handles --depth CLI / config file input)
    normalize_depth

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
        # OPPAYMENTS password enables: live monitor, space comparison, and
        # DB summary. Reclaim itself runs as SYS regardless; skipping it
        # just disables those extras.
        if [[ -z "$PASSWORD" ]]; then
            echo "  OPPAYMENTS password (optional, enables live monitor, space"
            echo "  comparison and DB summary; press Enter to skip):"
            read -rsp "  OPPAYMENTS password: " PASSWORD
            echo ""
        fi

        # ---- DBA grants (SYS is available) ----
        log_info "Granting DBA view access to $USERNAME..."
        sqlplus -S "sys/${SYS_PASSWORD}@${TNS_NAME} AS SYSDBA" <<'SQLEOF' >> "$LOG_FILE" 2>&1
SET HEADING OFF FEEDBACK OFF
GRANT SELECT ON sys.dba_segments TO oppayments;
GRANT SELECT ON sys.dba_lobs TO oppayments;
GRANT SELECT ON sys.dba_data_files TO oppayments;
EXIT;
SQLEOF
        log_ok "DBA view grants applied"

        # ---- Deploy PL/SQL package if OPPAYMENTS password available ----
        # Needed for post-reclaim space comparison. Idempotent.
        local _pkg_deployed="N"
        if [[ -n "$PASSWORD" ]]; then
            for sqlf in 01_create_purge_log_table.sql 02_epf_purge_pkg_spec.sql 03_epf_purge_pkg_body.sql; do
                sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<SQLEOF >> "$LOG_FILE" 2>&1
SET SERVEROUTPUT ON SIZE UNLIMITED
SET ECHO OFF FEEDBACK ON
@${SQL_DIR}/${sqlf}
EXIT;
SQLEOF
            done
            _pkg_deployed="Y"
        fi

        # ---- Configuration summary ----
        log_info ""
        log_info "============================================================"
        log_info "  Configuration Summary  (RECLAIM-ONLY)"
        log_info "============================================================"
        log_info "[Connection]"
        log_info "  TNS Name:       $TNS_NAME"
        log_info "  Username:       $USERNAME"
        log_info "[Reclaim]"
        log_info "  Log File:       $LOG_FILE"
        log_info "  Monitor Log:    $MONITOR_LOG_FILE"
        log_info "============================================================"
        log_info ""

        reclaim_warning_banner
        if [[ "$RECLAIM_CONFIRMED" != "Y" ]]; then
            log_info "Reclaim cancelled by user."
            echo "Finished: $(date)" >> "$LOG_FILE"
            exit 0
        fi

        if [[ -n "$PASSWORD" ]]; then
            start_monitor
        else
            log_info "Skipping live monitor (no OPPAYMENTS password). Tail $LOG_FILE for progress."
        fi

        log_info "Skipping purge. Running online reclaim only."
        log_info "Reclaim progress is shown in the monitor window."
        log_info "This terminal will wait silently until reclaim completes..."
        execute_reclaim_online

        if [[ -n "$MONITOR_PID" ]]; then
            sleep 15
            stop_monitor
        fi

        log_ok "Online reclaim completed"

        # ---- Post-reclaim: status check ----
        if [[ -n "$PASSWORD" ]]; then
            local probe
            probe=$(sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF' 2>/dev/null | tr -d '\r'
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 4000 TRIMSPOOL ON
SELECT NVL(status, 'MISSING') || '|' || NVL(REPLACE(message, CHR(10), ' '), '')
FROM (
    SELECT status, message FROM oppayments.epf_purge_log
    WHERE operation = 'RECLAIM_END'
    ORDER BY log_timestamp DESC
) WHERE ROWNUM = 1;
EXIT;
SQLEOF
            )
            local reclaim_end_status="${probe%%|*}"
            reclaim_end_status=$(echo "$reclaim_end_status" | xargs)
            local reclaim_end_msg="${probe#*|}"
            reclaim_end_msg=$(echo "$reclaim_end_msg" | xargs)
            if [[ -z "$reclaim_end_status" || "$reclaim_end_status" == "MISSING" ]]; then
                log_warn "No RECLAIM_END row found. Reclaim may not have completed."
            elif [[ "$reclaim_end_status" == "ERROR" ]]; then
                log_warn "Reclaim ended with status=ERROR: ${reclaim_end_msg}"
            fi
        fi

        # ---- Post-reclaim: space comparison (requires PL/SQL package) ----
        if [[ "$_pkg_deployed" == "Y" ]]; then
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
        WHERE operation IN ('RUN_END','RECLAIM_END')
        ORDER BY log_timestamp DESC
    ) WHERE ROWNUM = 1;
    DELETE FROM oppayments.epf_purge_space_snapshot
    WHERE run_id = l_run_id AND snapshot_phase = 'AFTER';
    COMMIT;
    oppayments.epf_purge_pkg.capture_space_snapshot(l_run_id, 'AFTER');
    oppayments.epf_purge_pkg.print_space_comparison(l_run_id, 'ALL');
END;
/
EXIT;
SQLEOF
        fi

        echo "" | tee -a "$LOG_FILE"
        write_final_summary
        log_ok "EPF Space Reclaim completed."
        log_ok "  Main log:    $LOG_FILE"
        log_ok "  Monitor log: $MONITOR_LOG_FILE"
        echo "Finished: $(date)" >> "$LOG_FILE"
        exit 0
    fi

    # If key params missing, go interactive
    local _was_interactive="N"
    if [[ -z "$TNS_NAME" || -z "$PASSWORD" ]]; then
        _was_interactive="Y"
        interactive_prompts
    fi

    # Confirm settings (grouped for readability)
    log_header "Configuration Summary"
    log_info "[Connection]"
    log_info "  TNS Name:       $TNS_NAME"
    log_info "  Username:       $USERNAME"
    log_info "[Purge]"
    log_info "  Retention:      $RETENTION_DAYS days"
    log_info "  Depth:          $PURGE_DEPTH"
    log_info "  Mode:           $PURGE_MODE"
    log_info "  Batch Size:     $BATCH_SIZE"
    log_info "  Dry Run:        $DRY_RUN"
    log_info "[Maintenance]"
    log_info "  Optimize DB:    $OPTIMIZE_DB"
    log_info "  Reclaim Space:  $RECLAIM_SPACE"
    log_info "  Drop Package:   $DROP_PACKAGE_AFTER"
    log_info "  Truncate Logs:  $TRUNCATE_LOGS"
    log_info "  Drop Logs:      $DROP_LOGS"
    log_info "  Log File:       $LOG_FILE"
    log_info "  Monitor Log:    $MONITOR_LOG_FILE"

    # Pre-run confirmation
    echo "" | tee -a "$LOG_FILE"
    log_info "--- Pre-run Confirmation ---"
    if [[ -n "$EPF_EST_ALL" ]]; then
        log_info "  Data retention:    ${RETENTION_DAYS} days"
        build_scope_summary
        log_info "  Purge scope:       ${PURGE_DEPTH} (${EPF_SCOPE_GB} GB across ${EPF_SCOPE_TABLES} tables)"
        if [[ "${PURGE_MODE}" == "CLOB_ONLY" ]]; then
            log_info "  Purge mode:        CLOB_ONLY (clear CLOB content, keep rows)"
            log_info "  Estimated impact:  ~${EPF_CLOB_TOTAL_GB} GB CLOB data across ${EPF_CLOB_TABLE_COUNT} tables"
        elif [[ "${PURGE_MODE}" == "CLOB_N_LOGS" ]]; then
            log_info "  Purge mode:        CLOB_N_LOGS (clear CLOBs + full delete on LOGS)"
            log_info "  Estimated impact:  ~${EPF_CLOB_TOTAL_GB} GB CLOBs + ~${EPF_LOG_GB} GB LOGS rows"
        else
            log_info "  Purge mode:        FULL (delete entire rows)"
            log_info "  Estimated purge:   ~${EPF_SCOPE_EST} GB (rough estimate based on row ratios)"
        fi
    fi
    log_info "  Batch size:        ${BATCH_SIZE} rows per commit"
    if [[ "${DRY_RUN^^}" == "Y" ]]; then
        log_info "  Dry run:            YES (count only, no data will be changed)"
    else
        log_info "  Dry run:            NO (live execution)"
    fi
    echo "" | tee -a "$LOG_FILE"
    log_info "  Disk overhead:     ~2-5 GB temporary UNDO growth (auto-recovered)"
    if [[ "${OPTIMIZE_DB^^}" == "Y" ]]; then
        log_info "  Optimize DB:       Yes (~4 GB temporary for redo logs)"
    else
        log_info "  Optimize DB:       No"
    fi
    if [[ "${RECLAIM_SPACE^^}" == "Y" ]]; then
        log_info "  Reclaim space:     Yes (iterative drain: peak overshoot = largest table + UNDO/redo)"
        echo "" | tee -a "$LOG_FILE"
        log_warn "  RECLAIM will drop all indexes, PKs, UKs, and FKs for the duration"
        log_warn "  of the operation. The application MUST be stopped/quiesced before"
        log_warn "  proceeding. If you only need to purge now, run without --reclaim"
        log_warn "  and use --reclaim-only later during a maintenance window."
    else
        log_info "  Reclaim space:     No"
    fi

    # Pre-run confirmation prompt
    if [[ "${ASSUME_YES:-N}" != "Y" ]]; then
        echo "" | tee -a "$LOG_FILE"
        local _confirm
        read -rp "  Proceed? (Y/N) [Y]: " _confirm
        if [[ "${_confirm^^}" == "N" ]]; then
            log_info "Aborted by user."
            return 0
        fi
    fi

    # Execute steps
    check_prerequisites

    # Auto-capture sizes for non-interactive runs (interactive_prompts already
    # did this). Used for the max-iter recommendation in execute_reclaim_online
    # and matches the data the operator would have seen interactively.
    if [[ "$_was_interactive" != "Y" && -z "$EPF_TOTAL_GB" ]]; then
        if capture_module_sizes; then
            compute_recommended_max_iter
            log_ok "Breakdown: PAYMENTS=${EPF_PAY_GB} GB  LOGS=${EPF_LOG_GB} GB  BANK_STATEMENTS=${EPF_BST_GB} GB"
            log_ok "Retention ${RETENTION_DAYS} days -- estimated purge: ~${EPF_EST_ALL} GB"
            if [[ "${RECLAIM_SPACE^^}" == "Y" && -z "$MAX_ITERATIONS" ]]; then
                log_info "Recommended max_iterations for ${EPF_DATAFILE_GB}GB tablespace: $EPF_RECOMMENDED_MAX_ITER"
            fi
        fi
    fi

    # --show-sizes is deprecated -- sizes are always captured + shown above
    if [[ "${SHOW_SIZES^^}" == "Y" ]]; then
        log_warn "--show-sizes is deprecated; sizes are now always shown automatically. Flag accepted but does nothing."
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
        local fkidx_out
        fkidx_out=$(sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF'
SET SERVEROUTPUT ON SIZE UNLIMITED
@sql/06b_create_purge_indexes.sql
SQLEOF
        )
        echo "$fkidx_out" >> "$LOG_FILE"
        # Extract summary line to console
        local fk_summary
        fk_summary=$(echo "$fkidx_out" | grep "^Summary:" | head -1)
        if [[ -n "$fk_summary" ]]; then
            log_ok "FK indexes: $fk_summary"
        else
            log_ok "Temporary FK indexes created"
        fi
    fi

    start_monitor
    tune_undo_pre_purge
    execute_purge
    restore_undo_post_purge
    display_summary

    # Post-purge SHRINK SPACE: make purge results visible in space comparison.
    # Runs independently of reclaim -- even when reclaim is skipped, the space
    # comparison should show meaningful segment-level change.
    if [[ "${DRY_RUN^^}" != "Y" ]]; then
        execute_shrink
    fi

    # Drop temporary FK indexes (before reclaim to avoid extra segments)
    if [[ "${OPTIMIZE_DB^^}" == "Y" && "${DRY_RUN^^}" != "Y" ]]; then
        log_info "Dropping temporary FK indexes..."
        sqlplus -S "${USERNAME}/${PASSWORD}@${TNS_NAME}" <<'SQLEOF' >> "$LOG_FILE" 2>&1
SET SERVEROUTPUT ON SIZE UNLIMITED
@sql/06c_drop_purge_indexes.sql
EXIT;
SQLEOF
        log_ok "Temporary FK indexes dropped"
    fi

    # --reclaim runs the drop-and-recreate reclaim (DDL capture, drop FK/PK/UK
    # + indexes, SHRINK tables, resize datafiles, recreate everything).
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
            reclaim_warning_banner
            if [[ "$RECLAIM_CONFIRMED" != "Y" ]]; then
                log_info "Reclaim cancelled by user. Purge already completed."
            else
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
    fi

    capture_space_comparison

    # ========================================================================
    # Shrink UNDO and TEMP tablespaces (runs in ALL modes).
    # When reclaim ran, its Step 14 already did this; skip the duplicate.
    # For purge-only, this is the only place UNDO/TEMP get shrunk.
    # ========================================================================
    if [[ "${RECLAIM_SPACE^^}" != "Y" ]] && [[ -n "${SYS_PASSWORD:-}" ]] && [[ "${DRY_RUN^^}" != "Y" ]]; then
        log_info "Shrinking UNDO and TEMP tablespaces..."
        sqlplus -S "sys/${SYS_PASSWORD}@${TNS_NAME} AS SYSDBA" <<'SQLEOF' >> "$LOG_FILE" 2>&1
SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK OFF
DECLARE
    v_undo_ts   VARCHAR2(128);
    v_undo_file VARCHAR2(513);
    v_undo_gb   NUMBER;
    v_ok        BOOLEAN := FALSE;
BEGIN
    BEGIN
        EXECUTE IMMEDIATE 'SELECT value FROM v$parameter WHERE name = ''undo_tablespace''' INTO v_undo_ts;
        EXECUTE IMMEDIATE 'SELECT file_name, ROUND(bytes/1024/1024/1024, 2) FROM dba_data_files WHERE tablespace_name = :1 FETCH FIRST 1 ROW ONLY' INTO v_undo_file, v_undo_gb USING v_undo_ts;
    EXCEPTION WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('  Could not identify UNDO: ' || SQLERRM);
        GOTO skip_undo;
    END;
    DBMS_OUTPUT.PUT_LINE('  UNDO: ' || v_undo_ts || ' (' || v_undo_gb || ' GB)');
    IF v_undo_gb < 2 THEN
        DBMS_OUTPUT.PUT_LINE('  UNDO already small. Skipping.');
        GOTO skip_undo;
    END IF;
    FOR tg IN 1..4 LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER DATABASE DATAFILE ''' || v_undo_file || ''' RESIZE ' || tg || 'G';
            DBMS_OUTPUT.PUT_LINE('  UNDO resized to ' || tg || ' GB (freed ' || ROUND(v_undo_gb - tg, 2) || ' GB)');
            v_ok := TRUE;
            EXIT;
        EXCEPTION WHEN OTHERS THEN NULL;
        END;
    END LOOP;
    IF NOT v_ok THEN
        DBMS_OUTPUT.PUT_LINE('  UNDO resize failed (active undo prevents shrink). Skipping.');
    END IF;
    <<skip_undo>> NULL;
END;
/
BEGIN
    FOR f IN (SELECT file_name, bytes, ROUND(bytes/1024/1024/1024, 2) AS size_gb FROM dba_temp_files WHERE bytes > 1073741824 ORDER BY file_name) LOOP
        BEGIN
            EXECUTE IMMEDIATE 'ALTER DATABASE TEMPFILE ''' || f.file_name || ''' RESIZE 1G';
            DBMS_OUTPUT.PUT_LINE('  TEMP ' || f.file_name || ': ' || f.size_gb || ' GB -> 1 GB');
        EXCEPTION WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('  TEMP ' || f.file_name || ' could not shrink: ' || SQLERRM);
        END;
    END LOOP;
END;
/
EXIT;
SQLEOF
        log_ok "UNDO/TEMP shrink completed"
    fi

    stop_monitor
    cleanup_package
    cleanup_logs

    write_final_summary

    echo "" | tee -a "$LOG_FILE"
    log_ok "EPF Data Purge completed."
    log_ok "  Main log:    $LOG_FILE"
    log_ok "  Monitor log: $MONITOR_LOG_FILE"
    echo "Finished: $(date)" >> "$LOG_FILE"
}

main "$@"
