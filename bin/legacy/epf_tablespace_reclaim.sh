#!/bin/bash
# ============================================================================
# EPF Tablespace Reclaim - Linux/Unix Script
# ============================================================================
# Reclaims tablespace disk space by exporting all schemas that share the same
# tablespace as OPPAYMENTS, dropping the tablespace, recreating it as a
# bigfile autoextending tablespace, and reimporting all schemas.
#
# This is the "nuclear option" for space reclamation -- use when SHRINK SPACE
# and RESIZE are insufficient to free OS-level disk space.
#
# Usage:
#   Interactive:   ./epf_tablespace_reclaim.sh
#   With args:     ./epf_tablespace_reclaim.sh --tns EPFPROD --sys-password XXX
#
# Prerequisites:
#   - Oracle SQL*Plus installed and on PATH
#   - Oracle Data Pump (expdp/impdp) installed and on PATH
#   - ORACLE_HOME environment variable set
#   - SYS or DBA-privileged user credentials
#   - Sufficient disk space for export dump files
# ============================================================================

set -euo pipefail

# ============================================================================
# Defaults
# ============================================================================
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG_DIR="$PROJECT_DIR/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/epf_tablespace_reclaim_${TIMESTAMP}.log"

TNS_NAME=""
DBA_USER="sys"
DBA_PASSWORD=""
OPPAYMENTS_USER="oppayments"
DATAFILE_PATH=""
DATAFILE_SIZE="10G"
AUTOEXTEND_NEXT="1G"
AUTOEXTEND_MAXSIZE="UNLIMITED"
ASSUME_YES="N"
HOLD_TS_NAME=""  # Will be set to <TABLESPACE>_HOLD

# PDB/non-PDB detection
IS_PDB="N"

# ============================================================================
# Color output helpers
# ============================================================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info()    { echo -e "${BLUE}[INFO]${NC}  $1" | tee -a "$LOG_FILE"; }
log_ok()      { echo -e "${GREEN}[OK]${NC}    $1" | tee -a "$LOG_FILE"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $1" | tee -a "$LOG_FILE"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"; }
log_step()    { echo -e "\n${CYAN}>>> $1${NC}" | tee -a "$LOG_FILE"; }
log_header()  { echo -e "\n${BLUE}============================================================${NC}" | tee -a "$LOG_FILE"
                echo -e "${BLUE}  $1${NC}" | tee -a "$LOG_FILE"
                echo -e "${BLUE}============================================================${NC}" | tee -a "$LOG_FILE"; }

# ============================================================================
# Parse command-line arguments
# ============================================================================
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --tns)              TNS_NAME="$2"; shift 2 ;;
            --dba-user)         DBA_USER="$2"; shift 2 ;;
            --dba-password)     DBA_PASSWORD="$2"; shift 2 ;;
            --sys-password)     DBA_USER="sys"; DBA_PASSWORD="$2"; shift 2 ;;
            --oppayments-user)  OPPAYMENTS_USER="$2"; shift 2 ;;
            --datafile-path)    DATAFILE_PATH="$2"; shift 2 ;;
            --datafile-size)    DATAFILE_SIZE="$2"; shift 2 ;;
            --autoextend-next)  AUTOEXTEND_NEXT="$2"; shift 2 ;;
            --autoextend-max)   AUTOEXTEND_MAXSIZE="$2"; shift 2 ;;
            --assume-yes|-y)    ASSUME_YES="Y"; shift ;;
            --help|-h)          show_help; exit 0 ;;
            *)                  log_error "Unknown argument: $1"; show_help; exit 1 ;;
        esac
    done

    # Environment variable overrides
    if [[ -n "${EPF_DBA_PASSWORD:-}" ]]; then
        DBA_PASSWORD="$EPF_DBA_PASSWORD"
    fi
}

# ============================================================================
# Show help
# ============================================================================
show_help() {
    cat << 'HELPEOF'
EPF Tablespace Reclaim Tool
============================

Reclaims disk space by exporting all schemas sharing the OPPAYMENTS tablespace,
dropping and recreating the tablespace as bigfile + autoextend, then reimporting.

Usage:
  epf_tablespace_reclaim.sh [OPTIONS]

Options:
  --tns NAME             Oracle TNS name or connect string (required)
  --dba-user NAME        DBA username (default: sys)
  --dba-password PASS    DBA password (prefer EPF_DBA_PASSWORD env var)
  --sys-password PASS    Shortcut: sets --dba-user=sys --dba-password=PASS
  --oppayments-user NAME OPPAYMENTS schema name (default: oppayments)
  --datafile-path PATH   Path for the new datafile (auto-detected if omitted)
  --datafile-size SIZE   Initial size of new datafile (default: 10G)
  --autoextend-next SIZE Autoextend increment (default: 1G)
  --autoextend-max SIZE  Autoextend max size (default: UNLIMITED)
  --help, -h             Show this help message

Environment Variables:
  EPF_DBA_PASSWORD       DBA password (overrides --dba-password / --sys-password)

Flow:
  1. Discover OPPAYMENTS default tablespace
  2. Find all schemas using that tablespace
  3. Detect PDB or non-PDB environment
  4. Detect Data Pump directory (PDB_DATA_PUMP_DIR or DATA_PUMP_DIR)
  5. Export all affected schemas via expdp
  6. Create temporary holding tablespace
  7. Switch database/PDB default tablespace to holding TS (if applicable)
  8. Reassign all affected users to holding tablespace
  9. Drop the original tablespace (INCLUDING CONTENTS AND DATAFILES)
  10. Recreate tablespace as BIGFILE with AUTOEXTEND ON
  11. Switch database/PDB default tablespace back
  12. Reassign all users back to the recreated tablespace
  13. Import all schemas via impdp
  14. Verify imported objects
  15. Drop the holding tablespace

IMPORTANT: This requires application downtime. Ensure no sessions are active.
HELPEOF
}

# ============================================================================
# Build sqlplus connect string (handles SYS AS SYSDBA)
# ============================================================================
sqlplus_connect() {
    if [[ "${DBA_USER,,}" == "sys" ]]; then
        echo "${DBA_USER}/${DBA_PASSWORD}@${TNS_NAME} AS SYSDBA"
    else
        echo "${DBA_USER}/${DBA_PASSWORD}@${TNS_NAME}"
    fi
}

# ============================================================================
# Run a SQL query and return the result (trimmed)
# ============================================================================
run_sql() {
    local sql="$1"
    local connect_str
    connect_str=$(sqlplus_connect)
    local result
    result=$(sqlplus -S "${connect_str}" <<SQLEOF
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON
${sql}
EXIT;
SQLEOF
    )
    # Trim whitespace
    echo "$result" | sed '/^$/d' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

# ============================================================================
# Interactive prompts
# ============================================================================
interactive_prompts() {
    log_header "EPF Tablespace Reclaim - Configuration"

    if [[ -z "$TNS_NAME" ]]; then
        echo ""
        echo "  TNS Name / Connect String"
        echo "  The Oracle service name or TNS alias to connect to."
        echo "  Example: EPFPROD, localhost:1521/orcl"
        read -rp "  Enter TNS name: " TNS_NAME
    fi

    if [[ -z "$DBA_PASSWORD" ]]; then
        echo ""
        echo "  DBA User: $DBA_USER"
        echo "  Enter the password for the DBA user."
        echo "  (This user must have DBA/SYSDBA privileges)"
        read -rsp "  Password: " DBA_PASSWORD
        echo ""
    fi
}

# ============================================================================
# Check prerequisites
# ============================================================================
check_prerequisites() {
    log_header "Checking Prerequisites"
    local errors=0

    # Check sqlplus
    if command -v sqlplus &> /dev/null; then
        log_ok "SQL*Plus found: $(command -v sqlplus)"
    else
        log_error "SQL*Plus not found on PATH."
        errors=$((errors + 1))
    fi

    # Check expdp / impdp
    if command -v expdp &> /dev/null; then
        log_ok "expdp found: $(command -v expdp)"
    else
        log_error "expdp (Data Pump Export) not found on PATH."
        errors=$((errors + 1))
    fi

    if command -v impdp &> /dev/null; then
        log_ok "impdp found: $(command -v impdp)"
    else
        log_error "impdp (Data Pump Import) not found on PATH."
        errors=$((errors + 1))
    fi

    # Check ORACLE_HOME
    if [[ -n "${ORACLE_HOME:-}" ]]; then
        log_ok "ORACLE_HOME set: $ORACLE_HOME"
    else
        log_warn "ORACLE_HOME not set. Data Pump may still work if configured."
    fi

    # Test database connectivity
    if [[ $errors -eq 0 ]]; then
        log_info "Testing database connectivity..."
        local test_result
        test_result=$(run_sql "SELECT 'CONNECTION_OK' FROM DUAL;")
        if echo "$test_result" | grep -q "CONNECTION_OK"; then
            log_ok "Database connection successful"
        else
            log_error "Database connection failed. Check credentials and TNS name."
            echo "$test_result" >> "$LOG_FILE"
            errors=$((errors + 1))
        fi
    fi

    if [[ $errors -gt 0 ]]; then
        log_error "$errors prerequisite(s) failed. Cannot proceed."
        exit 1
    fi

    log_ok "All prerequisites passed"
}

# ============================================================================
# Step 1: Discover OPPAYMENTS tablespace
# ============================================================================
discover_tablespace() {
    log_step "Step 1: Discovering OPPAYMENTS default tablespace"

    TABLESPACE_NAME=$(run_sql "
        SELECT default_tablespace
        FROM dba_users
        WHERE UPPER(username) = UPPER('${OPPAYMENTS_USER}');
    ")

    if [[ -z "$TABLESPACE_NAME" || "$TABLESPACE_NAME" == *"no rows"* ]]; then
        log_error "Could not find user '${OPPAYMENTS_USER}' in dba_users."
        exit 1
    fi

    HOLD_TS_NAME="${TABLESPACE_NAME}_HOLD"

    log_ok "OPPAYMENTS default tablespace: ${TABLESPACE_NAME}"
}

# ============================================================================
# Step 2: Find all schemas sharing the tablespace
# ============================================================================
discover_schemas() {
    log_step "Step 2: Finding all schemas using tablespace ${TABLESPACE_NAME}"

    # Get all users whose default tablespace matches, excluding Oracle internal schemas
    SCHEMA_LIST=$(run_sql "
        SELECT username
        FROM dba_users
        WHERE default_tablespace = '${TABLESPACE_NAME}'
          AND username NOT IN (
              'SYS','SYSTEM','DBSNMP','OUTLN','XDB','WMSYS','EXFSYS','CTXSYS',
              'MDSYS','ORDSYS','ORDDATA','OLAPSYS','APEX_PUBLIC_USER',
              'FLOWS_FILES','ANONYMOUS','APPQOSSYS','GSMADMIN_INTERNAL',
              'OJVMSYS','DVSYS','DVF','AUDSYS','LBACSYS','GSMCATUSER',
              'REMOTE_SCHEDULER_AGENT','GSMUSER','SYSBACKUP','SYSDG',
              'SYSKM','SYSRAC','SYS\$UMF','DGPDB_INT','DBSFWUSER',
              'ORACLE_OCM','XS\$NULL','GGSHAREDCAP','PDBADMIN'
          )
          AND oracle_maintained = 'N'
        ORDER BY username;
    ")

    if [[ -z "$SCHEMA_LIST" ]]; then
        log_error "No user schemas found using tablespace ${TABLESPACE_NAME}."
        exit 1
    fi

    # Convert to array
    readarray -t SCHEMAS <<< "$SCHEMA_LIST"

    log_ok "Found ${#SCHEMAS[@]} schema(s) using tablespace ${TABLESPACE_NAME}:"
    for schema in "${SCHEMAS[@]}"; do
        log_info "  - ${schema}"
    done
}

# ============================================================================
# Step 3: Detect PDB or non-PDB environment
# ============================================================================
detect_pdb() {
    log_step "Step 3: Detecting PDB / non-PDB environment"

    # Check if we are in a PDB by looking at V$PDBS or CON_ID
    local con_id
    con_id=$(run_sql "SELECT SYS_CONTEXT('USERENV', 'CON_ID') FROM DUAL;")

    if [[ -n "$con_id" && "$con_id" -gt 2 ]] 2>/dev/null; then
        IS_PDB="Y"
        local pdb_name
        pdb_name=$(run_sql "SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM DUAL;")
        log_ok "PDB environment detected: ${pdb_name} (CON_ID=${con_id})"
    else
        IS_PDB="N"
        log_ok "Non-PDB (standalone or CDB root) environment detected"
    fi

    # Check if this tablespace is the database/PDB default
    DB_DEFAULT_TS=$(run_sql "SELECT property_value FROM database_properties WHERE property_name = 'DEFAULT_PERMANENT_TABLESPACE';")
    if [[ "$DB_DEFAULT_TS" == "$TABLESPACE_NAME" ]]; then
        IS_DB_DEFAULT="Y"
        log_warn "Tablespace ${TABLESPACE_NAME} is the database/PDB default tablespace"
        log_info "A holding tablespace will be used during the drop/recreate cycle"
    else
        IS_DB_DEFAULT="N"
        log_info "Tablespace ${TABLESPACE_NAME} is NOT the database/PDB default (default is: ${DB_DEFAULT_TS})"
    fi
}

# ============================================================================
# Step 4: Detect Data Pump directory
# ============================================================================
detect_datapump_dir() {
    log_step "Step 4: Detecting Data Pump directory"

    # Try PDB_DATA_PUMP_DIR first (indicates PDB environment)
    local pdb_dir
    pdb_dir=$(run_sql "
        SELECT directory_path
        FROM dba_directories
        WHERE directory_name = 'PDB_DATA_PUMP_DIR';
    ")

    if [[ -n "$pdb_dir" && "$pdb_dir" != *"no rows"* ]]; then
        DATAPUMP_DIR_NAME="PDB_DATA_PUMP_DIR"
        DATAPUMP_DIR_PATH="$pdb_dir"
        log_ok "Using PDB_DATA_PUMP_DIR: ${DATAPUMP_DIR_PATH}"
        return
    fi

    # Try DATA_PUMP_DIR (non-PDB / CDB root)
    local std_dir
    std_dir=$(run_sql "
        SELECT directory_path
        FROM dba_directories
        WHERE directory_name = 'DATA_PUMP_DIR';
    ")

    if [[ -n "$std_dir" && "$std_dir" != *"no rows"* ]]; then
        DATAPUMP_DIR_NAME="DATA_PUMP_DIR"
        DATAPUMP_DIR_PATH="$std_dir"
        log_ok "Using DATA_PUMP_DIR: ${DATAPUMP_DIR_PATH}"
        return
    fi

    # Neither exists -- prompt user
    log_warn "Neither PDB_DATA_PUMP_DIR nor DATA_PUMP_DIR found."
    echo ""
    echo "  No Data Pump directory is configured in this database."
    echo "  Please provide a path on the database server where export"
    echo "  dump files can be written. This path must:"
    echo "    - Exist on the database server filesystem"
    echo "    - Be writable by the Oracle OS user"
    echo "    - Have sufficient free space for the export"
    echo ""
    read -rp "  Enter Data Pump directory path: " DATAPUMP_DIR_PATH

    if [[ -z "$DATAPUMP_DIR_PATH" ]]; then
        log_error "Data Pump directory path is required."
        exit 1
    fi

    # Create the directory object in Oracle
    DATAPUMP_DIR_NAME="DATA_PUMP_DIR"
    log_info "Creating Oracle directory object DATA_PUMP_DIR -> ${DATAPUMP_DIR_PATH}"
    local create_result
    create_result=$(run_sql "
        CREATE OR REPLACE DIRECTORY DATA_PUMP_DIR AS '${DATAPUMP_DIR_PATH}';
        GRANT READ, WRITE ON DIRECTORY DATA_PUMP_DIR TO PUBLIC;
    ")

    if echo "$create_result" | grep -qi "ORA-"; then
        log_error "Failed to create Data Pump directory: $create_result"
        exit 1
    fi

    log_ok "Created DATA_PUMP_DIR: ${DATAPUMP_DIR_PATH}"
}

# ============================================================================
# Step 5: Discover existing datafile paths (for recreate)
# ============================================================================
discover_datafiles() {
    log_step "Step 5: Discovering current datafile information"

    # Get the first datafile path for this tablespace (used as template for new path)
    ORIGINAL_DATAFILE=$(run_sql "
        SELECT file_name
        FROM dba_data_files
        WHERE tablespace_name = '${TABLESPACE_NAME}'
          AND ROWNUM = 1;
    ")

    if [[ -z "$ORIGINAL_DATAFILE" || "$ORIGINAL_DATAFILE" == *"no rows"* ]]; then
        log_error "No datafiles found for tablespace ${TABLESPACE_NAME}."
        exit 1
    fi

    # Get total current size
    CURRENT_SIZE=$(run_sql "
        SELECT ROUND(SUM(bytes) / 1048576) || ' MB'
        FROM dba_data_files
        WHERE tablespace_name = '${TABLESPACE_NAME}';
    ")

    # Get total used space
    USED_SIZE=$(run_sql "
        SELECT ROUND((SUM(df.bytes) - NVL(SUM(fs.free_bytes), 0)) / 1048576) || ' MB'
        FROM (
            SELECT tablespace_name, SUM(bytes) AS bytes
            FROM dba_data_files
            WHERE tablespace_name = '${TABLESPACE_NAME}'
            GROUP BY tablespace_name
        ) df
        LEFT JOIN (
            SELECT tablespace_name, SUM(bytes) AS free_bytes
            FROM dba_free_space
            WHERE tablespace_name = '${TABLESPACE_NAME}'
            GROUP BY tablespace_name
        ) fs ON fs.tablespace_name = df.tablespace_name;
    ")

    # Get all datafile paths
    ALL_DATAFILES=$(run_sql "
        SELECT file_name || ' (' || ROUND(bytes / 1048576) || ' MB)'
        FROM dba_data_files
        WHERE tablespace_name = '${TABLESPACE_NAME}'
        ORDER BY file_id;
    ")

    log_ok "Current tablespace ${TABLESPACE_NAME}:"
    log_info "  Total allocated: ${CURRENT_SIZE}"
    log_info "  Used space:      ${USED_SIZE}"
    log_info "  Datafiles:"
    while IFS= read -r line; do
        [[ -n "$line" ]] && log_info "    ${line}"
    done <<< "$ALL_DATAFILES"

    # Derive new datafile path from original (replace existing filename with new name)
    if [[ -z "$DATAFILE_PATH" ]]; then
        local dir_part
        dir_part=$(dirname "$ORIGINAL_DATAFILE")
        DATAFILE_PATH="${dir_part}/${TABLESPACE_NAME,,}_bigfile_01.dbf"
        log_info "  New datafile path: ${DATAFILE_PATH}"
    fi

    # Derive holding tablespace datafile path
    HOLD_DATAFILE_PATH="$(dirname "$ORIGINAL_DATAFILE")/${HOLD_TS_NAME,,}01.dbf"
}

# ============================================================================
# Step 6: Check for active sessions
# ============================================================================
check_active_sessions() {
    log_step "Step 6: Checking for active sessions on affected schemas"

    local schemas_csv
    schemas_csv=$(printf "'%s'," "${SCHEMAS[@]}")
    schemas_csv="${schemas_csv%,}"

    local active_sessions
    active_sessions=$(run_sql "
        SELECT COUNT(*)
        FROM v\$session
        WHERE username IN (${schemas_csv})
          AND status != 'KILLED';
    ")

    if [[ "$active_sessions" -gt 0 ]]; then
        log_warn "${active_sessions} active session(s) found on affected schemas!"
        log_warn "Listing active sessions:"
        local session_details
        session_details=$(run_sql "
            SELECT username || ' (SID=' || sid || ', Serial#=' || serial# || ', Status=' || status || ', Program=' || NVL(program,'N/A') || ')'
            FROM v\$session
            WHERE username IN (${schemas_csv})
              AND status != 'KILLED';
        ")
        while IFS= read -r line; do
            [[ -n "$line" ]] && log_warn "  ${line}"
        done <<< "$session_details"

        echo ""
        echo -e "  ${YELLOW}WARNING: Active sessions detected. These should be terminated${NC}"
        echo "  before proceeding. The export may fail or produce inconsistent"
        echo "  data if sessions are modifying data during export."
        echo ""
        if [[ "${ASSUME_YES^^}" == "Y" ]]; then
            log_info "--assume-yes: auto-confirming active sessions"
        else
            read -rp "  Continue anyway? (YES to confirm): " confirm
            if [[ "$confirm" != "YES" ]]; then
                log_info "Aborted by user."
                exit 0
            fi
        fi
    else
        log_ok "No active sessions on affected schemas"
    fi
}

# ============================================================================
# Display plan and confirm
# ============================================================================
confirm_plan() {
    log_header "Execution Plan"

    echo "" | tee -a "$LOG_FILE"
    echo "  The following operations will be performed:" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    echo "  1. EXPORT schemas via Data Pump (expdp):" | tee -a "$LOG_FILE"
    for schema in "${SCHEMAS[@]}"; do
        echo "     - ${schema}" | tee -a "$LOG_FILE"
    done
    echo "     Directory: ${DATAPUMP_DIR_NAME} (${DATAPUMP_DIR_PATH})" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    echo "  2. CREATE holding tablespace ${HOLD_TS_NAME}" | tee -a "$LOG_FILE"
    if [[ "$IS_DB_DEFAULT" == "Y" ]]; then
        if [[ "$IS_PDB" == "Y" ]]; then
            echo "  3. ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE ${HOLD_TS_NAME}" | tee -a "$LOG_FILE"
        else
            echo "  3. ALTER DATABASE DEFAULT TABLESPACE ${HOLD_TS_NAME}" | tee -a "$LOG_FILE"
        fi
    fi
    echo "  4. REASSIGN all users to holding tablespace ${HOLD_TS_NAME}" | tee -a "$LOG_FILE"
    echo "  5. DROP tablespace ${TABLESPACE_NAME}" | tee -a "$LOG_FILE"
    echo "     INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    echo "  6. RECREATE tablespace ${TABLESPACE_NAME}" | tee -a "$LOG_FILE"
    echo "     Type: BIGFILE" | tee -a "$LOG_FILE"
    echo "     Datafile: ${DATAFILE_PATH}" | tee -a "$LOG_FILE"
    echo "     Size: ${DATAFILE_SIZE}" | tee -a "$LOG_FILE"
    echo "     Autoextend: ON (next ${AUTOEXTEND_NEXT}, maxsize ${AUTOEXTEND_MAXSIZE})" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    if [[ "$IS_DB_DEFAULT" == "Y" ]]; then
        if [[ "$IS_PDB" == "Y" ]]; then
            echo "  7. ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE ${TABLESPACE_NAME}" | tee -a "$LOG_FILE"
        else
            echo "  7. ALTER DATABASE DEFAULT TABLESPACE ${TABLESPACE_NAME}" | tee -a "$LOG_FILE"
        fi
    fi
    echo "  8. REASSIGN all users back to ${TABLESPACE_NAME}" | tee -a "$LOG_FILE"
    echo "  9. IMPORT all schemas via Data Pump (impdp)" | tee -a "$LOG_FILE"
    echo "  10. VERIFY objects and recompile" | tee -a "$LOG_FILE"
    echo "  11. DROP holding tablespace ${HOLD_TS_NAME}" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"
    echo -e "  ${RED}WARNING: This operation requires application DOWNTIME.${NC}" | tee -a "$LOG_FILE"
    echo -e "  ${RED}WARNING: The tablespace and ALL its data will be dropped.${NC}" | tee -a "$LOG_FILE"
    echo -e "  ${RED}WARNING: If import fails, you must restore from the dump files.${NC}" | tee -a "$LOG_FILE"
    echo "" | tee -a "$LOG_FILE"

    if [[ "${ASSUME_YES^^}" == "Y" ]]; then
        log_info "--assume-yes: auto-proceeding with execution plan"
    else
        read -rp "  Type YES to proceed: " confirm
        if [[ "$confirm" != "YES" ]]; then
            log_info "Aborted by user."
            exit 0
        fi
    fi
}

# ============================================================================
# Step 7: Export all schemas
# ============================================================================
export_schemas() {
    log_step "Step 7: Exporting all schemas via Data Pump"

    local schemas_csv
    schemas_csv=$(printf "%s," "${SCHEMAS[@]}")
    schemas_csv="${schemas_csv%,}"

    DUMP_FILE="epf_reclaim_${TIMESTAMP}.dmp"
    EXPORT_LOG="epf_reclaim_export_${TIMESTAMP}.log"

    log_info "Dump file:  ${DATAPUMP_DIR_NAME}:${DUMP_FILE}"
    log_info "Export log: ${DATAPUMP_DIR_NAME}:${EXPORT_LOG}"
    log_info "Schemas:    ${schemas_csv}"

    # Build expdp connect string
    local expdp_connect
    if [[ "${DBA_USER,,}" == "sys" ]]; then
        expdp_connect="'${DBA_USER}/${DBA_PASSWORD}@${TNS_NAME} AS SYSDBA'"
    else
        expdp_connect="${DBA_USER}/${DBA_PASSWORD}@${TNS_NAME}"
    fi

    # Run expdp
    local expdp_cmd="expdp ${expdp_connect} \
        SCHEMAS=${schemas_csv} \
        DIRECTORY=${DATAPUMP_DIR_NAME} \
        DUMPFILE=${DUMP_FILE} \
        LOGFILE=${EXPORT_LOG} \
        REUSE_DUMPFILES=YES"

    log_info "Running: expdp (this may take a while)..."
    echo "$expdp_cmd" >> "$LOG_FILE"

    eval "$expdp_cmd" 2>&1 | tee -a "$LOG_FILE"
    local expdp_exit=${PIPESTATUS[0]}

    if [[ $expdp_exit -ne 0 ]]; then
        log_error "Data Pump export failed with exit code ${expdp_exit}."
        log_error "Check the export log: ${DATAPUMP_DIR_PATH}/${EXPORT_LOG}"
        log_error "Aborting -- no changes have been made to the database."
        exit 1
    fi

    log_ok "Export completed successfully"
}

# ============================================================================
# Step 8: Create holding tablespace
# ============================================================================
create_holding_tablespace() {
    log_step "Step 8: Creating temporary holding tablespace ${HOLD_TS_NAME}"

    local result
    result=$(run_sql "
        CREATE TABLESPACE ${HOLD_TS_NAME}
        DATAFILE '${HOLD_DATAFILE_PATH}'
        SIZE 100M;
    ")

    if echo "$result" | grep -qi "ORA-"; then
        log_error "Failed to create holding tablespace: ${result}"
        exit 1
    fi

    log_ok "Holding tablespace ${HOLD_TS_NAME} created"
}

# ============================================================================
# Step 9: Switch database/PDB default tablespace to holding TS
# ============================================================================
switch_db_default_to_hold() {
    log_step "Step 9: Switching database default tablespace"

    if [[ "$IS_DB_DEFAULT" == "Y" ]]; then
        local alter_cmd
        if [[ "$IS_PDB" == "Y" ]]; then
            alter_cmd="ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE ${HOLD_TS_NAME};"
        else
            alter_cmd="ALTER DATABASE DEFAULT TABLESPACE ${HOLD_TS_NAME};"
        fi

        log_info "  ${alter_cmd}"
        local result
        result=$(run_sql "$alter_cmd")

        if echo "$result" | grep -qi "ORA-"; then
            log_error "Failed to switch default tablespace: ${result}"
            exit 1
        fi

        log_ok "Database default tablespace switched to ${HOLD_TS_NAME}"
    else
        log_info "Tablespace ${TABLESPACE_NAME} is not the database default -- skipping"
    fi
}

# ============================================================================
# Step 10: Reassign all affected users to holding tablespace
# ============================================================================
reassign_to_hold() {
    log_step "Step 10: Reassigning all affected users to ${HOLD_TS_NAME}"

    # Use dynamic PL/SQL to catch ALL users on this tablespace (including any we may
    # have missed or that were created by other processes)
    local result
    result=$(run_sql "
        SET SERVEROUTPUT ON SIZE UNLIMITED
        BEGIN
            FOR u IN (
                SELECT username
                FROM dba_users
                WHERE default_tablespace = '${TABLESPACE_NAME}'
            )
            LOOP
                EXECUTE IMMEDIATE 'ALTER USER ' || u.username || ' DEFAULT TABLESPACE ${HOLD_TS_NAME}';
                DBMS_OUTPUT.PUT_LINE('Switched: ' || u.username);
            END LOOP;
        END;
    ")

    echo "$result" | tee -a "$LOG_FILE"
    log_ok "All users reassigned to holding tablespace ${HOLD_TS_NAME}"
}

# ============================================================================
# Step 11: Drop the tablespace
# ============================================================================
drop_tablespace() {
    log_step "Step 11: Dropping tablespace ${TABLESPACE_NAME}"

    log_warn "Dropping tablespace ${TABLESPACE_NAME} INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS"

    local result
    result=$(run_sql "
        DROP TABLESPACE ${TABLESPACE_NAME}
        INCLUDING CONTENTS AND DATAFILES
        CASCADE CONSTRAINTS;
    ")

    if echo "$result" | grep -qi "ORA-"; then
        log_error "Failed to drop tablespace: ${result}"
        log_error "You may need to kill active sessions or resolve dependencies."
        exit 1
    fi

    log_ok "Tablespace ${TABLESPACE_NAME} dropped successfully"
}

# ============================================================================
# Step 12: Recreate the tablespace as bigfile + autoextend
# ============================================================================
recreate_tablespace() {
    log_step "Step 12: Recreating tablespace ${TABLESPACE_NAME} as BIGFILE"

    local create_sql="
        CREATE BIGFILE TABLESPACE ${TABLESPACE_NAME}
        DATAFILE '${DATAFILE_PATH}'
        SIZE ${DATAFILE_SIZE}
        AUTOEXTEND ON
        NEXT ${AUTOEXTEND_NEXT}
        MAXSIZE ${AUTOEXTEND_MAXSIZE}
        EXTENT MANAGEMENT LOCAL
        SEGMENT SPACE MANAGEMENT AUTO;
    "

    log_info "DDL: ${create_sql}"

    local result
    result=$(run_sql "$create_sql")

    if echo "$result" | grep -qi "ORA-"; then
        log_error "Failed to recreate tablespace: ${result}"
        log_error "CRITICAL: Tablespace has been dropped but not recreated."
        log_error "You must recreate it manually and then run impdp to restore data."
        log_error "Dump file location: ${DATAPUMP_DIR_PATH}/${DUMP_FILE}"
        exit 1
    fi

    log_ok "Tablespace ${TABLESPACE_NAME} recreated as BIGFILE with AUTOEXTEND"
}

# ============================================================================
# Step 13: Switch database/PDB default tablespace back
# ============================================================================
switch_db_default_back() {
    log_step "Step 13: Restoring database default tablespace"

    if [[ "$IS_DB_DEFAULT" == "Y" ]]; then
        local alter_cmd
        if [[ "$IS_PDB" == "Y" ]]; then
            alter_cmd="ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE ${TABLESPACE_NAME};"
        else
            alter_cmd="ALTER DATABASE DEFAULT TABLESPACE ${TABLESPACE_NAME};"
        fi

        log_info "  ${alter_cmd}"
        local result
        result=$(run_sql "$alter_cmd")

        if echo "$result" | grep -qi "ORA-"; then
            log_warn "Failed to switch default tablespace back: ${result}"
        else
            log_ok "Database default tablespace restored to ${TABLESPACE_NAME}"
        fi
    else
        log_info "Not the database default -- skipping"
    fi
}

# ============================================================================
# Step 14: Reassign all users back to the recreated tablespace
# ============================================================================
reassign_from_hold() {
    log_step "Step 14: Reassigning all users back to ${TABLESPACE_NAME}"

    local result
    result=$(run_sql "
        SET SERVEROUTPUT ON SIZE UNLIMITED
        BEGIN
            FOR u IN (
                SELECT username
                FROM dba_users
                WHERE default_tablespace = '${HOLD_TS_NAME}'
            )
            LOOP
                EXECUTE IMMEDIATE 'ALTER USER ' || u.username || ' DEFAULT TABLESPACE ${TABLESPACE_NAME}';
                DBMS_OUTPUT.PUT_LINE('Restored: ' || u.username);
            END LOOP;
        END;
    ")

    echo "$result" | tee -a "$LOG_FILE"
    log_ok "All users reassigned back to ${TABLESPACE_NAME}"
}

# ============================================================================
# Step 15: Import all schemas
# ============================================================================
import_schemas() {
    log_step "Step 15: Importing all schemas via Data Pump"

    local schemas_csv
    schemas_csv=$(printf "%s," "${SCHEMAS[@]}")
    schemas_csv="${schemas_csv%,}"

    IMPORT_LOG="epf_reclaim_import_${TIMESTAMP}.log"

    log_info "Dump file:  ${DATAPUMP_DIR_NAME}:${DUMP_FILE}"
    log_info "Import log: ${DATAPUMP_DIR_NAME}:${IMPORT_LOG}"

    # Build impdp connect string
    local impdp_connect
    if [[ "${DBA_USER,,}" == "sys" ]]; then
        impdp_connect="'${DBA_USER}/${DBA_PASSWORD}@${TNS_NAME} AS SYSDBA'"
    else
        impdp_connect="${DBA_USER}/${DBA_PASSWORD}@${TNS_NAME}"
    fi

    # Run impdp
    local impdp_cmd="impdp ${impdp_connect} \
        SCHEMAS=${schemas_csv} \
        DIRECTORY=${DATAPUMP_DIR_NAME} \
        DUMPFILE=${DUMP_FILE} \
        LOGFILE=${IMPORT_LOG} \
        TABLE_EXISTS_ACTION=REPLACE"

    log_info "Running: impdp (this may take a while)..."
    echo "$impdp_cmd" >> "$LOG_FILE"

    eval "$impdp_cmd" 2>&1 | tee -a "$LOG_FILE"
    local impdp_exit=${PIPESTATUS[0]}

    if [[ $impdp_exit -ne 0 ]]; then
        log_warn "Data Pump import completed with warnings/errors (exit code ${impdp_exit})."
        log_warn "Check the import log: ${DATAPUMP_DIR_PATH}/${IMPORT_LOG}"
        log_warn "Some warnings are normal (e.g., 'object already exists' for users)."
    else
        log_ok "Import completed successfully"
    fi
}

# ============================================================================
# Step 16: Verify imported objects
# ============================================================================
verify_import() {
    log_step "Step 16: Verifying imported objects"

    for schema in "${SCHEMAS[@]}"; do
        log_info "Schema: ${schema}"

        local obj_summary
        obj_summary=$(run_sql "
            SELECT object_type || ': ' || COUNT(*) || ' (valid: ' ||
                   SUM(CASE WHEN status = 'VALID' THEN 1 ELSE 0 END) ||
                   ', invalid: ' ||
                   SUM(CASE WHEN status = 'INVALID' THEN 1 ELSE 0 END) || ')'
            FROM dba_objects
            WHERE owner = UPPER('${schema}')
            GROUP BY object_type
            ORDER BY object_type;
        ")

        if [[ -n "$obj_summary" ]]; then
            while IFS= read -r line; do
                [[ -n "$line" ]] && log_info "  ${line}"
            done <<< "$obj_summary"
        else
            log_warn "  No objects found for ${schema}"
        fi

        # Check for invalid objects
        local invalid_count
        invalid_count=$(run_sql "
            SELECT COUNT(*)
            FROM dba_objects
            WHERE owner = UPPER('${schema}')
              AND status = 'INVALID';
        ")

        if [[ "$invalid_count" -gt 0 ]]; then
            log_warn "  ${invalid_count} invalid object(s) in ${schema}. Attempting recompile..."
            run_sql "
                BEGIN
                    DBMS_UTILITY.COMPILE_SCHEMA(schema => UPPER('${schema}'), compile_all => FALSE);
                END;
            " > /dev/null 2>&1

            # Recheck
            local still_invalid
            still_invalid=$(run_sql "
                SELECT COUNT(*)
                FROM dba_objects
                WHERE owner = UPPER('${schema}')
                  AND status = 'INVALID';
            ")

            if [[ "$still_invalid" -gt 0 ]]; then
                log_warn "  ${still_invalid} object(s) still invalid after recompile."
                local invalid_list
                invalid_list=$(run_sql "
                    SELECT object_type || ': ' || object_name
                    FROM dba_objects
                    WHERE owner = UPPER('${schema}')
                      AND status = 'INVALID'
                    ORDER BY object_type, object_name;
                ")
                while IFS= read -r line; do
                    [[ -n "$line" ]] && log_warn "    ${line}"
                done <<< "$invalid_list"
            else
                log_ok "  All objects now valid after recompile"
            fi
        else
            log_ok "  All objects valid"
        fi
    done

    # Verify new tablespace info
    log_info ""
    log_info "New tablespace ${TABLESPACE_NAME} status:"
    local ts_info
    ts_info=$(run_sql "
        SELECT 'Datafile: ' || file_name ||
               ', Size: ' || ROUND(bytes / 1048576) || ' MB' ||
               ', Autoextend: ' || autoextensible ||
               ', Max: ' || CASE WHEN maxbytes = 0 THEN 'N/A'
                                  WHEN maxbytes >= 34359738368 THEN 'UNLIMITED'
                                  ELSE ROUND(maxbytes / 1048576) || ' MB' END
        FROM dba_data_files
        WHERE tablespace_name = '${TABLESPACE_NAME}';
    ")
    while IFS= read -r line; do
        [[ -n "$line" ]] && log_info "  ${line}"
    done <<< "$ts_info"

    local new_size
    new_size=$(run_sql "
        SELECT ROUND(SUM(bytes) / 1048576) || ' MB'
        FROM dba_data_files
        WHERE tablespace_name = '${TABLESPACE_NAME}';
    ")
    log_info "  Total allocated: ${new_size}"
    log_info "  Previous size:   ${CURRENT_SIZE}"
}

# ============================================================================
# Step 17: Drop the holding tablespace
# ============================================================================
drop_holding_tablespace() {
    log_step "Step 17: Dropping holding tablespace ${HOLD_TS_NAME}"

    # Verify no users still on it
    local remaining
    remaining=$(run_sql "
        SELECT COUNT(*)
        FROM dba_users
        WHERE default_tablespace = '${HOLD_TS_NAME}';
    ")

    if [[ "$remaining" -gt 0 ]]; then
        log_warn "${remaining} user(s) still on ${HOLD_TS_NAME}. Reassigning to ${TABLESPACE_NAME}..."
        run_sql "
            BEGIN
                FOR u IN (
                    SELECT username FROM dba_users WHERE default_tablespace = '${HOLD_TS_NAME}'
                )
                LOOP
                    EXECUTE IMMEDIATE 'ALTER USER ' || u.username || ' DEFAULT TABLESPACE ${TABLESPACE_NAME}';
                END LOOP;
            END;
        " > /dev/null 2>&1
    fi

    local result
    result=$(run_sql "
        DROP TABLESPACE ${HOLD_TS_NAME}
        INCLUDING CONTENTS AND DATAFILES;
    ")

    if echo "$result" | grep -qi "ORA-"; then
        log_warn "Could not drop holding tablespace: ${result}"
        log_warn "You can drop it manually: DROP TABLESPACE ${HOLD_TS_NAME} INCLUDING CONTENTS AND DATAFILES;"
    else
        log_ok "Holding tablespace ${HOLD_TS_NAME} dropped"
    fi
}

# ============================================================================
# Main
# ============================================================================
main() {
    mkdir -p "$LOG_DIR"

    echo "EPF Tablespace Reclaim Tool" | tee "$LOG_FILE"
    echo "Log file: $LOG_FILE" | tee -a "$LOG_FILE"
    echo "Started: $(date)" | tee -a "$LOG_FILE"

    parse_args "$@"

    # Interactive prompts if key params missing
    if [[ -z "$TNS_NAME" || -z "$DBA_PASSWORD" ]]; then
        interactive_prompts
    fi

    # Execute steps
    check_prerequisites          # Pre-check
    discover_tablespace          # Step 1
    discover_schemas             # Step 2
    detect_pdb                   # Step 3
    detect_datapump_dir          # Step 4
    discover_datafiles           # Step 5
    check_active_sessions        # Step 6
    confirm_plan                 # Confirm
    export_schemas               # Step 7
    create_holding_tablespace    # Step 8
    switch_db_default_to_hold    # Step 9
    reassign_to_hold             # Step 10
    drop_tablespace              # Step 11
    recreate_tablespace          # Step 12
    switch_db_default_back       # Step 13
    reassign_from_hold           # Step 14
    import_schemas               # Step 15
    verify_import                # Step 16
    drop_holding_tablespace      # Step 17

    # Capture new tablespace size for comparison
    local new_size
    new_size=$(run_sql "SELECT ROUND(SUM(bytes) / 1048576) || ' MB' FROM dba_data_files WHERE tablespace_name = '${TABLESPACE_NAME}';")

    log_header "Tablespace Reclaim Complete"
    log_ok "Tablespace ${TABLESPACE_NAME} has been recreated as BIGFILE with AUTOEXTEND"
    echo ""
    log_info "Size comparison:"
    log_info "  Before:  ${CURRENT_SIZE}"
    log_info "  After:   ${new_size}"
    echo ""
    log_ok "Previous size: ${CURRENT_SIZE}"
    log_info "Export dump: ${DATAPUMP_DIR_PATH}/${DUMP_FILE}"
    log_info "Export log:  ${DATAPUMP_DIR_PATH}/${EXPORT_LOG}"
    log_info "Import log:  ${DATAPUMP_DIR_PATH}/${IMPORT_LOG}"
    log_info "Script log:  ${LOG_FILE}"
    echo ""
    log_warn "Keep the dump file (${DUMP_FILE}) until you have verified"
    log_warn "all data and application functionality. It is your rollback."
    echo ""
    echo "Finished: $(date)" >> "$LOG_FILE"
}

main "$@"
