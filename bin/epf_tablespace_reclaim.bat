@echo off
REM ============================================================================
REM EPF Tablespace Reclaim - Windows Script
REM ============================================================================
REM Reclaims tablespace disk space by exporting all schemas that share the same
REM tablespace as OPPAYMENTS, dropping the tablespace, recreating it as a
REM bigfile autoextending tablespace, and reimporting all schemas.
REM
REM Handles both PDB and non-PDB environments with a holding tablespace
REM approach to safely manage database default tablespace assignments.
REM
REM Usage:
REM   Interactive:   epf_tablespace_reclaim.bat
REM   With args:     epf_tablespace_reclaim.bat --tns EPFPROD --sys-password XXX
REM ============================================================================

setlocal enabledelayedexpansion

REM ============================================================================
REM Defaults
REM ============================================================================
set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
set "LOG_DIR=%PROJECT_DIR%\logs"

for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set "DT=%%I"
set "TIMESTAMP=%DT:~0,8%_%DT:~8,6%"
set "LOG_FILE=%LOG_DIR%\epf_tablespace_reclaim_%TIMESTAMP%.log"

set "TNS_NAME="
set "DBA_USER=sys"
set "DBA_PASSWORD="
set "OPPAYMENTS_USER=oppayments"
set "DATAFILE_PATH="
set "DATAFILE_SIZE=10G"
set "AUTOEXTEND_NEXT=1G"
set "AUTOEXTEND_MAXSIZE=UNLIMITED"
set "IS_PDB=N"
set "IS_DB_DEFAULT=N"
set "ASSUME_YES=N"

REM Ensure log directory exists
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
echo EPF Tablespace Reclaim Tool > "%LOG_FILE%"
echo Started: %DATE% %TIME% >> "%LOG_FILE%"

REM ============================================================================
REM Parse arguments
REM ============================================================================
:parse_args
if "%~1"=="" goto :args_done
if /i "%~1"=="--tns"             ( set "TNS_NAME=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--dba-user"        ( set "DBA_USER=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--dba-password"    ( set "DBA_PASSWORD=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--sys-password"    ( set "DBA_USER=sys" & set "DBA_PASSWORD=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--oppayments-user" ( set "OPPAYMENTS_USER=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--datafile-path"   ( set "DATAFILE_PATH=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--datafile-size"   ( set "DATAFILE_SIZE=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--autoextend-next" ( set "AUTOEXTEND_NEXT=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--autoextend-max"  ( set "AUTOEXTEND_MAXSIZE=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--assume-yes"      ( set "ASSUME_YES=Y" & shift & goto :parse_args )
if /i "%~1"=="-y"                ( set "ASSUME_YES=Y" & shift & goto :parse_args )
if /i "%~1"=="--help"            ( goto :show_help )
if /i "%~1"=="-h"                ( goto :show_help )
echo [ERROR] Unknown argument: %~1
goto :show_help
:args_done

REM Environment variable overrides
if defined EPF_DBA_PASSWORD set "DBA_PASSWORD=%EPF_DBA_PASSWORD%"

REM ============================================================================
REM Interactive prompts if key params missing
REM ============================================================================
if "%TNS_NAME%"=="" (
    echo.
    echo   ============================================================
    echo   EPF Tablespace Reclaim - Configuration
    echo   ============================================================
    echo.
    echo   TNS Name / Connect String
    echo   The Oracle service name or TNS alias to connect to.
    echo   Example: EPFPROD, localhost:1521/orcl
    set /p "TNS_NAME=  Enter TNS name: "
)

if "%DBA_PASSWORD%"=="" (
    echo.
    echo   DBA User: %DBA_USER%
    echo   Enter the password for the DBA user.
    echo   ^(This user must have DBA/SYSDBA privileges^)
    for /f "usebackq delims=" %%P in (`powershell -Command "$p = Read-Host '  Password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))"`) do set "DBA_PASSWORD=%%P"
)

REM Build connect string
if /i "%DBA_USER%"=="sys" (
    set "CONNECT_STR=%DBA_USER%/%DBA_PASSWORD%@%TNS_NAME% AS SYSDBA"
) else (
    set "CONNECT_STR=%DBA_USER%/%DBA_PASSWORD%@%TNS_NAME%"
)

REM ============================================================================
REM Check prerequisites
REM ============================================================================
echo.
echo   ============================================================
echo   Checking Prerequisites
echo   ============================================================

where sqlplus >nul 2>&1
if !ERRORLEVEL! neq 0 (
    echo [ERROR] SQL*Plus not found on PATH.
    exit /b 1
)
echo [OK]    SQL*Plus found

where expdp >nul 2>&1
if !ERRORLEVEL! neq 0 (
    echo [ERROR] expdp ^(Data Pump Export^) not found on PATH.
    exit /b 1
)
echo [OK]    expdp found

where impdp >nul 2>&1
if !ERRORLEVEL! neq 0 (
    echo [ERROR] impdp ^(Data Pump Import^) not found on PATH.
    exit /b 1
)
echo [OK]    impdp found

REM Test connectivity
echo [INFO]  Testing database connectivity...
echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0> "%TEMP%\epf_ts_test.sql"
echo SELECT 'CONNECTION_OK' FROM DUAL;>> "%TEMP%\epf_ts_test.sql"
echo EXIT;>> "%TEMP%\epf_ts_test.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_test.sql" > "%TEMP%\epf_ts_test_result.txt" 2>&1
findstr /i "CONNECTION_OK" "%TEMP%\epf_ts_test_result.txt" >nul 2>&1
if !ERRORLEVEL! neq 0 (
    echo [ERROR] Database connection failed. Check credentials and TNS name.
    type "%TEMP%\epf_ts_test_result.txt"
    del "%TEMP%\epf_ts_test.sql" "%TEMP%\epf_ts_test_result.txt" >nul 2>&1
    exit /b 1
)
del "%TEMP%\epf_ts_test.sql" "%TEMP%\epf_ts_test_result.txt" >nul 2>&1
echo [OK]    Database connection successful

REM ============================================================================
REM Step 1: Discover OPPAYMENTS tablespace
REM ============================================================================
echo.
echo ^>^>^> Step 1: Discovering OPPAYMENTS default tablespace

echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT default_tablespace FROM dba_users WHERE UPPER(username) = UPPER('%OPPAYMENTS_USER%');>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
set "TABLESPACE_NAME="
for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" set "TABLESPACE_NAME=!LINE!"
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

if "!TABLESPACE_NAME!"=="" (
    echo [ERROR] Could not find user '%OPPAYMENTS_USER%' in dba_users.
    exit /b 1
)
set "HOLD_TS_NAME=!TABLESPACE_NAME!_HOLD"
echo [OK]    OPPAYMENTS default tablespace: !TABLESPACE_NAME!

REM ============================================================================
REM Step 2: Find all schemas sharing the tablespace
REM ============================================================================
echo.
echo ^>^>^> Step 2: Finding all schemas using tablespace !TABLESPACE_NAME!

echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT username FROM dba_users WHERE default_tablespace = '!TABLESPACE_NAME!' AND username NOT IN ('SYS','SYSTEM','DBSNMP','OUTLN','XDB','WMSYS','EXFSYS','CTXSYS','MDSYS','ORDSYS','ORDDATA','OLAPSYS','APEX_PUBLIC_USER','FLOWS_FILES','ANONYMOUS','APPQOSSYS','GSMADMIN_INTERNAL','OJVMSYS','DVSYS','DVF','AUDSYS','LBACSYS','GSMCATUSER','REMOTE_SCHEDULER_AGENT','GSMUSER','SYSBACKUP','SYSDG','SYSKM','SYSRAC','DBSFWUSER','ORACLE_OCM','PDBADMIN') AND oracle_maintained = 'N' ORDER BY username;>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1

set "SCHEMA_COUNT=0"
set "SCHEMA_LIST="
for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" (
        set /a SCHEMA_COUNT+=1
        if "!SCHEMA_LIST!"=="" (
            set "SCHEMA_LIST=!LINE!"
        ) else (
            set "SCHEMA_LIST=!SCHEMA_LIST!,!LINE!"
        )
        echo [INFO]    - !LINE!
    )
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

if !SCHEMA_COUNT! EQU 0 (
    echo [ERROR] No user schemas found using tablespace !TABLESPACE_NAME!.
    exit /b 1
)
echo [OK]    Found !SCHEMA_COUNT! schema(s) using tablespace !TABLESPACE_NAME!

REM ============================================================================
REM Step 3: Detect PDB / non-PDB environment
REM ============================================================================
echo.
echo ^>^>^> Step 3: Detecting PDB / non-PDB environment

echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT SYS_CONTEXT('USERENV', 'CON_ID') FROM DUAL;>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
set "CON_ID=0"
for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" set "CON_ID=!LINE!"
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

if !CON_ID! GTR 2 (
    set "IS_PDB=Y"
    echo [OK]    PDB environment detected ^(CON_ID=!CON_ID!^)
) else (
    set "IS_PDB=N"
    echo [OK]    Non-PDB ^(standalone or CDB root^) environment detected
)

REM Check if tablespace is database/PDB default
echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT property_value FROM database_properties WHERE property_name = 'DEFAULT_PERMANENT_TABLESPACE';>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
set "DB_DEFAULT_TS="
for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" set "DB_DEFAULT_TS=!LINE!"
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

if /i "!DB_DEFAULT_TS!"=="!TABLESPACE_NAME!" (
    set "IS_DB_DEFAULT=Y"
    echo [WARN]  Tablespace !TABLESPACE_NAME! is the database/PDB default tablespace
    echo [INFO]  A holding tablespace will be used during the drop/recreate cycle
) else (
    set "IS_DB_DEFAULT=N"
    echo [INFO]  Tablespace !TABLESPACE_NAME! is NOT the database/PDB default ^(default is: !DB_DEFAULT_TS!^)
)

REM ============================================================================
REM Step 4: Detect Data Pump directory
REM ============================================================================
echo.
echo ^>^>^> Step 4: Detecting Data Pump directory

set "DATAPUMP_DIR_NAME="
set "DATAPUMP_DIR_PATH="

REM Try PDB_DATA_PUMP_DIR first
echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT directory_path FROM dba_directories WHERE directory_name = 'PDB_DATA_PUMP_DIR';>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1

for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" (
        set "DATAPUMP_DIR_NAME=PDB_DATA_PUMP_DIR"
        set "DATAPUMP_DIR_PATH=!LINE!"
    )
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

if not "!DATAPUMP_DIR_NAME!"=="" (
    echo [OK]    Using PDB_DATA_PUMP_DIR: !DATAPUMP_DIR_PATH!
    goto :datapump_done
)

REM Try DATA_PUMP_DIR
echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT directory_path FROM dba_directories WHERE directory_name = 'DATA_PUMP_DIR';>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1

for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" (
        set "DATAPUMP_DIR_NAME=DATA_PUMP_DIR"
        set "DATAPUMP_DIR_PATH=!LINE!"
    )
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

if not "!DATAPUMP_DIR_NAME!"=="" (
    echo [OK]    Using DATA_PUMP_DIR: !DATAPUMP_DIR_PATH!
    goto :datapump_done
)

REM Neither exists -- prompt user
echo [WARN]  Neither PDB_DATA_PUMP_DIR nor DATA_PUMP_DIR found.
echo.
echo   No Data Pump directory is configured in this database.
echo   Please provide a path on the database server where export
echo   dump files can be written.
echo.
set /p "DATAPUMP_DIR_PATH=  Enter Data Pump directory path: "

if "!DATAPUMP_DIR_PATH!"=="" (
    echo [ERROR] Data Pump directory path is required.
    exit /b 1
)

set "DATAPUMP_DIR_NAME=DATA_PUMP_DIR"
echo [INFO]  Creating Oracle directory object DATA_PUMP_DIR -^> !DATAPUMP_DIR_PATH!

echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0> "%TEMP%\epf_ts_q.sql"
echo CREATE OR REPLACE DIRECTORY DATA_PUMP_DIR AS '!DATAPUMP_DIR_PATH!';>> "%TEMP%\epf_ts_q.sql"
echo GRANT READ, WRITE ON DIRECTORY DATA_PUMP_DIR TO PUBLIC;>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
findstr /i "ORA-" "%TEMP%\epf_ts_r.txt" >nul 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [ERROR] Failed to create Data Pump directory:
    type "%TEMP%\epf_ts_r.txt"
    del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1
    exit /b 1
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1
echo [OK]    Created DATA_PUMP_DIR: !DATAPUMP_DIR_PATH!

:datapump_done

REM ============================================================================
REM Step 5: Discover current datafile information
REM ============================================================================
echo.
echo ^>^>^> Step 5: Discovering current datafile information

REM Get first datafile path
echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT file_name FROM dba_data_files WHERE tablespace_name = '!TABLESPACE_NAME!' AND ROWNUM = 1;>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
set "ORIGINAL_DATAFILE="
for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" set "ORIGINAL_DATAFILE=!LINE!"
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

REM Get total size
echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT ROUND(SUM(bytes) / 1048576) ^|^| ' MB' FROM dba_data_files WHERE tablespace_name = '!TABLESPACE_NAME!';>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
set "CURRENT_SIZE="
for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" set "CURRENT_SIZE=!LINE!"
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

echo [OK]    Current tablespace !TABLESPACE_NAME!:
echo [INFO]    Total allocated: !CURRENT_SIZE!
echo [INFO]    Original datafile: !ORIGINAL_DATAFILE!

REM Derive datafile paths from original (Oracle uses Unix-style paths on server)
if not "!DATAFILE_PATH!"=="" goto :datafile_path_done
REM Use SQL to extract directory from Oracle path (handles Unix / paths)
echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT SUBSTR('!ORIGINAL_DATAFILE!', 1, INSTR('!ORIGINAL_DATAFILE!', '/', -1)) ^|^| LOWER('!TABLESPACE_NAME!') ^|^| '_bigfile_01.dbf' FROM DUAL;>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"
sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" set "DATAFILE_PATH=!LINE!"
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1
echo [INFO]    New datafile path: !DATAFILE_PATH!
:datafile_path_done

REM Derive holding tablespace datafile path
echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT SUBSTR('!ORIGINAL_DATAFILE!', 1, INSTR('!ORIGINAL_DATAFILE!', '/', -1)) ^|^| LOWER('!HOLD_TS_NAME!') ^|^| '01.dbf' FROM DUAL;>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"
sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
set "HOLD_DATAFILE_PATH="
for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" set "HOLD_DATAFILE_PATH=!LINE!"
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

REM ============================================================================
REM Step 6: Check for active sessions
REM ============================================================================
echo.
echo ^>^>^> Step 6: Checking for active sessions

echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT COUNT(*) FROM v$session WHERE username IN (SELECT username FROM dba_users WHERE default_tablespace = '!TABLESPACE_NAME!' AND oracle_maintained = 'N') AND status != 'KILLED';>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
set "ACTIVE_SESSIONS=0"
for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" set "ACTIVE_SESSIONS=!LINE!"
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

if !ACTIVE_SESSIONS! GTR 0 (
    echo [WARN]  !ACTIVE_SESSIONS! active session^(s^) found on affected schemas!
    echo [WARN]  These should be terminated before proceeding.
    echo.
    if /i "!ASSUME_YES!"=="Y" (
        set "SESS_CONFIRM=YES"
        echo [INFO]  --assume-yes: auto-confirming active sessions
    ) else (
        set /p "SESS_CONFIRM=  Continue anyway? (YES to confirm): "
    )
    if not "!SESS_CONFIRM!"=="YES" (
        echo [INFO]  Aborted by user.
        exit /b 0
    )
) else (
    echo [OK]    No active sessions on affected schemas
)

REM ============================================================================
REM Confirm execution plan
REM ============================================================================
echo.
echo   ============================================================
echo   Execution Plan
echo   ============================================================
echo.
echo   1. EXPORT schemas via Data Pump (expdp):
echo      Schemas: !SCHEMA_LIST!
echo      Directory: !DATAPUMP_DIR_NAME! (!DATAPUMP_DIR_PATH!)
echo.
echo   2. CREATE holding tablespace !HOLD_TS_NAME!
if /i "!IS_DB_DEFAULT!"=="Y" (
    if /i "!IS_PDB!"=="Y" (
        echo   3. ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE !HOLD_TS_NAME!
    ) else (
        echo   3. ALTER DATABASE DEFAULT TABLESPACE !HOLD_TS_NAME!
    )
)
echo   4. REASSIGN all users to holding tablespace !HOLD_TS_NAME!
echo   5. DROP tablespace !TABLESPACE_NAME!
echo      INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS
echo.
echo   6. RECREATE tablespace !TABLESPACE_NAME!
echo      Type: BIGFILE
echo      Datafile: !DATAFILE_PATH!
echo      Size: !DATAFILE_SIZE!
echo      Autoextend: ON (next !AUTOEXTEND_NEXT!, maxsize !AUTOEXTEND_MAXSIZE!)
echo.
if /i "!IS_DB_DEFAULT!"=="Y" (
    if /i "!IS_PDB!"=="Y" (
        echo   7. ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE !TABLESPACE_NAME!
    ) else (
        echo   7. ALTER DATABASE DEFAULT TABLESPACE !TABLESPACE_NAME!
    )
)
echo   8. REASSIGN all users back to !TABLESPACE_NAME!
echo   9. IMPORT all schemas via Data Pump (impdp)
echo   10. VERIFY objects and recompile
echo   11. DROP holding tablespace !HOLD_TS_NAME!
echo.
echo   WARNING: This operation requires application DOWNTIME.
echo   WARNING: The tablespace and ALL its data will be dropped.
echo   WARNING: If import fails, you must restore from the dump files.
echo.
if /i "!ASSUME_YES!"=="Y" (
    set "CONFIRM=YES"
    echo [INFO]  --assume-yes: auto-proceeding with execution plan
) else (
    set /p "CONFIRM=  Type YES to proceed: "
)
if not "!CONFIRM!"=="YES" (
    echo [INFO]  Aborted by user.
    exit /b 0
)

REM ============================================================================
REM Step 7: Export all schemas
REM ============================================================================
echo.
echo ^>^>^> Step 7: Exporting all schemas via Data Pump

set "DUMP_FILE=epf_reclaim_%TIMESTAMP%.dmp"
set "EXPORT_LOG=epf_reclaim_export_%TIMESTAMP%.log"

echo [INFO]  Dump file:  !DATAPUMP_DIR_NAME!:!DUMP_FILE!
echo [INFO]  Schemas:    !SCHEMA_LIST!

if /i "%DBA_USER%"=="sys" (
    set "EXPDP_CONNECT='%DBA_USER%/%DBA_PASSWORD%@%TNS_NAME% AS SYSDBA'"
) else (
    set "EXPDP_CONNECT=%DBA_USER%/%DBA_PASSWORD%@%TNS_NAME%"
)

echo [INFO]  Running expdp (this may take a while)...
expdp !EXPDP_CONNECT! SCHEMAS=!SCHEMA_LIST! DIRECTORY=!DATAPUMP_DIR_NAME! DUMPFILE=!DUMP_FILE! LOGFILE=!EXPORT_LOG! REUSE_DUMPFILES=YES 2>&1
if !ERRORLEVEL! neq 0 (
    echo [ERROR] Data Pump export failed.
    echo [ERROR] Check the export log: !DATAPUMP_DIR_PATH!\!EXPORT_LOG!
    echo [ERROR] Aborting -- no changes have been made to the database.
    exit /b 1
)
echo [OK]    Export completed successfully

REM ============================================================================
REM Step 8: Create holding tablespace
REM ============================================================================
echo.
echo ^>^>^> Step 8: Creating temporary holding tablespace !HOLD_TS_NAME!

echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0> "%TEMP%\epf_ts_q.sql"
echo CREATE TABLESPACE !HOLD_TS_NAME! DATAFILE '!HOLD_DATAFILE_PATH!' SIZE 100M;>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
findstr /i "ORA-" "%TEMP%\epf_ts_r.txt" >nul 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [ERROR] Failed to create holding tablespace:
    type "%TEMP%\epf_ts_r.txt"
    del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1
    exit /b 1
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1
echo [OK]    Holding tablespace !HOLD_TS_NAME! created

REM ============================================================================
REM Step 9: Switch database/PDB default tablespace to holding TS
REM ============================================================================
echo.
echo ^>^>^> Step 9: Switching database default tablespace

if /i "!IS_DB_DEFAULT!"=="Y" (
    echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0> "%TEMP%\epf_ts_q.sql"
    if /i "!IS_PDB!"=="Y" (
        echo ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE !HOLD_TS_NAME!;>> "%TEMP%\epf_ts_q.sql"
        echo [INFO]    ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE !HOLD_TS_NAME!
    ) else (
        echo ALTER DATABASE DEFAULT TABLESPACE !HOLD_TS_NAME!;>> "%TEMP%\epf_ts_q.sql"
        echo [INFO]    ALTER DATABASE DEFAULT TABLESPACE !HOLD_TS_NAME!
    )
    echo EXIT;>> "%TEMP%\epf_ts_q.sql"

    sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
    findstr /i "ORA-" "%TEMP%\epf_ts_r.txt" >nul 2>&1
    if !ERRORLEVEL! EQU 0 (
        echo [ERROR] Failed to switch default tablespace:
        type "%TEMP%\epf_ts_r.txt"
        del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1
        exit /b 1
    )
    del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1
    echo [OK]    Database default tablespace switched to !HOLD_TS_NAME!
) else (
    echo [INFO]  Not the database default -- skipping
)

REM ============================================================================
REM Step 10: Reassign all affected users to holding tablespace
REM ============================================================================
echo.
echo ^>^>^> Step 10: Reassigning all users to !HOLD_TS_NAME!

echo SET SERVEROUTPUT ON SIZE UNLIMITED> "%TEMP%\epf_ts_q.sql"
echo BEGIN>> "%TEMP%\epf_ts_q.sql"
echo   FOR u IN (SELECT username FROM dba_users WHERE default_tablespace = '!TABLESPACE_NAME!')>> "%TEMP%\epf_ts_q.sql"
echo   LOOP>> "%TEMP%\epf_ts_q.sql"
echo     EXECUTE IMMEDIATE 'ALTER USER ' ^|^| u.username ^|^| ' DEFAULT TABLESPACE !HOLD_TS_NAME!';>> "%TEMP%\epf_ts_q.sql"
echo     DBMS_OUTPUT.PUT_LINE('Switched: ' ^|^| u.username);>> "%TEMP%\epf_ts_q.sql"
echo   END LOOP;>> "%TEMP%\epf_ts_q.sql"
echo END;>> "%TEMP%\epf_ts_q.sql"
echo />> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" 2>&1
del "%TEMP%\epf_ts_q.sql" >nul 2>&1
echo [OK]    All users reassigned to !HOLD_TS_NAME!

REM ============================================================================
REM Step 11: Drop the tablespace
REM ============================================================================
echo.
echo ^>^>^> Step 11: Dropping tablespace !TABLESPACE_NAME!

echo [WARN]  Dropping tablespace !TABLESPACE_NAME! INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS

echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0> "%TEMP%\epf_ts_q.sql"
echo DROP TABLESPACE !TABLESPACE_NAME! INCLUDING CONTENTS AND DATAFILES CASCADE CONSTRAINTS;>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
findstr /i "ORA-" "%TEMP%\epf_ts_r.txt" >nul 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [ERROR] Failed to drop tablespace:
    type "%TEMP%\epf_ts_r.txt"
    del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1
    exit /b 1
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1
echo [OK]    Tablespace !TABLESPACE_NAME! dropped successfully

REM ============================================================================
REM Step 12: Recreate the tablespace
REM ============================================================================
echo.
echo ^>^>^> Step 12: Recreating tablespace !TABLESPACE_NAME! as BIGFILE

echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0> "%TEMP%\epf_ts_q.sql"
echo CREATE BIGFILE TABLESPACE !TABLESPACE_NAME! DATAFILE '!DATAFILE_PATH!' SIZE !DATAFILE_SIZE! AUTOEXTEND ON NEXT !AUTOEXTEND_NEXT! MAXSIZE !AUTOEXTEND_MAXSIZE! EXTENT MANAGEMENT LOCAL SEGMENT SPACE MANAGEMENT AUTO;>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
findstr /i "ORA-" "%TEMP%\epf_ts_r.txt" >nul 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [ERROR] Failed to recreate tablespace:
    type "%TEMP%\epf_ts_r.txt"
    echo [ERROR] CRITICAL: Tablespace has been dropped but not recreated.
    echo [ERROR] Dump file: !DATAPUMP_DIR_PATH!\!DUMP_FILE!
    del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1
    exit /b 1
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1
echo [OK]    Tablespace !TABLESPACE_NAME! recreated as BIGFILE with AUTOEXTEND

REM ============================================================================
REM Step 13: Switch database/PDB default tablespace back
REM ============================================================================
echo.
echo ^>^>^> Step 13: Restoring database default tablespace

if /i "!IS_DB_DEFAULT!"=="Y" (
    echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0> "%TEMP%\epf_ts_q.sql"
    if /i "!IS_PDB!"=="Y" (
        echo ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE !TABLESPACE_NAME!;>> "%TEMP%\epf_ts_q.sql"
        echo [INFO]    ALTER PLUGGABLE DATABASE DEFAULT TABLESPACE !TABLESPACE_NAME!
    ) else (
        echo ALTER DATABASE DEFAULT TABLESPACE !TABLESPACE_NAME!;>> "%TEMP%\epf_ts_q.sql"
        echo [INFO]    ALTER DATABASE DEFAULT TABLESPACE !TABLESPACE_NAME!
    )
    echo EXIT;>> "%TEMP%\epf_ts_q.sql"

    sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" >> "%LOG_FILE%" 2>&1
    del "%TEMP%\epf_ts_q.sql" >nul 2>&1
    echo [OK]    Database default tablespace restored to !TABLESPACE_NAME!
) else (
    echo [INFO]  Not the database default -- skipping
)

REM ============================================================================
REM Step 14: Reassign all users back to the recreated tablespace
REM ============================================================================
echo.
echo ^>^>^> Step 14: Reassigning all users back to !TABLESPACE_NAME!

echo SET SERVEROUTPUT ON SIZE UNLIMITED> "%TEMP%\epf_ts_q.sql"
echo BEGIN>> "%TEMP%\epf_ts_q.sql"
echo   FOR u IN (SELECT username FROM dba_users WHERE default_tablespace = '!HOLD_TS_NAME!')>> "%TEMP%\epf_ts_q.sql"
echo   LOOP>> "%TEMP%\epf_ts_q.sql"
echo     EXECUTE IMMEDIATE 'ALTER USER ' ^|^| u.username ^|^| ' DEFAULT TABLESPACE !TABLESPACE_NAME!';>> "%TEMP%\epf_ts_q.sql"
echo     DBMS_OUTPUT.PUT_LINE('Restored: ' ^|^| u.username);>> "%TEMP%\epf_ts_q.sql"
echo   END LOOP;>> "%TEMP%\epf_ts_q.sql"
echo END;>> "%TEMP%\epf_ts_q.sql"
echo />> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" 2>&1
del "%TEMP%\epf_ts_q.sql" >nul 2>&1
echo [OK]    All users reassigned back to !TABLESPACE_NAME!

REM ============================================================================
REM Step 15: Import all schemas
REM ============================================================================
echo.
echo ^>^>^> Step 15: Importing all schemas via Data Pump

set "IMPORT_LOG=epf_reclaim_import_%TIMESTAMP%.log"

echo [INFO]  Dump file:  !DATAPUMP_DIR_NAME!:!DUMP_FILE!
echo [INFO]  Import log: !DATAPUMP_DIR_NAME!:!IMPORT_LOG!

if /i "%DBA_USER%"=="sys" (
    set "IMPDP_CONNECT='%DBA_USER%/%DBA_PASSWORD%@%TNS_NAME% AS SYSDBA'"
) else (
    set "IMPDP_CONNECT=%DBA_USER%/%DBA_PASSWORD%@%TNS_NAME%"
)

echo [INFO]  Running impdp (this may take a while)...
impdp !IMPDP_CONNECT! SCHEMAS=!SCHEMA_LIST! DIRECTORY=!DATAPUMP_DIR_NAME! DUMPFILE=!DUMP_FILE! LOGFILE=!IMPORT_LOG! TABLE_EXISTS_ACTION=REPLACE 2>&1
if !ERRORLEVEL! neq 0 (
    echo [WARN]  Data Pump import completed with warnings/errors.
    echo [WARN]  Check the import log: !DATAPUMP_DIR_PATH!\!IMPORT_LOG!
) else (
    echo [OK]    Import completed successfully
)

REM ============================================================================
REM Step 16: Verify imported objects
REM ============================================================================
echo.
echo ^>^>^> Step 16: Verifying imported objects

echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 200 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT owner ^|^| ': ' ^|^| object_type ^|^| ' - Total: ' ^|^| COUNT(*) ^|^| ', Valid: ' ^|^| SUM(CASE WHEN status = 'VALID' THEN 1 ELSE 0 END) ^|^| ', Invalid: ' ^|^| SUM(CASE WHEN status = 'INVALID' THEN 1 ELSE 0 END) FROM dba_objects WHERE owner IN (SELECT username FROM dba_users WHERE default_tablespace = '!TABLESPACE_NAME!' AND oracle_maintained = 'N') GROUP BY owner, object_type ORDER BY owner, object_type;>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" 2>&1
del "%TEMP%\epf_ts_q.sql" >nul 2>&1

REM Recompile invalid objects
echo [INFO]  Recompiling any invalid objects...
echo SET SERVEROUTPUT ON SIZE UNLIMITED> "%TEMP%\epf_ts_q.sql"
for %%S in (!SCHEMA_LIST!) do (
    echo BEGIN DBMS_UTILITY.COMPILE_SCHEMA(schema =^> '%%S', compile_all =^> FALSE); END;>> "%TEMP%\epf_ts_q.sql"
    echo />> "%TEMP%\epf_ts_q.sql"
)
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" >> "%LOG_FILE%" 2>&1
del "%TEMP%\epf_ts_q.sql" >nul 2>&1
echo [OK]    Recompilation complete

REM ============================================================================
REM Step 17: Drop the holding tablespace
REM ============================================================================
echo.
echo ^>^>^> Step 17: Dropping holding tablespace !HOLD_TS_NAME!

echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0> "%TEMP%\epf_ts_q.sql"
echo DROP TABLESPACE !HOLD_TS_NAME! INCLUDING CONTENTS AND DATAFILES;>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"

sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
findstr /i "ORA-" "%TEMP%\epf_ts_r.txt" >nul 2>&1
if !ERRORLEVEL! EQU 0 (
    echo [WARN]  Could not drop holding tablespace:
    type "%TEMP%\epf_ts_r.txt"
    echo [WARN]  Drop it manually: DROP TABLESPACE !HOLD_TS_NAME! INCLUDING CONTENTS AND DATAFILES;
) else (
    echo [OK]    Holding tablespace !HOLD_TS_NAME! dropped
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

REM ============================================================================
REM Capture NEW tablespace size for comparison
REM ============================================================================
echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON> "%TEMP%\epf_ts_q.sql"
echo SELECT ROUND(SUM(bytes) / 1048576) ^|^| ' MB' FROM dba_data_files WHERE tablespace_name = '!TABLESPACE_NAME!';>> "%TEMP%\epf_ts_q.sql"
echo EXIT;>> "%TEMP%\epf_ts_q.sql"
sqlplus -S "!CONNECT_STR!" @"%TEMP%\epf_ts_q.sql" > "%TEMP%\epf_ts_r.txt" 2>&1
set "NEW_SIZE="
for /f "usebackq tokens=* delims=" %%A in ("%TEMP%\epf_ts_r.txt") do (
    set "LINE=%%A"
    for /f "tokens=* delims= " %%B in ("!LINE!") do set "LINE=%%B"
    if not "!LINE!"=="" set "NEW_SIZE=!LINE!"
)
del "%TEMP%\epf_ts_q.sql" "%TEMP%\epf_ts_r.txt" >nul 2>&1

REM ============================================================================
REM Done
REM ============================================================================
echo.
echo   ============================================================
echo   Tablespace Reclaim Complete
echo   ============================================================
echo   Tablespace !TABLESPACE_NAME! recreated as BIGFILE with AUTOEXTEND
echo.
echo   Size comparison:
echo     Before:  !CURRENT_SIZE!
echo     After:   !NEW_SIZE!
echo.
echo   Previous size: !CURRENT_SIZE!
echo   Export dump: !DATAPUMP_DIR_PATH!\!DUMP_FILE!
echo   Export log:  !DATAPUMP_DIR_PATH!\!EXPORT_LOG!
echo   Import log:  !DATAPUMP_DIR_PATH!\!IMPORT_LOG!
echo   Script log:  %LOG_FILE%
echo.
echo   [WARN]  Keep the dump file until you have verified all data
echo           and application functionality. It is your rollback.
echo.

echo Finished: %DATE% %TIME% >> "%LOG_FILE%"
endlocal
exit /b 0

REM ============================================================================
REM Help
REM ============================================================================
:show_help
echo.
echo EPF Tablespace Reclaim Tool
echo ============================
echo.
echo Reclaims disk space by exporting all schemas sharing the OPPAYMENTS
echo tablespace, dropping and recreating it as bigfile + autoextend,
echo then reimporting. Handles both PDB and non-PDB environments.
echo.
echo Usage:
echo   epf_tablespace_reclaim.bat [OPTIONS]
echo.
echo Options:
echo   --tns NAME             Oracle TNS name or connect string (required)
echo   --dba-user NAME        DBA username (default: sys)
echo   --dba-password PASS    DBA password (prefer EPF_DBA_PASSWORD env var)
echo   --sys-password PASS    Shortcut: sets --dba-user=sys --dba-password=PASS
echo   --oppayments-user NAME OPPAYMENTS schema name (default: oppayments)
echo   --datafile-path PATH   Path for the new datafile (auto-detected if omitted)
echo   --datafile-size SIZE   Initial size of new datafile (default: 10G)
echo   --autoextend-next SIZE Autoextend increment (default: 1G)
echo   --autoextend-max SIZE  Autoextend max size (default: UNLIMITED)
echo   --help, -h             Show this help message
echo.
echo Environment Variables:
echo   EPF_DBA_PASSWORD       DBA password (overrides --dba-password)
echo.
exit /b 0
