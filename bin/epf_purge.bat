@echo off
REM ============================================================================
REM EPF Data Purge - Windows Wrapper Script
REM ============================================================================
REM Deploys and executes the EPF purge PL/SQL package against an Oracle database.
REM
REM Usage:
REM   Interactive:   epf_purge.bat
REM   With config:   epf_purge.bat --config ..\config\epf_purge.conf
REM   With args:     epf_purge.bat --tns EPFPROD --user oppayments --retention 90
REM
REM Prerequisites:
REM   - Oracle SQL*Plus installed and on PATH
REM   - ORACLE_HOME environment variable set
REM   - Database user with DELETE on oppayments.*, CREATE TABLE, CREATE PROCEDURE
REM ============================================================================

setlocal enabledelayedexpansion

REM ============================================================================
REM Defaults
REM ============================================================================
set "SCRIPT_DIR=%~dp0"
set "PROJECT_DIR=%SCRIPT_DIR%.."
set "SQL_DIR=%PROJECT_DIR%\sql"
set "LOG_DIR=%PROJECT_DIR%\logs"

for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set "DT=%%I"
set "TIMESTAMP=%DT:~0,8%_%DT:~8,6%"
set "LOG_FILE=%LOG_DIR%\epf_purge_%TIMESTAMP%.log"

set "TNS_NAME="
set "USERNAME=oppayments"
set "PASSWORD="
set "RETENTION_DAYS=30"
set "PURGE_DEPTH=ALL"
set "BATCH_SIZE=1000"
set "DRY_RUN=N"
set "RECLAIM_SPACE=N"
set "RECLAIM_ONLY=N"
set "SYS_PASSWORD="
set "ASSUME_YES=N"
set "DROP_PACKAGE_AFTER=N"
set "DROP_LOGS=N"
set "CONFIG_FILE="

REM Ensure log directory exists
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo EPF Data Purge Tool > "%LOG_FILE%"
echo Started: %DATE% %TIME% >> "%LOG_FILE%"

REM ============================================================================
REM Parse arguments
REM ============================================================================
:parse_args
if "%~1"=="" goto :args_done
if /i "%~1"=="--config"     ( set "CONFIG_FILE=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--tns"        ( set "TNS_NAME=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--user"       ( set "USERNAME=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--password"   ( set "PASSWORD=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--retention"  ( set "RETENTION_DAYS=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--depth"      ( set "PURGE_DEPTH=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--batch-size" ( set "BATCH_SIZE=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--dry-run"    ( set "DRY_RUN=Y" & shift & goto :parse_args )
if /i "%~1"=="--reclaim"      ( set "RECLAIM_SPACE=Y" & shift & goto :parse_args )
if /i "%~1"=="--reclaim-only" ( set "RECLAIM_ONLY=Y" & set "RECLAIM_SPACE=Y" & shift & goto :parse_args )
if /i "%~1"=="--sys-password"  ( set "SYS_PASSWORD=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--assume-yes"    ( set "ASSUME_YES=Y" & shift & goto :parse_args )
if /i "%~1"=="-y"              ( set "ASSUME_YES=Y" & shift & goto :parse_args )
if /i "%~1"=="--drop-pkg"   ( set "DROP_PACKAGE_AFTER=Y" & shift & goto :parse_args )
if /i "%~1"=="--drop-logs"  ( set "DROP_LOGS=Y" & shift & goto :parse_args )
if /i "%~1"=="--help"       ( goto :show_help )
if /i "%~1"=="-h"           ( goto :show_help )
echo [ERROR] Unknown argument: %~1
goto :show_help
:args_done

REM ============================================================================
REM Load config file if specified
REM ============================================================================
if not "%CONFIG_FILE%"=="" (
    if not exist "%CONFIG_FILE%" (
        echo [ERROR] Config file not found: %CONFIG_FILE%
        exit /b 1
    )
    echo [INFO]  Loading configuration from: %CONFIG_FILE%
    for /f "usebackq eol=# tokens=1,* delims==" %%A in ("%CONFIG_FILE%") do (
        set "%%A=%%B"
    )
)

REM Environment variable overrides for password
if defined EPF_PURGE_PASSWORD set "PASSWORD=%EPF_PURGE_PASSWORD%"

REM ============================================================================
REM --reclaim-only short-circuit: skip purge entirely, run reclaim tool only
REM ============================================================================
if /i "%RECLAIM_ONLY%"=="Y" (
    if "%TNS_NAME%"=="" (
        echo.
        echo   ============================================================
        echo   EPF Tablespace Reclaim ^(RECLAIM-ONLY MODE - no purge^)
        echo   ============================================================
        set /p "TNS_NAME=  Enter TNS name: "
    )
    set "RECLAIM_ARGS=--tns "!TNS_NAME!""
    if not "!SYS_PASSWORD!"=="" set "RECLAIM_ARGS=!RECLAIM_ARGS! --sys-password "!SYS_PASSWORD!""
    if /i "!ASSUME_YES!"=="Y"   set "RECLAIM_ARGS=!RECLAIM_ARGS! --assume-yes"
    echo [INFO]  Skipping purge. Delegating to epf_tablespace_reclaim.bat
    call "%SCRIPT_DIR%epf_tablespace_reclaim.bat" !RECLAIM_ARGS!
    exit /b !ERRORLEVEL!
)

REM ============================================================================
REM Interactive prompts ONLY if key params missing (no TNS or no password)
REM When CLI args provide TNS + password, skip all interactive prompts.
REM ============================================================================
set "INTERACTIVE=N"
if "%TNS_NAME%"=="" set "INTERACTIVE=Y"
if "%PASSWORD%"=="" set "INTERACTIVE=Y"

if /i "%INTERACTIVE%"=="Y" (
    if "%TNS_NAME%"=="" (
        echo.
        echo   ============================================================
        echo   EPF Data Purge - Configuration
        echo   ============================================================
        echo.
        echo   TNS Name / Connect String
        echo   This is the Oracle service name or TNS alias used to connect
        echo   to the database. Example: EPFPROD, localhost:1521/orcl
        set /p "TNS_NAME=  Enter TNS name: "
    )

    if "%PASSWORD%"=="" (
        echo.
        echo   Database User: %USERNAME%
        echo   Enter the password for this database user.
        REM Use PowerShell for masked password input
        for /f "usebackq delims=" %%P in (`powershell -Command "$p = Read-Host '  Password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))"`) do set "PASSWORD=%%P"
    )

    echo.
    echo   Retention Period
    echo   Data older than this many days will be purged.
    echo   Current value: !RETENTION_DAYS! days
    set /p "RETENTION_INPUT=  Retention days [!RETENTION_DAYS!]: "
    if not "!RETENTION_INPUT!"=="" set "RETENTION_DAYS=!RETENTION_INPUT!"

    echo.
    echo   Purge Depth
    echo   Controls which data modules are purged:
    echo     ALL             - Purge all modules (payments, logs, bank statements)
    echo     PAYMENTS        - Purge bulk payments and file integrations only
    echo     LOGS            - Purge audit trails and technical logs only
    echo     BANK_STATEMENTS - Purge bank statement dispatching only
    set /p "DEPTH_INPUT=  Purge depth [!PURGE_DEPTH!]: "
    if not "!DEPTH_INPUT!"=="" set "PURGE_DEPTH=!DEPTH_INPUT!"

    echo.
    echo   Batch Size
    echo   Number of parent records processed per commit. Larger = faster
    echo   but uses more undo/redo space. Recommended: 500-5000.
    set /p "BATCH_INPUT=  Batch size [!BATCH_SIZE!]: "
    if not "!BATCH_INPUT!"=="" set "BATCH_SIZE=!BATCH_INPUT!"

    echo.
    echo   Dry Run
    echo   If yes, the tool will count how many rows would be deleted
    echo   without actually deleting anything. Good for a first test.
    set /p "DRY_INPUT=  Dry run? (Y/N) [!DRY_RUN!]: "
    if not "!DRY_INPUT!"=="" set "DRY_RUN=!DRY_INPUT!"

    echo.
    echo   Space Reclamation
    echo   After purging, attempt to reclaim space within Oracle tablespaces
    echo   using SHRINK SPACE.
    set /p "RECLAIM_INPUT=  Reclaim space? (Y/N) [!RECLAIM_SPACE!]: "
    if not "!RECLAIM_INPUT!"=="" set "RECLAIM_SPACE=!RECLAIM_INPUT!"

    REM If reclaim requested, collect SYS password NOW so the long-running
    REM purge can finish unattended and chain into the reclaim tool.
    if /i "!RECLAIM_SPACE!"=="Y" (
        if "!SYS_PASSWORD!"=="" (
            echo.
            echo   Reclaim requires DBA/SYS credentials to drop and recreate
            echo   the tablespace. Enter the SYS password now so the reclaim
            echo   step runs unattended after the purge.
            for /f "usebackq delims=" %%P in (`powershell -Command "$p = Read-Host '  SYS password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))"`) do set "SYS_PASSWORD=%%P"
        )
    )

    echo.
    echo   Drop Package After Execution
    echo   If yes, the PL/SQL package will be removed from the database
    echo   after the purge completes. The log table is preserved.
    set /p "DROP_INPUT=  Drop package after? (Y/N) [!DROP_PACKAGE_AFTER!]: "
    if not "!DROP_INPUT!"=="" set "DROP_PACKAGE_AFTER=!DROP_INPUT!"
)

REM ============================================================================
REM Display configuration summary
REM ============================================================================
echo.
echo   ============================================================
echo   Configuration Summary
echo   ============================================================
echo   TNS Name:       %TNS_NAME%
echo   Username:       %USERNAME%
echo   Retention:      %RETENTION_DAYS% days
echo   Purge Depth:    %PURGE_DEPTH%
echo   Batch Size:     %BATCH_SIZE%
echo   Dry Run:        %DRY_RUN%
echo   Reclaim Space:  %RECLAIM_SPACE%
echo   Drop Package:   %DROP_PACKAGE_AFTER%
echo   Log File:       %LOG_FILE%
echo   ============================================================
echo.

REM ============================================================================
REM Check prerequisites
REM ============================================================================
echo [INFO]  Checking prerequisites...

where sqlplus >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] SQL*Plus not found on PATH. Install Oracle Client.
    echo [ERROR] SQL*Plus not found >> "%LOG_FILE%"
    exit /b 1
)
echo [OK]    SQL*Plus found

if "%ORACLE_HOME%"=="" (
    echo [WARN]  ORACLE_HOME not set. SQL*Plus may still work.
) else (
    echo [OK]    ORACLE_HOME set: %ORACLE_HOME%
)

REM Test connectivity
echo [INFO]  Testing database connectivity...
echo SELECT 'CONNECTION_OK' FROM DUAL; > "%TEMP%\epf_test.sql"
echo EXIT; >> "%TEMP%\epf_test.sql"

sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" @"%TEMP%\epf_test.sql" > "%TEMP%\epf_test_result.txt" 2>&1
findstr /i "CONNECTION_OK" "%TEMP%\epf_test_result.txt" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Database connection failed. Check credentials and TNS name.
    type "%TEMP%\epf_test_result.txt"
    type "%TEMP%\epf_test_result.txt" >> "%LOG_FILE%"
    del "%TEMP%\epf_test.sql" "%TEMP%\epf_test_result.txt" >nul 2>&1
    exit /b 1
)
del "%TEMP%\epf_test.sql" "%TEMP%\epf_test_result.txt" >nul 2>&1
echo [OK]    Database connection successful

REM ============================================================================
REM Deploy PL/SQL package
REM ============================================================================
echo.
echo   ============================================================
echo   Deploying PL/SQL Package
echo   ============================================================

for %%F in (01_create_purge_log_table.sql 02_epf_purge_pkg_spec.sql 03_epf_purge_pkg_body.sql) do (
    echo [INFO]  Running: %%F
    (
        echo SET SERVEROUTPUT ON SIZE UNLIMITED
        echo SET ECHO OFF FEEDBACK ON
        echo @"%SQL_DIR%\%%F"
        echo EXIT;
    ) | sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" >> "%LOG_FILE%" 2>&1
    if !ERRORLEVEL! neq 0 (
        echo [ERROR] Failed: %%F
    ) else (
        echo [OK]    %%F executed
    )
)

REM Check compilation errors
echo [INFO]  Checking for package compilation errors...
echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 200 > "%TEMP%\epf_check.sql"
echo SELECT type ^|^| ': Line ' ^|^| line ^|^| ' - ' ^|^| text FROM user_errors WHERE name = 'EPF_PURGE_PKG' ORDER BY type, sequence; >> "%TEMP%\epf_check.sql"
echo EXIT; >> "%TEMP%\epf_check.sql"

sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" @"%TEMP%\epf_check.sql" > "%TEMP%\epf_check_result.txt" 2>&1
for %%A in ("%TEMP%\epf_check_result.txt") do if %%~zA gtr 5 (
    echo [ERROR] Package has compilation errors:
    type "%TEMP%\epf_check_result.txt"
    type "%TEMP%\epf_check_result.txt" >> "%LOG_FILE%"
    del "%TEMP%\epf_check.sql" "%TEMP%\epf_check_result.txt" >nul 2>&1
    exit /b 1
)
del "%TEMP%\epf_check.sql" "%TEMP%\epf_check_result.txt" >nul 2>&1
echo [OK]    Package compiled without errors

REM ============================================================================
REM Execute purge
REM ============================================================================
echo.
echo   ============================================================
echo   Executing Purge
echo   ============================================================

set "DRY_RUN_BOOL=FALSE"
set "RECLAIM_BOOL=FALSE"
if /i "%DRY_RUN%"=="Y" set "DRY_RUN_BOOL=TRUE"
if /i "%RECLAIM_SPACE%"=="Y" set "RECLAIM_BOOL=TRUE"

echo [INFO]  Parameters: retention=%RETENTION_DAYS% days, depth=%PURGE_DEPTH%, batch=%BATCH_SIZE%

echo SET SERVEROUTPUT ON SIZE UNLIMITED> "%TEMP%\epf_exec.sql"
echo SET TIMING ON>> "%TEMP%\epf_exec.sql"
echo SET ECHO OFF FEEDBACK OFF>> "%TEMP%\epf_exec.sql"
echo BEGIN>> "%TEMP%\epf_exec.sql"
echo     oppayments.epf_purge_pkg.run_purge(>> "%TEMP%\epf_exec.sql"
echo         p_retention_days =^> %RETENTION_DAYS%,>> "%TEMP%\epf_exec.sql"
echo         p_purge_depth    =^> '%PURGE_DEPTH%',>> "%TEMP%\epf_exec.sql"
echo         p_batch_size     =^> %BATCH_SIZE%,>> "%TEMP%\epf_exec.sql"
echo         p_dry_run        =^> %DRY_RUN_BOOL%>> "%TEMP%\epf_exec.sql"
echo     ^);>> "%TEMP%\epf_exec.sql"
echo END;>> "%TEMP%\epf_exec.sql"
echo />> "%TEMP%\epf_exec.sql"
echo EXIT;>> "%TEMP%\epf_exec.sql"

REM Stream output live to console (piped through more for real-time display)
REM and also capture to log file via PowerShell Tee-Object
powershell -Command "& { sqlplus -S '%USERNAME%/%PASSWORD%@%TNS_NAME%' '@%TEMP%\epf_exec.sql' 2>&1 | Tee-Object -FilePath '%LOG_FILE%' -Append }"
del "%TEMP%\epf_exec.sql" >nul 2>&1

echo [OK]    Purge execution completed

REM ============================================================================
REM Invoke tablespace reclaim tool if requested
REM ============================================================================
if /i "%RECLAIM_SPACE%"=="Y" (
    if /i "%DRY_RUN%"=="Y" (
        echo [INFO]  Skipping tablespace reclaim ^(dry run^)
    ) else (
        echo.
        echo   ============================================================
        echo   Invoking Tablespace Reclaim Tool
        echo   ============================================================
        echo [INFO]  --reclaim now delegates to epf_tablespace_reclaim.bat
        echo [INFO]  ^(export/import/recreate-as-BIGFILE^). DBA credentials required.
        call "%SCRIPT_DIR%epf_tablespace_reclaim.bat" --tns "%TNS_NAME%" --sys-password "%SYS_PASSWORD%" --assume-yes
        if !ERRORLEVEL! neq 0 (
            echo [WARN]  Tablespace reclaim did not complete successfully.
        )
    )
)

REM ============================================================================
REM Drop package if requested
REM ============================================================================
if /i "%DROP_PACKAGE_AFTER%"=="Y" (
    echo.
    echo [INFO]  Dropping PL/SQL package...
    (
        echo @"%SQL_DIR%\04_drop_epf_purge_pkg.sql"
        echo EXIT;
    ) | sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" >> "%LOG_FILE%" 2>&1
    echo [OK]    Package dropped
)

REM ============================================================================
REM Drop purge log tables if requested
REM ============================================================================
if /i "%DROP_LOGS%"=="Y" (
    echo.
    echo [INFO]  Dropping purge log tables...
    echo DROP TABLE oppayments.epf_purge_space_snapshot PURGE;> "%TEMP%\epf_droplogs.sql"
    echo DROP TABLE oppayments.epf_purge_log PURGE;>> "%TEMP%\epf_droplogs.sql"
    echo EXIT;>> "%TEMP%\epf_droplogs.sql"
    sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" @"%TEMP%\epf_droplogs.sql" >> "%LOG_FILE%" 2>&1
    del "%TEMP%\epf_droplogs.sql" >nul 2>&1
    echo [OK]    Purge log tables dropped
)

REM ============================================================================
REM Done
REM ============================================================================
echo.
echo [OK]    EPF Data Purge completed. Log: %LOG_FILE%
echo Finished: %DATE% %TIME% >> "%LOG_FILE%"

endlocal
exit /b 0

REM ============================================================================
REM Help
REM ============================================================================
:show_help
echo.
echo EPF Data Purge Tool
echo ===================
echo.
echo Usage:
echo   epf_purge.bat [OPTIONS]
echo.
echo Options:
echo   --config FILE     Load settings from config file
echo   --tns NAME        Oracle TNS name or connect string
echo   --user NAME       Database username (default: oppayments)
echo   --password PASS   Database password (prefer EPF_PURGE_PASSWORD env var)
echo   --retention N     Purge data older than N days (default: 30)
echo   --depth DEPTH     Purge scope: ALL, PAYMENTS, LOGS, BANK_STATEMENTS
echo   --batch-size N    Rows per batch commit (default: 1000)
echo   --dry-run         Count rows only, do not delete anything
echo   --reclaim         After purge, invoke epf_tablespace_reclaim.bat
echo   --reclaim-only    Skip purge entirely, run reclaim tool only
echo   --drop-pkg        Drop the PL/SQL package after execution
echo   --drop-logs       Drop purge log tables (epf_purge_log, epf_purge_space_snapshot)
echo   --help, -h        Show this help message
echo.
echo Environment Variables:
echo   EPF_PURGE_PASSWORD   Database password (overrides config and --password)
echo.
echo Examples:
echo   epf_purge.bat
echo   epf_purge.bat --tns EPFPROD --user oppayments --retention 90 --dry-run
echo   epf_purge.bat --config ..\config\epf_purge.conf
echo.
exit /b 0
