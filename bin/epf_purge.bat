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
set "SKIP_STALL_CHECKS=N"
set "OPTIMIZE_DB=N"
set "SYS_PASSWORD="
set "ASSUME_YES=N"
set "DROP_PACKAGE_AFTER=N"
set "DROP_LOGS=N"
set "TRUNCATE_LOGS=N"
set "SHOW_SIZES=N"
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
if /i "%~1"=="--reclaim-online"      ( set "RECLAIM_SPACE=Y" & shift & goto :parse_args )
if /i "%~1"=="--reclaim-online-only" ( set "RECLAIM_ONLY=Y" & set "RECLAIM_SPACE=Y" & shift & goto :parse_args )
if /i "%~1"=="--no-stall-check" ( set "SKIP_STALL_CHECKS=Y" & shift & goto :parse_args )
if /i "%~1"=="--optimize-db" ( set "OPTIMIZE_DB=Y" & shift & goto :parse_args )
if /i "%~1"=="--sys-password"  ( set "SYS_PASSWORD=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--assume-yes"    ( set "ASSUME_YES=Y" & shift & goto :parse_args )
if /i "%~1"=="-y"              ( set "ASSUME_YES=Y" & shift & goto :parse_args )
if /i "%~1"=="--drop-pkg"   ( set "DROP_PACKAGE_AFTER=Y" & shift & goto :parse_args )
if /i "%~1"=="--drop-logs"  ( set "DROP_LOGS=Y" & shift & goto :parse_args )
if /i "%~1"=="--truncate-logs" ( set "TRUNCATE_LOGS=Y" & shift & goto :parse_args )
if /i "%~1"=="--show-sizes" ( set "SHOW_SIZES=Y" & shift & goto :parse_args )
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
REM --reclaim-only short-circuit: skip purge entirely, run online reclaim only
REM ============================================================================
if /i "%RECLAIM_ONLY%"=="Y" (
    if "%TNS_NAME%"=="" (
        echo.
        echo   ============================================================
        echo   EPF Space Reclaim ^(RECLAIM-ONLY MODE - no purge^)
        echo   ============================================================
        set /p "TNS_NAME=  Enter TNS name: "
    )
    if "!SYS_PASSWORD!"=="" (
        for /f "usebackq delims=" %%P in (`powershell -Command "$p = Read-Host '  SYS password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))"`) do set "SYS_PASSWORD=%%P"
    )
    echo [INFO]  Skipping purge. Running online reclaim only.
    echo DEFINE skip_stall_checks = !SKIP_STALL_CHECKS!> "%TEMP%\epf_reclaim_online.sql"
    echo @"%SQL_DIR%\05_reclaim_tablespace.sql">> "%TEMP%\epf_reclaim_online.sql"
    echo EXIT;>> "%TEMP%\epf_reclaim_online.sql"
    powershell -Command "& { $fs=[IO.FileStream]::new('%LOG_FILE%','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S 'sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA' '@%TEMP%\epf_reclaim_online.sql' 2>&1 | ForEach-Object { $_; $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
    del "%TEMP%\epf_reclaim_online.sql" >nul 2>&1
    echo [OK]    Online reclaim completed. Log: %LOG_FILE%
    exit /b 0
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
    echo   Show Module Data Sizes ^(--show-sizes^)
    echo   Queries the database to show data sizes per purge module
    echo   to help you choose the appropriate purge depth.
    set /p "SIZES_INPUT=  Show data sizes? (Y/N) [!SHOW_SIZES!]: "
    if not "!SIZES_INPUT!"=="" set "SHOW_SIZES=!SIZES_INPUT!"

    if /i "!SHOW_SIZES!"=="Y" (
        echo.
        echo [INFO]  Querying data sizes per module...
        sqlplus -S "!USERNAME!/!PASSWORD!@!TNS_NAME!" @"%SQL_DIR%\11_show_module_sizes.sql" 2>nul
    )

    echo.
    echo   Purge Depth
    echo   Controls which data modules are purged:
    echo     ALL             - Purge all modules ^(payments, logs, bank statements^)
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
    echo   Drop Package After Execution
    echo   If yes, the PL/SQL package will be removed from the database
    echo   after the purge completes. The log table is preserved.
    set /p "DROP_INPUT=  Drop package after? (Y/N) [!DROP_PACKAGE_AFTER!]: "
    if not "!DROP_INPUT!"=="" set "DROP_PACKAGE_AFTER=!DROP_INPUT!"

    echo.
    echo   Truncate Purge Logs ^(--truncate-logs^)
    echo   Clears all previous purge run history from the log tables.
    echo   Useful when re-running after a failed or test purge.
    set /p "TRUNC_INPUT=  Truncate logs? (Y/N) [!TRUNCATE_LOGS!]: "
    if not "!TRUNC_INPUT!"=="" set "TRUNCATE_LOGS=!TRUNC_INPUT!"

    echo.
    echo   Pre-Purge Database Optimization ^(--optimize-db^)
    echo   Enlarges redo logs to 1 GB and gathers optimizer statistics.
    echo   Recommended for first-time purge on databases with small redo logs.
    echo   Requires SYS/DBA credentials. Idempotent and auto-reverts on failure.
    echo   ^>^> Extra disk space: ~4 GB temporary ^(new redo logs before old ones deleted^)
    set /p "OPTDB_INPUT=  Optimize DB? (Y/N) [!OPTIMIZE_DB!]: "
    if not "!OPTDB_INPUT!"=="" set "OPTIMIZE_DB=!OPTDB_INPUT!"

    echo.
    echo   Post-Purge Space Reclaim ^(--reclaim^)
    echo   After purge, shrinks and squeezes the tablespace to free OS disk space.
    echo   Online operation ^(no downtime^). Requires SYS/DBA credentials.
    echo   ^>^> No extra disk space needed ^(MOVE uses existing free space in tablespace^)
    set /p "RECLAIM_INPUT=  Reclaim space? (Y/N) [!RECLAIM_SPACE!]: "
    if not "!RECLAIM_INPUT!"=="" set "RECLAIM_SPACE=!RECLAIM_INPUT!"

    if /i "!RECLAIM_SPACE!"=="Y" (
        echo.
        echo   Skip Stall Checks ^(--no-stall-check^)
        echo   When enabled, reclaim always runs all 2000 iterations without
        echo   stopping early on zero-progress checkpoints.
        set /p "STALL_INPUT=  Skip stall checks? (Y/N) [!SKIP_STALL_CHECKS!]: "
        if not "!STALL_INPUT!"=="" set "SKIP_STALL_CHECKS=!STALL_INPUT!"
    )
)

REM Prompt for SYS password if optimize-db or reclaim enabled (interactive)
if /i "%OPTIMIZE_DB%"=="Y" if "!SYS_PASSWORD!"=="" (
    echo.
    echo   SYS/DBA password ^(needed for optimize-db and/or reclaim^)
    for /f "usebackq delims=" %%P in (`powershell -Command "$p = Read-Host '  SYS password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))"`) do set "SYS_PASSWORD=%%P"
)
if /i "%RECLAIM_SPACE%"=="Y" if "!SYS_PASSWORD!"=="" (
    echo.
    echo   SYS/DBA password ^(needed for reclaim^)
    for /f "usebackq delims=" %%P in (`powershell -Command "$p = Read-Host '  SYS password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))"`) do set "SYS_PASSWORD=%%P"
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
echo   Optimize DB:    %OPTIMIZE_DB%
echo   Reclaim Space:  %RECLAIM_SPACE%
if /i "%RECLAIM_SPACE%"=="Y" echo   Skip Stall:     %SKIP_STALL_CHECKS%
echo   Drop Package:   %DROP_PACKAGE_AFTER%
echo   Truncate Logs:  %TRUNCATE_LOGS%
echo   Log File:       %LOG_FILE%
echo   ============================================================
echo.
echo   --- Approximate Disk Space Requirements ---
echo   Purge:        ~2-5 GB temporary (UNDO growth, auto-recovered after retention)
if /i "%OPTIMIZE_DB%"=="Y" echo   Optimize DB:  ~4 GB temporary (4x1GB redo logs, old ones deleted after)
if /i "%RECLAIM_SPACE%"=="Y" echo   Reclaim:      No extra space (uses existing free space in tablespace)
if /i "%OPTIMIZE_DB%"=="Y" (
    echo   [WARN]  PEAK TOTAL:   ~9 GB of temporary free disk space required
) else (
    echo   [WARN]  PEAK TOTAL:   ~5 GB of temporary free disk space required
)
echo   The purge itself frees space; this is only the temporary overhead during execution.
echo.

REM ============================================================================
REM Check prerequisites
REM ============================================================================
call :log "[INFO]  Checking prerequisites..."

where sqlplus >nul 2>&1
if %ERRORLEVEL% neq 0 (
    call :log "[ERROR] SQL*Plus not found on PATH. Install Oracle Client."
    exit /b 1
)
call :log "[OK]    SQL*Plus found"

if "%ORACLE_HOME%"=="" (
    call :log "[WARN]  ORACLE_HOME not set. SQL*Plus may still work."
) else (
    call :log "[OK]    ORACLE_HOME set: %ORACLE_HOME%"
)

REM Test connectivity
call :log "[INFO]  Testing database connectivity..."
echo SELECT 'CONNECTION_OK' FROM DUAL; > "%TEMP%\epf_test.sql"
echo EXIT; >> "%TEMP%\epf_test.sql"

sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" @"%TEMP%\epf_test.sql" > "%TEMP%\epf_test_result.txt" 2>&1
findstr /i "CONNECTION_OK" "%TEMP%\epf_test_result.txt" >nul 2>&1
if %ERRORLEVEL% neq 0 (
    call :log "[ERROR] Database connection failed. Check credentials and TNS name."
    type "%TEMP%\epf_test_result.txt"
    type "%TEMP%\epf_test_result.txt" >> "%LOG_FILE%"
    del "%TEMP%\epf_test.sql" "%TEMP%\epf_test_result.txt" >nul 2>&1
    exit /b 1
)
del "%TEMP%\epf_test.sql" "%TEMP%\epf_test_result.txt" >nul 2>&1
call :log "[OK]    Database connection successful"

REM Show module sizes if requested (non-interactive mode)
if /i "%SHOW_SIZES%"=="Y" (
    if /i not "%INTERACTIVE%"=="Y" (
        echo.
        echo [INFO]  Querying data sizes per module...
        sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" @"%SQL_DIR%\11_show_module_sizes.sql" 2>nul
    )
)

REM ============================================================================
REM Pre-purge DB optimization if requested
REM ============================================================================
if /i "%OPTIMIZE_DB%"=="Y" (
    if "!SYS_PASSWORD!"=="" (
        echo   DB optimization requires DBA/SYS credentials.
        for /f "usebackq delims=" %%P in (`powershell -Command "$p = Read-Host '  SYS password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))"`) do set "SYS_PASSWORD=%%P"
    )
    echo.
    echo   ============================================================
    echo   Pre-Purge Database Optimization
    echo   ============================================================
    echo @"%SQL_DIR%\06_optimize_db.sql"> "%TEMP%\epf_optimize.sql"
    echo EXIT;>> "%TEMP%\epf_optimize.sql"
    powershell -Command "& { $fs=[IO.FileStream]::new('%LOG_FILE%','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S 'sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA' '@%TEMP%\epf_optimize.sql' 2>&1 | ForEach-Object { $_; $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
    del "%TEMP%\epf_optimize.sql" >nul 2>&1
    call :log "[OK]    DB optimization completed"
)

REM ============================================================================
REM Deploy PL/SQL package
REM ============================================================================
call :log "."
call :log "  ============================================================"
call :log "  Deploying PL/SQL Package"
call :log "  ============================================================"

for %%F in (01_create_purge_log_table.sql 02_epf_purge_pkg_spec.sql 03_epf_purge_pkg_body.sql) do (
    call :log "[INFO]  Running: %%F"
    (
        echo SET SERVEROUTPUT ON SIZE UNLIMITED
        echo SET ECHO OFF FEEDBACK ON
        echo @"%SQL_DIR%\%%F"
        echo EXIT;
    ) | sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" >> "%LOG_FILE%" 2>&1
    if !ERRORLEVEL! neq 0 (
        call :log "[ERROR] Failed: %%F"
    ) else (
        call :log "[OK]    %%F executed"
    )
)

REM Check compilation errors
call :log "[INFO]  Checking for package compilation errors..."
echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 200 > "%TEMP%\epf_check.sql"
echo SELECT type ^|^| ': Line ' ^|^| line ^|^| ' - ' ^|^| text FROM user_errors WHERE name = 'EPF_PURGE_PKG' ORDER BY type, sequence; >> "%TEMP%\epf_check.sql"
echo EXIT; >> "%TEMP%\epf_check.sql"

sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" @"%TEMP%\epf_check.sql" > "%TEMP%\epf_check_result.txt" 2>&1
for %%A in ("%TEMP%\epf_check_result.txt") do if %%~zA gtr 5 (
    call :log "[ERROR] Package has compilation errors:"
    type "%TEMP%\epf_check_result.txt"
    type "%TEMP%\epf_check_result.txt" >> "%LOG_FILE%"
    del "%TEMP%\epf_check.sql" "%TEMP%\epf_check_result.txt" >nul 2>&1
    exit /b 1
)
del "%TEMP%\epf_check.sql" "%TEMP%\epf_check_result.txt" >nul 2>&1
call :log "[OK]    Package compiled without errors"

REM ============================================================================
REM Grant DBA view access for space snapshots (needs SYS)
REM ============================================================================
REM The space comparison needs dba_segments to match reclaim report numbers.
REM Grants are idempotent and only run when SYS password is available.
if not "!SYS_PASSWORD!"=="" (
    call :log "[INFO]  Granting DBA view access to %USERNAME% for space snapshots..."
    (
        echo SET HEADING OFF FEEDBACK OFF
        echo GRANT SELECT ON sys.dba_segments TO %USERNAME%;
        echo GRANT SELECT ON sys.dba_lobs TO %USERNAME%;
        echo EXIT;
    ) | sqlplus -S "sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA" >> "%LOG_FILE%" 2>&1
    call :log "[OK]    DBA view grants applied"
)

REM ============================================================================
REM Truncate purge logs if requested (clear old run history)
REM ============================================================================
if /i "%TRUNCATE_LOGS%"=="Y" (
    call :log "[INFO]  Truncating purge log tables..."
    (
        echo TRUNCATE TABLE oppayments.epf_purge_log;
        echo TRUNCATE TABLE oppayments.epf_purge_space_snapshot;
        echo EXIT;
    ) | sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" >nul 2>&1
    call :log "[OK]    Purge logs truncated"
)

REM ============================================================================
REM Execute purge
REM ============================================================================
call :log "."
call :log "  ============================================================"
call :log "  Executing Purge"
call :log "  ============================================================"

set "DRY_RUN_BOOL=FALSE"
set "RECLAIM_BOOL=FALSE"
if /i "%DRY_RUN%"=="Y" set "DRY_RUN_BOOL=TRUE"
if /i "%RECLAIM_SPACE%"=="Y" set "RECLAIM_BOOL=TRUE"

call :log "[INFO]  Parameters: retention=%RETENTION_DAYS% days, depth=%PURGE_DEPTH%, batch=%BATCH_SIZE%"

REM ============================================================================
REM Create temporary FK indexes for purge performance (optional, with --optimize-db)
REM ============================================================================
if /i "%OPTIMIZE_DB%"=="Y" (
    if /i not "%DRY_RUN%"=="Y" (
        call :log "[INFO]  Creating temporary FK indexes for purge performance..."
        (
            echo SET SERVEROUTPUT ON SIZE UNLIMITED
            echo @"%SQL_DIR%\06b_create_purge_indexes.sql"
        ) | sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" >> "%LOG_FILE%" 2>&1
        call :log "[OK]    Temporary FK indexes created"
    )
)

REM ============================================================================
REM Pre-purge: Tune UNDO to prevent excessive tablespace growth
REM ============================================================================
REM Bulk deletes generate large amounts of undo data. By default Oracle keeps
REM expired undo for undo_retention seconds (typically 900s = 15 min).
REM During a multi-hour purge this causes the undo tablespace to grow unbounded.
REM We lower undo_retention to 60s and cap the datafile autoextend max to limit
REM growth.  The original values are restored after the purge.
if not "!SYS_PASSWORD!"=="" (
    if /i not "%DRY_RUN%"=="Y" (
        echo [INFO]  Tuning UNDO for bulk delete ^(retention=60s, maxsize=8G^)
        sqlplus -S "sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA" @"%SQL_DIR%\08_undo_tune.sql" 2>&1
    )
)

REM ============================================================================
REM Start live progress monitor in a SEPARATE console window
REM ============================================================================
REM Earlier attempts to share this console with the monitor (Start-Process
REM -NoNewWindow) had handle-inheritance buffering issues that hid live output
REM until Ctrl+C. A separate console window owns its own console handle, so
REM output is flushed normally.
REM
REM Layout:
REM   - This window  : summary lines only ([INFO]/[OK]/[WARN] from the wrapper)
REM   - Monitor window: live updates polled from epf_purge_log every 10s
REM   - Log file     : both summary and live updates appended together
set "MONITOR_PID="
set "MONITOR_SCRIPT=%SCRIPT_DIR%epf_monitor.ps1"

if not exist "!MONITOR_SCRIPT!" (
    echo [WARN]  Monitor script not found: !MONITOR_SCRIPT!
    echo [WARN]  Purge will continue without live monitor. Tail %LOG_FILE% for log.
    goto :skip_monitor_start
)

REM Write a tiny launcher .bat that the new console window will run. Using a
REM file instead of inline arguments avoids the cmd-^>powershell-^>cmd quote
REM escaping mess. Each line is appended individually with ^>^> to AVOID a
REM parenthesised redirection block: cmd's paren parser counts ^( and ^) inside
REM the block (including inside echo arguments) and any unbalanced or
REM dot-adjacent paren breaks parsing with ". was unexpected at this time.".
REM The trailing pause keeps the window open after the monitor exits so the
REM operator can read the final RECLAIM_END / RUN_END lines.
> "%TEMP%\epf_monitor_launcher.bat" echo @echo off
>> "%TEMP%\epf_monitor_launcher.bat" echo title EPF Live Monitor
>> "%TEMP%\epf_monitor_launcher.bat" echo echo EPF Live progress monitor.
>> "%TEMP%\epf_monitor_launcher.bat" echo echo Connection: %USERNAME%/******@%TNS_NAME%
>> "%TEMP%\epf_monitor_launcher.bat" echo echo Log file:   %LOG_FILE%
>> "%TEMP%\epf_monitor_launcher.bat" echo powershell -ExecutionPolicy Bypass -File "%MONITOR_SCRIPT%" -ConnStr "%USERNAME%/%PASSWORD%@%TNS_NAME%" -PollSec 10 -MaxWaitMin 360 -LogFile "%LOG_FILE%"
>> "%TEMP%\epf_monitor_launcher.bat" echo echo [Monitor exited. Press any key to close this window.]
>> "%TEMP%\epf_monitor_launcher.bat" echo pause ^>nul

REM Spawn the launcher in a NEW console window. PowerShell's Start-Process
REM without -NoNewWindow creates a new console; -PassThru gives us the PID.
REM Top-level echoes do NOT need parens escaping. The monitor messages here
REM avoid parens entirely just to keep parsing trivial.
echo [INFO]  Opening live progress monitor in a separate console window...
echo [INFO]  This window will keep showing summary lines only.
echo [INFO]  All output also written to: %LOG_FILE%

powershell -Command "& { try { $p = Start-Process cmd -ArgumentList '/c','%TEMP%\epf_monitor_launcher.bat' -PassThru -ErrorAction Stop; $p.Id | Out-File -FilePath '%TEMP%\epf_monitor_pid.txt' -Encoding ascii } catch { $_.Exception.Message | Out-File -FilePath '%TEMP%\epf_monitor_err.txt' -Encoding ascii } }" 2>nul

if exist "%TEMP%\epf_monitor_err.txt" (
    set /p MONITOR_ERR=<"%TEMP%\epf_monitor_err.txt"
    echo [WARN]  Failed to open monitor window: !MONITOR_ERR!
    echo [WARN]  Purge will continue without live monitor. Tail %LOG_FILE% for log.
    del "%TEMP%\epf_monitor_err.txt" >nul 2>&1
    goto :skip_monitor_start
)

REM Inside if/else blocks, ALL parens count toward the block's paren depth --
REM including parens inside echo arguments. We use ^( and ^) to escape so the
REM parens print literally without disturbing the parser.
if exist "%TEMP%\epf_monitor_pid.txt" (
    set /p MONITOR_PID=<"%TEMP%\epf_monitor_pid.txt"
    echo [OK]    Live monitor opened in separate window ^(cmd PID: !MONITOR_PID!^).
) else (
    echo [WARN]  Monitor may not have started ^(no PID captured^).
    echo [WARN]  Purge will continue without live monitor. Tail %LOG_FILE% for log.
)
del "%TEMP%\epf_monitor_pid.txt" >nul 2>&1

:skip_monitor_start

echo SET SERVEROUTPUT ON SIZE UNLIMITED> "%TEMP%\epf_exec.sql"
echo SET LINESIZE 200>> "%TEMP%\epf_exec.sql"
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

REM Stream output live to console and capture to log file in UTF-8
REM Use FileStream with FileShare.ReadWrite so the monitor process can also write to the log
powershell -Command "& { $fs=[IO.FileStream]::new('%LOG_FILE%','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S '%USERNAME%/%PASSWORD%@%TNS_NAME%' '@%TEMP%\epf_exec.sql' 2>&1 | ForEach-Object { $_; $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
del "%TEMP%\epf_exec.sql" >nul 2>&1

call :log "[OK]    Purge execution completed"

REM ============================================================================
REM Post-purge: Restore undo_retention to Oracle default
REM ============================================================================
REM Use PowerShell FileStream(...,'ReadWrite') for the log append so we can
REM share the file with the running monitor process. Plain cmd '>>' uses
REM FILE_SHARE_READ only and fails with "file is being used by another process"
REM because the monitor holds the same file open for write.
if not "!SYS_PASSWORD!"=="" (
    if /i not "%DRY_RUN%"=="Y" (
        echo [INFO]  Restoring undo_retention to 900s
        (
            echo WHENEVER SQLERROR EXIT FAILURE
            echo SET HEADING OFF FEEDBACK OFF VERIFY OFF
            echo ALTER SYSTEM SET undo_retention = 900;
            echo SELECT 'undo_retention=' ^|^| value FROM v$parameter WHERE name = 'undo_retention';
            echo EXIT;
        ) > "%TEMP%\epf_undo_restore.sql"
        sqlplus -S "sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA" @"%TEMP%\epf_undo_restore.sql" > "%TEMP%\epf_undo_restore.out" 2>&1
        powershell -Command "& { $fs=[IO.FileStream]::new('%LOG_FILE%','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { Get-Content '%TEMP%\epf_undo_restore.out' | ForEach-Object { $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
        findstr /C:"undo_retention=900" "%TEMP%\epf_undo_restore.out" >nul 2>&1
        if !ERRORLEVEL! EQU 0 (
            echo [OK]    undo_retention restored to 900s
        ) else (
            echo [WARN]  FAILED to restore undo_retention to 900s. Current value may still be 60s.
            echo [WARN]  Manual fix ^(as SYS^): ALTER SYSTEM SET undo_retention = 900;
            echo [WARN]  Last sqlplus output:
            type "%TEMP%\epf_undo_restore.out"
        )
        del "%TEMP%\epf_undo_restore.sql" >nul 2>&1
        del "%TEMP%\epf_undo_restore.out" >nul 2>&1
    )
)

REM ============================================================================
REM Drop temporary FK indexes (created by --optimize-db)
REM ============================================================================
if /i "%OPTIMIZE_DB%"=="Y" (
    if /i not "%DRY_RUN%"=="Y" (
        call :log "[INFO]  Dropping temporary FK indexes..."
        (
            echo SET SERVEROUTPUT ON SIZE UNLIMITED
            echo @"%SQL_DIR%\06c_drop_purge_indexes.sql"
            echo EXIT;
        ) > "%TEMP%\epf_drop_idx.sql"
        powershell -Command "& { $fs=[IO.FileStream]::new('%LOG_FILE%','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S '%USERNAME%/%PASSWORD%@%TNS_NAME%' '@%TEMP%\epf_drop_idx.sql' 2>&1 | ForEach-Object { $_; $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
        del "%TEMP%\epf_drop_idx.sql" >nul 2>&1
        call :log "[OK]    Temporary FK indexes dropped"
    )
)

REM ============================================================================
REM Space reclaim if requested (SHRINK + squeeze + resize)
REM ============================================================================
REM Drain delays (Start-Sleep 15s) bracket the reclaim block. The monitor polls
REM every 10s, so without these delays:
REM   - leftover BANK_STATEMENTS batch lines and "** PURGE COMPLETED **"
REM     interleave with the reclaim header
REM   - "** RECLAIM COMPLETED **" may never surface because the wrapper
REM     terminates the monitor before its next poll
if /i "%RECLAIM_SPACE%"=="Y" (
    if /i "%DRY_RUN%"=="Y" (
        echo [INFO]  Skipping space reclaim ^(dry run^)
    ) else (
        if "!SYS_PASSWORD!"=="" (
            echo   Space reclaim requires DBA/SYS credentials.
            for /f "usebackq delims=" %%P in (`powershell -Command "$p = Read-Host '  SYS password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))"`) do set "SYS_PASSWORD=%%P"
        )
        powershell -Command "Start-Sleep -Seconds 15"
        echo.
        echo   ============================================================
        echo   Online Tablespace Reclaim ^(SHRINK + squeeze + resize^)
        echo   ============================================================
        echo DEFINE skip_stall_checks = !SKIP_STALL_CHECKS!> "%TEMP%\epf_reclaim_online.sql"
        echo @"%SQL_DIR%\05_reclaim_tablespace.sql">> "%TEMP%\epf_reclaim_online.sql"
        echo EXIT;>> "%TEMP%\epf_reclaim_online.sql"
        powershell -Command "& { $fs=[IO.FileStream]::new('%LOG_FILE%','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S 'sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA' '@%TEMP%\epf_reclaim_online.sql' 2>&1 | ForEach-Object { $_; $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
        del "%TEMP%\epf_reclaim_online.sql" >nul 2>&1
        powershell -Command "Start-Sleep -Seconds 15"
        call :log "[OK]    Online reclaim completed"
    )
)

REM ============================================================================
REM Post-reclaim: Capture AFTER space snapshot and print comparison
REM ============================================================================
REM Space comparison is done here (after reclaim) instead of inside run_purge
REM because DELETE alone does not change segment sizes - only SHRINK/MOVE does.
if /i not "%DRY_RUN%"=="Y" (
    echo [INFO]  Capturing post-reclaim space snapshot and comparison...
    powershell -Command "& { $fs=[IO.FileStream]::new('%LOG_FILE%','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S '%USERNAME%/%PASSWORD%@%TNS_NAME%' '@%SQL_DIR%\09_space_compare.sql' 2>&1 | ForEach-Object { $_; $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
)

REM ============================================================================
REM Stop monitor (safe to run even if monitor never started)
REM ============================================================================
REM We do NOT auto-kill the separate console window. The launcher .bat ends in
REM `pause`, so the window stays open after the monitor exits naturally and the
REM operator can read the final RECLAIM_END / RUN_END lines. Closing the window
REM is a manual action. If the wrapper finishes before the monitor exits (rare),
REM the operator simply sees the still-running monitor and can close it.
del "%TEMP%\epf_monitor_launcher.bat" >nul 2>&1
if not "!MONITOR_PID!"=="" (
    echo [INFO]  Live monitor window left open for review. Close it manually when done.
)
:monitor_stopped

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
    call :log "[OK]    Package dropped"
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
    call :log "[OK]    Purge log tables dropped"
)

REM ============================================================================
REM Done
REM ============================================================================
call :log "."
call :log "[OK]    EPF Data Purge completed. Log: %LOG_FILE%"
echo Finished: %DATE% %TIME% >> "%LOG_FILE%"

endlocal
exit /b 0

REM ============================================================================
REM Log helper: writes message to both console and log file
REM ============================================================================
:log
echo(%~1
(>>"%LOG_FILE%" echo(%~1) 2>nul
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
echo   --optimize-db     Run DB optimization before purge (enlarge redo logs, gather stats)
echo                     Needs DBA/SYS creds. ~4 GB temp disk space. Idempotent.
echo   --reclaim         After purge, run online space reclaim (SHRINK + squeeze + resize)
echo                     No downtime required. Needs DBA/SYS creds.
echo   --reclaim-only    Skip purge entirely, run online reclaim only
echo   --no-stall-check  Disable stall detection during reclaim (always run all iterations)
echo   --drop-pkg        Drop the PL/SQL package after execution
echo   --drop-logs       Drop purge log tables (epf_purge_log, epf_purge_space_snapshot)
echo   --truncate-logs   Clear all purge run history before starting (keeps tables)
echo   --show-sizes      Show data sizes per module to help choose purge depth
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
