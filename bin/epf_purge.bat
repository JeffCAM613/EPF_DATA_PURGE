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
set "MONITOR_LOG_FILE=%LOG_DIR%\epf_purge_%TIMESTAMP%_monitor.log"

set "TNS_NAME="
set "USERNAME=oppayments"
set "PASSWORD="
set "RETENTION_DAYS=30"
set "PURGE_DEPTH=ALL"
set "PURGE_MODE=FULL"
set "BATCH_SIZE=1000"
set "DRY_RUN=N"
set "RECLAIM_SPACE=N"
set "RECLAIM_ONLY=N"
set "SKIP_STALL_CHECKS=N"
set "ALLOW_OFFLINE_IDX=N"
set "OPTIMIZE_DB=N"
set "SYS_PASSWORD="
set "ASSUME_YES=N"
set "DROP_PACKAGE_AFTER=N"
set "DROP_LOGS=N"
set "TRUNCATE_LOGS=N"
set "SHOW_SIZES=N"
set "MAX_ITERATIONS="
set "CONFIG_FILE="

REM Auto-computed module sizes (populated by capture_module_sizes once DB is reachable)
set "EPF_PAY_GB="
set "EPF_LOG_GB="
set "EPF_BST_GB="
set "EPF_TOTAL_GB="
set "EPF_DATAFILE_GB="
set "EPF_RECOMMENDED_MAX_ITER=2000"

REM Ensure log directory exists
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo EPF Data Purge Tool > "%LOG_FILE%"
echo Started: %DATE% %TIME% >> "%LOG_FILE%"

REM ============================================================================
REM Parse arguments
REM ============================================================================
REM Per-param "_SUPPLIED=Y" markers let the interactive_prompts block below
REM skip ANY prompt whose value was already provided on the command line.
REM Net effect: supplying a flag means you never see its prompt, regardless
REM of which OTHER prompts are still triggered (e.g. you can pass
REM --retention/--depth/--batch-size and only get prompted for password if
REM no --password is given). When TNS+PASSWORD are both supplied the entire
REM prompt block is skipped (INTERACTIVE=N) -- this is the existing
REM "single-command unattended run" path.
set "RETENTION_SUPPLIED=N"
set "DEPTH_SUPPLIED=N"
set "BATCH_SUPPLIED=N"
set "DRY_RUN_SUPPLIED=N"
set "DROP_PKG_SUPPLIED=N"
set "TRUNCATE_LOGS_SUPPLIED=N"
set "DROP_LOGS_SUPPLIED=N"
set "OPTIMIZE_DB_SUPPLIED=N"
set "RECLAIM_SUPPLIED=N"
:parse_args
if "%~1"=="" goto :args_done
if /i "%~1"=="--config"     ( set "CONFIG_FILE=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--tns"        ( set "TNS_NAME=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--user"       ( set "USERNAME=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--password"   ( set "PASSWORD=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--retention"  ( set "RETENTION_DAYS=%~2" & set "RETENTION_SUPPLIED=Y" & shift & shift & goto :parse_args )
if /i "%~1"=="--depth"      ( set "PURGE_DEPTH=%~2" & set "DEPTH_SUPPLIED=Y" & shift & shift & goto :parse_args )
if /i "%~1"=="--mode"       ( set "PURGE_MODE=%~2" & set "MODE_SUPPLIED=Y" & shift & shift & goto :parse_args )
if /i "%~1"=="--batch-size" ( set "BATCH_SIZE=%~2" & set "BATCH_SUPPLIED=Y" & shift & shift & goto :parse_args )
if /i "%~1"=="--dry-run"    ( set "DRY_RUN=Y" & set "DRY_RUN_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--no-dry-run" ( set "DRY_RUN=N" & set "DRY_RUN_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--reclaim"      ( set "RECLAIM_SPACE=Y" & set "RECLAIM_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--no-reclaim"   ( set "RECLAIM_SPACE=N" & set "RECLAIM_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--reclaim-only" ( set "RECLAIM_ONLY=Y" & set "RECLAIM_SPACE=Y" & set "RECLAIM_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--reclaim-online"      ( set "RECLAIM_SPACE=Y" & set "RECLAIM_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--reclaim-online-only" ( set "RECLAIM_ONLY=Y" & set "RECLAIM_SPACE=Y" & set "RECLAIM_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--no-stall-check" ( set "SKIP_STALL_CHECKS=Y" & shift & goto :parse_args )
if /i "%~1"=="--allow-offline-index-rebuild" ( set "ALLOW_OFFLINE_IDX=Y" & shift & goto :parse_args )
if /i "%~1"=="--optimize-db" ( set "OPTIMIZE_DB=Y" & set "OPTIMIZE_DB_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--no-optimize-db" ( set "OPTIMIZE_DB=N" & set "OPTIMIZE_DB_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--sys-password"  ( set "SYS_PASSWORD=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--assume-yes"    ( set "ASSUME_YES=Y" & shift & goto :parse_args )
if /i "%~1"=="-y"              ( set "ASSUME_YES=Y" & shift & goto :parse_args )
if /i "%~1"=="--drop-pkg"   ( set "DROP_PACKAGE_AFTER=Y" & set "DROP_PKG_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--no-drop-pkg" ( set "DROP_PACKAGE_AFTER=N" & set "DROP_PKG_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--drop-logs"  ( set "DROP_LOGS=Y" & set "DROP_LOGS_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--truncate-logs" ( set "TRUNCATE_LOGS=Y" & set "TRUNCATE_LOGS_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--no-truncate-logs" ( set "TRUNCATE_LOGS=N" & set "TRUNCATE_LOGS_SUPPLIED=Y" & shift & goto :parse_args )
if /i "%~1"=="--show-sizes" ( set "SHOW_SIZES=Y" & shift & goto :parse_args )
if /i "%~1"=="--max-iterations" ( set "MAX_ITERATIONS=%~2" & shift & shift & goto :parse_args )
if /i "%~1"=="--help"       ( goto :show_help )
if /i "%~1"=="-h"           ( goto :show_help )
echo [ERROR] Unknown argument: %~1
goto :show_help
:args_done

REM ============================================================================
REM Load config file if specified
REM ============================================================================
REM Config file values are applied as DEFAULTS -- CLI arguments take precedence.
REM We load the config into EPF_CFG_* temp vars, then only apply values that
REM were not already set via command-line flags.
if not "%CONFIG_FILE%"=="" (
    if not exist "%CONFIG_FILE%" (
        echo [ERROR] Config file not found: %CONFIG_FILE%
        exit /b 1
    )
    echo [INFO]  Loading configuration from: %CONFIG_FILE%
    for /f "usebackq eol=# tokens=1,* delims==" %%A in ("%CONFIG_FILE%") do (
        set "EPF_CFG_%%A=%%B"
    )
    REM Apply config values only when CLI did not supply them
    if "%TNS_NAME%"==""                    if defined EPF_CFG_TNS_NAME        set "TNS_NAME=!EPF_CFG_TNS_NAME!"
    if "%USERNAME%"=="oppayments"          if defined EPF_CFG_USERNAME         set "USERNAME=!EPF_CFG_USERNAME!"
    if "%PASSWORD%"==""                    if defined EPF_CFG_PASSWORD         set "PASSWORD=!EPF_CFG_PASSWORD!"
    if "%SYS_PASSWORD%"==""                if defined EPF_CFG_SYS_PASSWORD     set "SYS_PASSWORD=!EPF_CFG_SYS_PASSWORD!"
    if /i "%RETENTION_SUPPLIED%"=="N"      if defined EPF_CFG_RETENTION_DAYS   set "RETENTION_DAYS=!EPF_CFG_RETENTION_DAYS!"
    if /i "%DEPTH_SUPPLIED%"=="N"          if defined EPF_CFG_PURGE_DEPTH      set "PURGE_DEPTH=!EPF_CFG_PURGE_DEPTH!"
    if /i "%BATCH_SUPPLIED%"=="N"          if defined EPF_CFG_BATCH_SIZE       set "BATCH_SIZE=!EPF_CFG_BATCH_SIZE!"
    if /i "%DRY_RUN_SUPPLIED%"=="N"        if defined EPF_CFG_DRY_RUN          set "DRY_RUN=!EPF_CFG_DRY_RUN!"
    if /i "%RECLAIM_SUPPLIED%"=="N"        if defined EPF_CFG_RECLAIM_SPACE    set "RECLAIM_SPACE=!EPF_CFG_RECLAIM_SPACE!"
    if /i "%DROP_PKG_SUPPLIED%"=="N"       if defined EPF_CFG_DROP_PACKAGE_AFTER set "DROP_PACKAGE_AFTER=!EPF_CFG_DROP_PACKAGE_AFTER!"
    if /i "%TRUNCATE_LOGS_SUPPLIED%"=="N"  if defined EPF_CFG_TRUNCATE_LOGS    set "TRUNCATE_LOGS=!EPF_CFG_TRUNCATE_LOGS!"
    if /i "%OPTIMIZE_DB_SUPPLIED%"=="N"    if defined EPF_CFG_OPTIMIZE_DB      set "OPTIMIZE_DB=!EPF_CFG_OPTIMIZE_DB!"
    if not defined MODE_SUPPLIED            if defined EPF_CFG_PURGE_MODE       set "PURGE_MODE=!EPF_CFG_PURGE_MODE!"
    if /i "%ASSUME_YES%"=="N"               if defined EPF_CFG_ASSUME_YES       set "ASSUME_YES=!EPF_CFG_ASSUME_YES!"
    if /i "%DROP_LOGS%"=="N"                if defined EPF_CFG_DROP_LOGS        set "DROP_LOGS=!EPF_CFG_DROP_LOGS!"
    REM Clean up temp vars
    for /f "delims==" %%V in ('set EPF_CFG_ 2^>nul') do set "%%V="
)

REM Environment variable overrides for passwords
if defined EPF_PURGE_PASSWORD set "PASSWORD=%EPF_PURGE_PASSWORD%"
REM Parallel env var for SYS / DBA password so unattended runs that need
REM --reclaim or --optimize-db don't have to type the password interactively.
if defined EPF_SYS_PASSWORD set "SYS_PASSWORD=%EPF_SYS_PASSWORD%"

REM Normalize PURGE_DEPTH early (handles --depth CLI / config file input)
call :normalize_depth

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
    REM OPPAYMENTS password enables: live monitor, space comparison, final
    REM summary from DB. Reclaim itself runs as SYS regardless; if the user
    REM skips, those extras are disabled and only the inline sqlplus stream
    REM + the log file remain visible.
    if "!PASSWORD!"=="" (
        echo.
        echo   OPPAYMENTS password ^(optional - enables live monitor, space
        echo   comparison and DB summary; press Enter to skip^)
        for /f "usebackq delims=" %%P in (`powershell -Command "$p = Read-Host '  OPPAYMENTS password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))"`) do set "PASSWORD=%%P"
    )

    REM ---- DBA grants (SYS is available) ----
    echo.
    echo [INFO]  Granting DBA view access to !USERNAME!...
    > "%TEMP%\epf_grants.sql" (
        echo SET HEADING OFF FEEDBACK OFF
        echo GRANT SELECT ON sys.dba_segments TO !USERNAME!;
        echo GRANT SELECT ON sys.dba_lobs TO !USERNAME!;
        echo GRANT SELECT ON sys.dba_data_files TO !USERNAME!;
        echo EXIT;
    )
    sqlplus -S "sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA" @"%TEMP%\epf_grants.sql" >> "%LOG_FILE%" 2>&1
    del "%TEMP%\epf_grants.sql" >nul 2>&1
    call :log "[OK]    DBA view grants applied"

    REM ---- Deploy PL/SQL package if OPPAYMENTS password available ----
    REM Needed for post-reclaim space comparison (capture_space_snapshot,
    REM print_space_comparison). Package deployment is idempotent.
    set "EPF_PKG_DEPLOYED=N"
    if not "!PASSWORD!"=="" (
        for %%F in (01_create_purge_log_table.sql 02_epf_purge_pkg_spec.sql 03_epf_purge_pkg_body.sql) do (
            (
                echo SET SERVEROUTPUT ON SIZE UNLIMITED
                echo SET ECHO OFF FEEDBACK ON
                echo @"%SQL_DIR%\%%F"
                echo EXIT;
            ) | sqlplus -S "!USERNAME!/!PASSWORD!@!TNS_NAME!" >> "%LOG_FILE%" 2>&1
        )
        set "EPF_PKG_DEPLOYED=Y"
    )

    REM ---- Configuration summary ----
    call :log "."
    call :log "  ============================================================"
    call :log "  Configuration Summary  ^(RECLAIM-ONLY^)"
    call :log "  ============================================================"
    call :log "  [Connection]"
    call :log "    TNS Name:       !TNS_NAME!"
    call :log "    Username:       !USERNAME!"
    call :log "  [Reclaim]"
    call :log "    Log File:       %LOG_FILE%"
    call :log "    Monitor Log:    %MONITOR_LOG_FILE%"
    call :log "  ============================================================"
    call :log "."

    call :reclaim_warning_banner
    if /i not "!RECLAIM_CONFIRMED!"=="Y" (
        echo [INFO]  Reclaim cancelled by user.
        exit /b 0
    )

    set "MONITOR_PID="
    set "MONITOR_SCRIPT=%SCRIPT_DIR%epf_monitor.ps1"
    call :start_monitor

    call :log "[INFO]  Skipping purge. Running online reclaim only."
    call :log "[INFO]  Reclaim progress is shown in the monitor window."

    REM ---- Execute reclaim ----
    REM Positional args: target_pct_free, max_iterations (ignored), skip_stall_checks (ignored), allow_offline_idx (ignored)
    > "%TEMP%\epf_reclaim_online.sql" echo @"%SQL_DIR%\05_reclaim_tablespace.sql" 10 !MAX_ITERATIONS! !SKIP_STALL_CHECKS! !ALLOW_OFFLINE_IDX!
    >> "%TEMP%\epf_reclaim_online.sql" echo EXIT;
    REM Full reclaim output goes to temp file (live detail is in monitor log).
    powershell -Command "& { sqlplus -S 'sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA' '@%TEMP%\epf_reclaim_online.sql' 2>&1 | Out-File -Encoding utf8 '%TEMP%\epf_reclaim_output.txt' }"
    del "%TEMP%\epf_reclaim_online.sql" >nul 2>&1
    REM Extract summary banner (RECLAIM COMPLETE/FAILED) and append to main log.
    powershell -Command "& { $lines = Get-Content '%TEMP%\epf_reclaim_output.txt' -ErrorAction SilentlyContinue; $fs=[IO.FileStream]::new('%LOG_FILE%','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { $inBanner=$false; $eqCount=0; foreach($l in $lines) { if($l -match '^\s*RECLAIM (COMPLETE|FAILED|ABORTED)') { $inBanner=$true; $eqCount=0; $eq='============================================================'; $w.WriteLine(''); $w.WriteLine($eq); Write-Host $eq; $w.WriteLine($l); Write-Host $l } elseif($inBanner) { $w.WriteLine($l); Write-Host $l; if($l -match '^====') { $eqCount++; if($eqCount -ge 2) { $inBanner=$false } } } } } finally { $w.Close(); $fs.Close() } }"
    del "%TEMP%\epf_reclaim_output.txt" >nul 2>&1

    REM ---- Drain delay so monitor picks up RECLAIM_END ----
    if not "!MONITOR_PID!"=="" powershell -Command "Start-Sleep -Seconds 15"

    call :log "[OK]    Online reclaim completed"

    REM ---- Post-reclaim: reclaim status check ----
    if not "!PASSWORD!"=="" (
        > "%TEMP%\epf_reclaim_probe.sql" echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 4000 TRIMSPOOL ON
        >> "%TEMP%\epf_reclaim_probe.sql" echo SELECT NVL^(status, 'MISSING'^) ^|^| '^|' ^|^| NVL^(REPLACE^(message, CHR^(10^), ' '^), ''^) FROM ^( SELECT status, message FROM oppayments.epf_purge_log WHERE operation = 'RECLAIM_END' ORDER BY log_timestamp DESC ^) WHERE ROWNUM = 1;
        >> "%TEMP%\epf_reclaim_probe.sql" echo EXIT;
        sqlplus -S "!USERNAME!/!PASSWORD!@!TNS_NAME!" @"%TEMP%\epf_reclaim_probe.sql" > "%TEMP%\epf_reclaim_probe.out" 2>nul
        del "%TEMP%\epf_reclaim_probe.sql" >nul 2>&1
        set "RECLAIM_END_LINE="
        for /f "usebackq delims=" %%L in ("%TEMP%\epf_reclaim_probe.out") do (
            if not "%%L"=="" set "RECLAIM_END_LINE=%%L"
        )
        del "%TEMP%\epf_reclaim_probe.out" >nul 2>&1
        for /f "tokens=1,* delims=|" %%A in ("!RECLAIM_END_LINE!") do (
            set "RECLAIM_END_STATUS=%%A"
            set "RECLAIM_END_MSG=%%B"
        )
        if "!RECLAIM_END_STATUS!"=="" set "RECLAIM_END_STATUS=MISSING"
        if /i "!RECLAIM_END_STATUS!"=="MISSING" (
            call :log "[WARN]  No RECLAIM_END row found. Reclaim may not have completed."
        ) else if /i "!RECLAIM_END_STATUS!"=="ERROR" (
            call :log "[WARN]  Reclaim ended with status=ERROR: !RECLAIM_END_MSG!"
        )
    )

    REM ---- Post-reclaim: space comparison (requires PL/SQL package) ----
    if /i "!EPF_PKG_DEPLOYED!"=="Y" (
        echo [INFO]  Capturing post-reclaim space snapshot and comparison...
        > "%TEMP%\epf_space_compare.sql" echo SET SERVEROUTPUT ON SIZE UNLIMITED
        >> "%TEMP%\epf_space_compare.sql" echo SET LINESIZE 200
        >> "%TEMP%\epf_space_compare.sql" echo SET HEADING OFF FEEDBACK OFF
        >> "%TEMP%\epf_space_compare.sql" echo DECLARE
        >> "%TEMP%\epf_space_compare.sql" echo     l_run_id RAW^(16^);
        >> "%TEMP%\epf_space_compare.sql" echo BEGIN
        >> "%TEMP%\epf_space_compare.sql" echo     SELECT run_id INTO l_run_id FROM ^(
        >> "%TEMP%\epf_space_compare.sql" echo         SELECT run_id FROM oppayments.epf_purge_log
        >> "%TEMP%\epf_space_compare.sql" echo         WHERE operation IN ^('RUN_END','RECLAIM_END'^)
        >> "%TEMP%\epf_space_compare.sql" echo         ORDER BY log_timestamp DESC
        >> "%TEMP%\epf_space_compare.sql" echo     ^) WHERE ROWNUM = 1;
        >> "%TEMP%\epf_space_compare.sql" echo     DELETE FROM oppayments.epf_purge_space_snapshot
        >> "%TEMP%\epf_space_compare.sql" echo     WHERE run_id = l_run_id AND snapshot_phase = 'AFTER';
        >> "%TEMP%\epf_space_compare.sql" echo     COMMIT;
        >> "%TEMP%\epf_space_compare.sql" echo     oppayments.epf_purge_pkg.capture_space_snapshot^(l_run_id, 'AFTER'^);
        >> "%TEMP%\epf_space_compare.sql" echo     oppayments.epf_purge_pkg.print_space_comparison^(l_run_id, 'ALL'^);
        >> "%TEMP%\epf_space_compare.sql" echo END;
        >> "%TEMP%\epf_space_compare.sql" echo /
        >> "%TEMP%\epf_space_compare.sql" echo EXIT;
        powershell -Command "& { $fs=[IO.FileStream]::new('!LOG_FILE!','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S '!USERNAME!/!PASSWORD!@!TNS_NAME!' '@%TEMP%\epf_space_compare.sql' 2>&1 | ForEach-Object { $_; $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
        del "%TEMP%\epf_space_compare.sql" >nul 2>&1
    )

    REM ---- Monitor window left open for review ----
    del "%TEMP%\epf_monitor_launcher.bat" >nul 2>&1
    if not "!MONITOR_PID!"=="" (
        echo [INFO]  Live monitor window left open for review. Close it manually when done.
    )

    REM ---- Final summary ----
    call :write_final_summary
    call :log "."
    call :log "[OK]    EPF Space Reclaim completed."
    call :log "        Main log:    %LOG_FILE%"
    call :log "        Monitor log: %MONITOR_LOG_FILE%"
    call :log "Finished: %DATE% %TIME%"
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

    if "!SYS_PASSWORD!"=="" (
        echo.
        echo   SYS/DBA Password ^(optional^)
        echo   Enables accurate tablespace sizing and is required later for
        echo   optimize-db and space reclaim. Press Enter to skip.
        for /f "usebackq delims=" %%P in (`powershell -Command "$p = Read-Host '  SYS password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))"`) do set "SYS_PASSWORD=%%P"
    )

    REM Apply DBA grants early so capture_module_sizes can query dba_data_files.
    REM Uses a temp file instead of pipe to avoid delayed-expansion loss in
    REM cmd.exe subprocesses spawned by the pipe operator.
    if not "!SYS_PASSWORD!"=="" (
        echo.
        echo [INFO]  Granting DBA view access to !USERNAME!...
        > "%TEMP%\epf_grants.sql" (
            echo SET HEADING OFF FEEDBACK OFF
            echo GRANT SELECT ON sys.dba_segments TO !USERNAME!;
            echo GRANT SELECT ON sys.dba_lobs TO !USERNAME!;
            echo GRANT SELECT ON sys.dba_data_files TO !USERNAME!;
            echo EXIT;
        )
        sqlplus -S "sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA" @"%TEMP%\epf_grants.sql" >> "%LOG_FILE%" 2>&1
        del "%TEMP%\epf_grants.sql" >nul 2>&1
        call :log "[OK]    DBA view grants applied"
    )

    echo.
    echo   Retention Period
    echo   Data older than this many days will be purged.
    echo   Current value: !RETENTION_DAYS! days
    if /i not "!RETENTION_SUPPLIED!"=="Y" (
        set /p "RETENTION_INPUT=  Retention days [!RETENTION_DAYS!]: "
        if not "!RETENTION_INPUT!"=="" set "RETENTION_DAYS=!RETENTION_INPUT!"
    ) else (
        echo   Using --retention !RETENTION_DAYS!
    )

    REM Capture module sizes for depth prompt + max-iter recommendation.
    echo.
    echo [INFO]  Querying current data sizes...
    call :capture_module_sizes
    call :compute_recommended_max_iter
    if defined EPF_TOTAL_GB (
        if "!EPF_DATAFILE_GB!"=="0.00" (
            call :log "[OK]    Schema usage: !EPF_TOTAL_GB! GB"
        ) else (
            set "EPF_MSG=[OK]    Schema usage: !EPF_TOTAL_GB! GB | datafiles: !EPF_DATAFILE_GB! GB"
            call :log_msg
        )
        set "EPF_MSG=[OK]    Purge coverage: !EPF_EST_ALL! GB est. of !EPF_COVERAGE_GB! GB | Outside coverage: !EPF_OTHER_GB! GB (!EPF_OTHER_PCT!%%)"
        call :log_msg
        call :log "[OK]    Breakdown: PAYMENTS=!EPF_PAY_GB! GB  LOGS=!EPF_LOG_GB! GB  BANK_STATEMENTS=!EPF_BST_GB! GB"
        set "EPF_MSG=[OK]    CLOBs: !EPF_CLOB_TOTAL_GB! GB total (!EPF_CLOB_PCT!%%) | PAY=!EPF_CLOB_PAY_GB! GB  LOGS=!EPF_CLOB_LOG_GB! GB  BST=!EPF_CLOB_BST_GB! GB"
        call :log_msg
        call :log "[OK]    Retention !RETENTION_DAYS! days -- estimated purge: ~!EPF_EST_ALL! GB"
    ) else (
        echo [WARN]  Could not query data sizes -- depth prompt will not show GB hints.
        if defined EPF_SIZE_ERR echo [WARN]    Reason: !EPF_SIZE_ERR!
    )

    echo.
    echo   Purge Mode
    echo   Controls what happens to matched rows:
    echo.
    echo     FULL         Delete entire rows older than retention ^(default^)
    echo.
    echo     CLOB_ONLY    Clear CLOB content only -- rows preserved, LOB space freed
    echo                  UPDATE SET col = EMPTY_CLOB^(^) on tables with LOBs:
    if exist "%TEMP%\epf_clob_tables.out" (
        for /f "delims=" %%L in ('type "%TEMP%\epf_clob_tables.out"') do (
            echo                    %%L
        )
        set "EPF_MSG=                  Total CLOB reclaimable: ~!EPF_CLOB_TOTAL_GB! GB"
        call :log_msg
    )
    echo.
    echo     CLOB_N_LOGS  CLOB_ONLY + full DELETE on LOGS module
    echo                  Clears CLOBs on above tables AND deletes log rows
    echo                  ^(audit_trail, audit_archive, op.spec_trt_log^)
    echo.
    if /i not "!MODE_SUPPLIED!"=="Y" (
        set /p "MODE_INPUT=  Purge mode [!PURGE_MODE!]: "
        if not "!MODE_INPUT!"=="" set "PURGE_MODE=!MODE_INPUT!"
    ) else (
        echo   Using --mode !PURGE_MODE!
    )
    call :normalize_mode

    REM Only show Purge Depth for FULL mode -- CLOB modes cover all LOB tables in selected depth
    if /i "!PURGE_MODE!"=="FULL" (
    echo.
    echo   Purge Depth
    echo   Controls which data modules are purged:
    if defined EPF_TOTAL_GB (
        echo.
        echo     ALL              [ret:!RETENTION_DAYS! ^| ~!EPF_EST_ALL! GB of !EPF_COVERAGE_GB! GB]
        echo                        Purge all modules ^(payments, logs, bank statements^)
        echo                        ^(27 tables: bulk_payment, payment, file_integration,
        echo                         bulk_payment_additional_info, bulk_signature,
        echo                         mandatory_signers, oidc_request_token, payment_audit,
        echo                         payment_additional_info, import_audit, import_audit_messages,
        echo                         transmission_execution, transmission_execution_audit,
        echo                         transmission_exception, notification_execution,
        echo                         approbation_execution, approbation_execution_opt,
        echo                         workflow_execution, workflow_execution_opt,
        echo                         bulkpayment_exception, invoice, invoice_additional_info,
        echo                         audit_trail, audit_archive, op.spec_trt_log,
        echo                         file_dispatching, directory_dispatching^)
        echo.
        echo     PAYMENTS         [ret:!RETENTION_DAYS! ^| ~!EPF_EST_PAY! GB of !EPF_PAY_GB! GB]
        echo                        Purge bulk payments and file integrations only
        echo                        ^(22 tables: bulk_payment, payment, file_integration,
        echo                         bulk_payment_additional_info, bulk_signature,
        echo                         mandatory_signers, oidc_request_token, payment_audit,
        echo                         payment_additional_info, import_audit, import_audit_messages,
        echo                         transmission_execution, transmission_execution_audit,
        echo                         transmission_exception, notification_execution,
        echo                         approbation_execution, approbation_execution_opt,
        echo                         workflow_execution, workflow_execution_opt,
        echo                         bulkpayment_exception, invoice, invoice_additional_info^)
        echo.
        echo     LOGS             [ret:!RETENTION_DAYS! ^| ~!EPF_EST_LOG! GB of !EPF_LOG_GB! GB]
        echo                        Purge audit trails and technical logs only
        echo                        ^(3 tables: audit_trail, audit_archive, op.spec_trt_log^)
        echo.
        echo     BANK_STATEMENTS  [ret:!RETENTION_DAYS! ^| ~!EPF_EST_BST! GB of !EPF_BST_GB! GB]
        echo                        Purge bank statement dispatching only
        echo                        ^(2 tables: file_dispatching, directory_dispatching^)
        echo.
        echo     * Purge estimate is ROUGH -- based on row ratio in root tables, actual may vary.
        echo     * ~!EPF_OTHER_GB! GB ^(!EPF_OTHER_PCT!%%^) of schema comes from tables outside purge coverage.
    ) else (
        echo     ALL             - Purge all modules ^(payments, logs, bank statements^)
        echo     PAYMENTS        - Purge bulk payments and file integrations only
        echo     LOGS            - Purge audit trails and technical logs only
        echo     BANK_STATEMENTS - Purge bank statement dispatching only
    )
    echo.
    echo     Combine modules with commas: PAYMENTS,LOGS  PAYMENTS,BANK_STATEMENTS
    echo     If ALL appears in the list it overrides everything else.
    if /i not "!DEPTH_SUPPLIED!"=="Y" (
        set /p "DEPTH_INPUT=  Purge depth [!PURGE_DEPTH!]: "
        if not "!DEPTH_INPUT!"=="" set "PURGE_DEPTH=!DEPTH_INPUT!"
    ) else (
        echo   Using --depth !PURGE_DEPTH!
    )
    call :normalize_depth
    )

    echo.
    echo   Batch Size
    echo   Number of parent records processed per commit. Larger = faster
    echo   but uses more undo/redo space. Recommended: 500-5000.
    if /i not "!BATCH_SUPPLIED!"=="Y" (
        set /p "BATCH_INPUT=  Batch size [!BATCH_SIZE!]: "
        if not "!BATCH_INPUT!"=="" set "BATCH_SIZE=!BATCH_INPUT!"
    ) else (
        echo   Using --batch-size !BATCH_SIZE!
    )

    echo.
    echo   Dry Run
    echo   If yes, the tool will count how many rows would be deleted
    echo   without actually deleting anything. Good for a first test.
    if /i not "!DRY_RUN_SUPPLIED!"=="Y" (
        set /p "DRY_INPUT=  Dry run? (Y/N) [!DRY_RUN!]: "
        if not "!DRY_INPUT!"=="" set "DRY_RUN=!DRY_INPUT!"
    ) else (
        echo   Using --dry-run=!DRY_RUN!
    )

    echo.
    echo   Drop Package After Execution
    echo   If yes, the PL/SQL package will be removed from the database
    echo   after the purge completes. The log table is preserved.
    if /i not "!DROP_PKG_SUPPLIED!"=="Y" (
        set /p "DROP_INPUT=  Drop package after? (Y/N) [!DROP_PACKAGE_AFTER!]: "
        if not "!DROP_INPUT!"=="" set "DROP_PACKAGE_AFTER=!DROP_INPUT!"
    ) else (
        echo   Using --drop-pkg=!DROP_PACKAGE_AFTER!
    )

    echo.
    echo   Truncate Purge Logs ^(--truncate-logs^)
    echo   Clears all previous purge run history from the log tables.
    echo   Useful when re-running after a failed or test purge.
    if /i not "!TRUNCATE_LOGS_SUPPLIED!"=="Y" (
        set /p "TRUNC_INPUT=  Truncate logs? (Y/N) [!TRUNCATE_LOGS!]: "
        if not "!TRUNC_INPUT!"=="" set "TRUNCATE_LOGS=!TRUNC_INPUT!"
    ) else (
        echo   Using --truncate-logs=!TRUNCATE_LOGS!
    )

    echo.
    echo   Pre-Purge Database Optimization ^(--optimize-db^)
    echo   Enlarges redo logs to 1 GB and gathers optimizer statistics.
    echo   Recommended for first-time purge on databases with small redo logs.
    echo   Requires SYS/DBA credentials. Idempotent and auto-reverts on failure.
    echo   ^>^> Extra disk space: ~4 GB temporary ^(new redo logs before old ones deleted^)
    if /i not "!OPTIMIZE_DB_SUPPLIED!"=="Y" (
        set /p "OPTDB_INPUT=  Optimize DB? (Y/N) [!OPTIMIZE_DB!]: "
        if not "!OPTDB_INPUT!"=="" set "OPTIMIZE_DB=!OPTDB_INPUT!"
    ) else (
        echo   Using --optimize-db=!OPTIMIZE_DB!
    )

    echo.
    echo   Post-Purge Space Reclaim ^(--reclaim^)
    echo   After purge, reclaims OS disk space by dropping all OPPAYMENTS+OP
    echo   indexes/constraints, compacting tables, resizing the data ^(and
    echo   index, if separate^) tablespace, then recreating everything.
    echo   Requires SYS/DBA credentials. Application MUST be quiesced
    echo   ^(no writes^) for the duration -- PK uniqueness is not enforced
    echo   between drop and recreate. Auto-detects tablespaces; works for
    echo   any tablespace name and for both shared and split data/index
    echo   layouts. You will see a confirmation banner before it runs.
    if /i not "!RECLAIM_SUPPLIED!"=="Y" (
        set /p "RECLAIM_INPUT=  Reclaim space? (Y/N) [!RECLAIM_SPACE!]: "
        if not "!RECLAIM_INPUT!"=="" set "RECLAIM_SPACE=!RECLAIM_INPUT!"
    ) else (
        echo   Using --reclaim=!RECLAIM_SPACE!
    )

    if /i "!RECLAIM_SPACE!"=="Y" (
        echo.
        echo   Reclaim mode: drop indexes / SHRINK / recreate.
        echo   You will be shown a confirmation banner with full details
        echo   right before the reclaim runs.
        echo   ^(legacy --max-iterations / --no-stall-check /
        echo    --allow-offline-index-rebuild flags are accepted but ignored^)
    )
)

REM Prompt for SYS password if optimize-db or reclaim enabled but not yet provided
set "EPF_NEED_SYS=N"
if /i "%OPTIMIZE_DB%"=="Y" if "!SYS_PASSWORD!"=="" set "EPF_NEED_SYS=Y"
if /i "%RECLAIM_SPACE%"=="Y" if "!SYS_PASSWORD!"=="" set "EPF_NEED_SYS=Y"
if "!EPF_NEED_SYS!"=="Y" (
    echo.
    echo   SYS/DBA password ^(required for optimize-db / reclaim^)
    for /f "usebackq delims=" %%P in (`powershell -Command "$p = Read-Host '  SYS password' -AsSecureString; [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($p))"`) do set "SYS_PASSWORD=%%P"
)

REM ============================================================================
REM Display configuration summary (also written to log file)
REM ============================================================================
REM Previously these used plain `echo` so they only hit the console; the log
REM file then had no record of the run's parameters, which made post-mortem
REM debugging painful. Use :log so the same lines land in both console and
REM log file. write_final_summary at end of run prints them again for a
REM self-contained record.
call :log "."
call :log "  ============================================================"
call :log "  Configuration Summary"
call :log "  ============================================================"
call :log "  [Connection]"
call :log "    TNS Name:       %TNS_NAME%"
call :log "    Username:       %USERNAME%"
call :log "  [Purge]"
call :log "    Retention:      %RETENTION_DAYS% days"
call :log "    Depth:          %PURGE_DEPTH%"
call :log "    Mode:           %PURGE_MODE%"
call :log "    Batch Size:     %BATCH_SIZE%"
call :log "    Dry Run:        %DRY_RUN%"
call :log "  [Maintenance]"
call :log "    Optimize DB:    %OPTIMIZE_DB%"
call :log "    Reclaim Space:  %RECLAIM_SPACE%"
call :log "    Drop Package:   %DROP_PACKAGE_AFTER%"
call :log "    Truncate Logs:  %TRUNCATE_LOGS%"
call :log "    Log File:       %LOG_FILE%"
call :log "    Monitor Log:    %MONITOR_LOG_FILE%"
call :log "  ============================================================"
call :log "."
call :log "  --- Pre-run Confirmation ---"
if defined EPF_EST_ALL (
    call :log "  Data retention:    %RETENTION_DAYS% days"
    REM Build scope description and estimate from (possibly multi) depth
    call :build_scope_summary
    set "EPF_MSG=  Purge scope:       !PURGE_DEPTH! (!EPF_SCOPE_GB! GB across !EPF_SCOPE_TABLES! tables)"
    call :log_msg
    if /i "!PURGE_MODE!"=="CLOB_ONLY" (
        set "EPF_MSG=  Purge mode:        CLOB_ONLY (clear CLOB content, keep rows)"
        call :log_msg
        set "EPF_MSG=  Estimated impact:  ~!EPF_CLOB_TOTAL_GB! GB CLOB data across !EPF_CLOB_TABLE_COUNT! tables"
        call :log_msg
    ) else if /i "!PURGE_MODE!"=="CLOB_N_LOGS" (
        set "EPF_MSG=  Purge mode:        CLOB_N_LOGS (clear CLOBs + full delete on LOGS)"
        call :log_msg
        set "EPF_MSG=  Estimated impact:  ~!EPF_CLOB_TOTAL_GB! GB CLOBs + ~!EPF_LOG_GB! GB LOGS rows"
        call :log_msg
    ) else (
        set "EPF_MSG=  Purge mode:        FULL (delete entire rows)"
        call :log_msg
        set "EPF_MSG=  Estimated purge:   ~!EPF_SCOPE_EST! GB (rough estimate based on row ratios)"
        call :log_msg
    )
)
call :log "  Batch size:        %BATCH_SIZE% rows per commit"
if /i "%DRY_RUN%"=="Y" (
    set "EPF_MSG=  Dry run:            YES (count only, no data will be changed)"
    call :log_msg
) else (
    set "EPF_MSG=  Dry run:            NO (live execution)"
    call :log_msg
)
call :log "."
set "EPF_MSG=  Disk overhead:     ~2-5 GB temporary UNDO growth (auto-recovered)"
call :log_msg
if /i "%OPTIMIZE_DB%"=="Y" (
    set "EPF_MSG=  Optimize DB:       Yes (~4 GB temporary for redo logs)"
    call :log_msg
)
if /i not "%OPTIMIZE_DB%"=="Y" call :log "  Optimize DB:       No"
if /i "%RECLAIM_SPACE%"=="Y" (
    set "EPF_MSG=  Reclaim space:     Yes (iterative drain: peak overshoot = largest table + UNDO/redo)"
    call :log_msg
    call :log "."
    call :log "  [WARN]  RECLAIM will drop all indexes, PKs, UKs, and FKs for the duration"
    call :log "          of the operation. The application MUST be stopped/quiesced before"
    call :log "          proceeding. If you only need to purge now, run without --reclaim"
    call :log "          and use --reclaim-only later during a maintenance window."
) else (
    call :log "  Reclaim space:     No"
)
call :log "."

REM ============================================================================
REM Pre-run confirmation prompt
REM ============================================================================
if /i not "%ASSUME_YES%"=="Y" (
    echo.
    set /p "CONFIRM_INPUT=  Proceed? (Y/N) [Y]: "
    if /i "!CONFIRM_INPUT!"=="N" (
        call :log "[INFO]  Aborted by user."
        exit /b 0
    )
)

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

REM Auto-capture sizes for non-interactive runs (interactive_prompts already
REM did this). Used for the max-iter recommendation in the reclaim block and
REM matches the data the operator would have seen interactively.
if /i not "%INTERACTIVE%"=="Y" (
    if not defined EPF_TOTAL_GB (
        call :capture_module_sizes
        call :compute_recommended_max_iter
        if defined EPF_TOTAL_GB (
            call :log "[OK]    Breakdown: PAYMENTS=!EPF_PAY_GB! GB  LOGS=!EPF_LOG_GB! GB  BANK_STATEMENTS=!EPF_BST_GB! GB"
            call :log "[OK]    Retention !RETENTION_DAYS! days -- estimated purge: ~!EPF_EST_ALL! GB"
            if /i "!RECLAIM_SPACE!"=="Y" if "!MAX_ITERATIONS!"=="" (
                call :log "[INFO]  Recommended max_iterations for !EPF_DATAFILE_GB!GB tablespace: !EPF_RECOMMENDED_MAX_ITER!"
            )
        )
    )
)

REM --show-sizes is deprecated -- sizes are always captured + shown above
if /i "%SHOW_SIZES%"=="Y" (
    call :log "[WARN]  --show-sizes is deprecated; sizes are now always shown automatically. Flag accepted but does nothing."
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
echo [INFO]  Deploying PL/SQL package...
set "EPF_DEPLOY_ERRORS=0"
for %%F in (01_create_purge_log_table.sql 02_epf_purge_pkg_spec.sql 03_epf_purge_pkg_body.sql) do (
    (
        echo SET SERVEROUTPUT ON SIZE UNLIMITED
        echo SET ECHO OFF FEEDBACK ON
        echo @"%SQL_DIR%\%%F"
        echo EXIT;
    ) | sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" >> "%LOG_FILE%" 2>&1
    if !ERRORLEVEL! neq 0 (
        set /a "EPF_DEPLOY_ERRORS+=1"
    )
)

REM Check compilation errors
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
if !EPF_DEPLOY_ERRORS! gtr 0 (
    call :log "[ERROR] Package deployment failed. Check log for details."
    exit /b 1
)
call :log "[OK]    Package deployed (log table, spec, body)"

REM ============================================================================
REM Grant DBA view access for space snapshots (needs SYS)
REM ============================================================================
REM The space comparison needs dba_segments to match reclaim report numbers.
REM Grants are idempotent and only run when SYS password is available.
if not "!SYS_PASSWORD!"=="" (
    call :log "[INFO]  Granting DBA view access to !USERNAME! for space snapshots..."
    > "%TEMP%\epf_grants.sql" (
        echo SET HEADING OFF FEEDBACK OFF
        echo GRANT SELECT ON sys.dba_segments TO !USERNAME!;
        echo GRANT SELECT ON sys.dba_lobs TO !USERNAME!;
        echo GRANT SELECT ON sys.dba_data_files TO !USERNAME!;
        echo EXIT;
    )
    sqlplus -S "sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA" @"%TEMP%\epf_grants.sql" >> "%LOG_FILE%" 2>&1
    del "%TEMP%\epf_grants.sql" >nul 2>&1
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
    ) | sqlplus -S "!USERNAME!/!PASSWORD!@!TNS_NAME!" >nul 2>&1
    call :log "[OK]    Purge logs truncated"
)

REM ============================================================================
REM Execute purge
REM ============================================================================
call :log "[INFO]  Executing purge (monitor window shows live progress)..."

set "DRY_RUN_BOOL=FALSE"
set "RECLAIM_BOOL=FALSE"
if /i "%DRY_RUN%"=="Y" set "DRY_RUN_BOOL=TRUE"
if /i "%RECLAIM_SPACE%"=="Y" set "RECLAIM_BOOL=TRUE"

REM ============================================================================
REM Create temporary FK indexes for purge performance (optional, with --optimize-db)
REM ============================================================================
if /i "%OPTIMIZE_DB%"=="Y" (
    if /i not "%DRY_RUN%"=="Y" (
        call :log "[INFO]  Creating temporary FK indexes for purge performance..."
        (
            echo SET SERVEROUTPUT ON SIZE UNLIMITED
            echo @"%SQL_DIR%\06b_create_purge_indexes.sql"
        ) | sqlplus -S "!USERNAME!/!PASSWORD!@!TNS_NAME!" > "%TEMP%\epf_fkidx_create.out" 2>&1
        type "%TEMP%\epf_fkidx_create.out" >> "%LOG_FILE%"
        REM Extract summary line to console
        for /f "usebackq delims=" %%L in (`findstr /B /C:"Summary:" "%TEMP%\epf_fkidx_create.out"`) do (
            call :log "[OK]    FK indexes: %%L"
        )
        if "!ERRORLEVEL!"=="1" call :log "[OK]    Temporary FK indexes created"
        del "%TEMP%\epf_fkidx_create.out" >nul 2>&1
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
        sqlplus -S "sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA" @"%SQL_DIR%\utility\08_undo_tune.sql" 2>&1
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
REM   - Main log file : wrapper output (config, sqlplus, summary, run log replay)
REM   - Monitor log   : live monitor output (separate file, no interleaving)
set "MONITOR_PID="
set "MONITOR_SCRIPT=%SCRIPT_DIR%epf_monitor.ps1"

call :start_monitor

echo SET SERVEROUTPUT ON SIZE UNLIMITED> "%TEMP%\epf_exec.sql"
echo SET LINESIZE 200>> "%TEMP%\epf_exec.sql"
echo SET TIMING ON>> "%TEMP%\epf_exec.sql"
echo SET ECHO OFF FEEDBACK OFF>> "%TEMP%\epf_exec.sql"
echo BEGIN>> "%TEMP%\epf_exec.sql"
echo     oppayments.epf_purge_pkg.run_purge(>> "%TEMP%\epf_exec.sql"
echo         p_retention_days =^> %RETENTION_DAYS%,>> "%TEMP%\epf_exec.sql"
echo         p_purge_depth    =^> '%PURGE_DEPTH%',>> "%TEMP%\epf_exec.sql"
echo         p_batch_size     =^> %BATCH_SIZE%,>> "%TEMP%\epf_exec.sql"
echo         p_dry_run        =^> %DRY_RUN_BOOL%,>> "%TEMP%\epf_exec.sql"
echo         p_purge_mode     =^> '%PURGE_MODE%'>> "%TEMP%\epf_exec.sql"
echo     ^);>> "%TEMP%\epf_exec.sql"
echo END;>> "%TEMP%\epf_exec.sql"
echo />> "%TEMP%\epf_exec.sql"
echo EXIT;>> "%TEMP%\epf_exec.sql"

REM Purge output goes to log file only. Monitor window shows live progress.
REM Run summary is extracted to console below.
powershell -Command "& { $fs=[IO.FileStream]::new('%LOG_FILE%','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S '%USERNAME%/%PASSWORD%@%TNS_NAME%' '@%TEMP%\epf_exec.sql' 2>&1 | ForEach-Object { $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
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
        powershell -Command "& { $fs=[IO.FileStream]::new('!LOG_FILE!','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { Get-Content '!TEMP!\epf_undo_restore.out' | ForEach-Object { $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
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
REM Post-purge SHRINK SPACE (segment compaction)
REM ============================================================================
REM Makes purge results visible in the space comparison report. Runs
REM independently of reclaim -- even when reclaim is skipped, the comparison
REM should show meaningful segment-level change.
if /i not "%DRY_RUN%"=="Y" (
    call :log "[INFO]  Running SHRINK SPACE (in-place compaction, no index drop)..."

    > "%TEMP%\epf_shrink.sql" echo @"%SQL_DIR%\05a_shrink_tables.sql"
    >> "%TEMP%\epf_shrink.sql" echo EXIT;
    powershell -Command "& { $fs=[IO.FileStream]::new('!LOG_FILE!','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S '!USERNAME!/!PASSWORD!@!TNS_NAME!' '@%TEMP%\epf_shrink.sql' 2>&1 | ForEach-Object { $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
    del "%TEMP%\epf_shrink.sql" >nul 2>&1
    call :log "[OK]    SHRINK SPACE completed"
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
        powershell -Command "& { $fs=[IO.FileStream]::new('!LOG_FILE!','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S '!USERNAME!/!PASSWORD!@!TNS_NAME!' '@%TEMP%\epf_drop_idx.sql' 2>&1 | ForEach-Object { $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
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
        call :reclaim_warning_banner
        if /i not "!RECLAIM_CONFIRMED!"=="Y" (
            call :log "[INFO]  Reclaim cancelled by user. Purge already completed."
        ) else (
            powershell -Command "Start-Sleep -Seconds 15"
            echo.
            echo   ============================================================
            echo   Tablespace Reclaim ^(iterative drain + refill^)
            echo   ============================================================
            if "!MAX_ITERATIONS!"=="" set "MAX_ITERATIONS=!EPF_RECOMMENDED_MAX_ITER!"
            if "!MAX_ITERATIONS!"=="" set "MAX_ITERATIONS=2000"
            call :log "[INFO]  Running reclaim (monitor window shows live progress)..."
            REM Positional args: target_pct_free, max_iterations (ignored), skip_stall_checks (ignored), allow_offline_idx (ignored)
            > "%TEMP%\epf_reclaim_online.sql" echo @"%SQL_DIR%\05_reclaim_tablespace.sql" 10 !MAX_ITERATIONS! !SKIP_STALL_CHECKS! !ALLOW_OFFLINE_IDX!
            >> "%TEMP%\epf_reclaim_online.sql" echo EXIT;
            REM Full reclaim output goes to temp file (live detail is in monitor log).
            REM Summary banner is extracted to main log below.
            powershell -Command "& { sqlplus -S 'sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA' '@%TEMP%\epf_reclaim_online.sql' 2>&1 | Out-File -Encoding utf8 '%TEMP%\epf_reclaim_output.txt' }"
            del "%TEMP%\epf_reclaim_online.sql" >nul 2>&1
            REM Extract summary banner (RECLAIM COMPLETE/FAILED through end marker)
            REM and append to main log. Full output stays in temp for debugging.
            powershell -Command "& { $lines = Get-Content '%TEMP%\epf_reclaim_output.txt' -ErrorAction SilentlyContinue; $fs=[IO.FileStream]::new('!LOG_FILE!','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { $inBanner=$false; $eqCount=0; foreach($l in $lines) { if($l -match '^\s*RECLAIM (COMPLETE|FAILED|ABORTED)') { $inBanner=$true; $eqCount=0; $eq='============================================================'; $w.WriteLine(''); $w.WriteLine($eq); Write-Host $eq; $w.WriteLine($l); Write-Host $l } elseif($inBanner) { $w.WriteLine($l); Write-Host $l; if($l -match '^====') { $eqCount++; if($eqCount -ge 2) { $inBanner=$false } } } } } finally { $w.Close(); $fs.Close() } }"
            del "%TEMP%\epf_reclaim_output.txt" >nul 2>&1
            powershell -Command "Start-Sleep -Seconds 15"
            call :log "[OK]    Online reclaim completed"
        )
    )
)

REM ============================================================================
REM Post-purge/reclaim: Capture AFTER space snapshot and print comparison
REM ============================================================================
REM Space comparison is done here (after shrink/reclaim). DELETE alone does not
REM change segment sizes; SHRINK (always run post-purge) and/or the full reclaim
REM make the change visible.
if /i not "%DRY_RUN%"=="Y" (

    REM ----- Pre-comparison reclaim status check (post-fail warning) -----
    if /i "%RECLAIM_SPACE%"=="Y" (
        > "%TEMP%\epf_reclaim_probe.sql" echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 4000 TRIMSPOOL ON
        >> "%TEMP%\epf_reclaim_probe.sql" echo SELECT NVL^(status, 'MISSING'^) ^|^| '^|' ^|^| NVL^(REPLACE^(message, CHR^(10^), ' '^), ''^) FROM ^( SELECT status, message FROM oppayments.epf_purge_log WHERE operation = 'RECLAIM_END' ORDER BY log_timestamp DESC ^) WHERE ROWNUM = 1;
        >> "%TEMP%\epf_reclaim_probe.sql" echo EXIT;
        sqlplus -S "!USERNAME!/!PASSWORD!@!TNS_NAME!" @"%TEMP%\epf_reclaim_probe.sql" > "%TEMP%\epf_reclaim_probe.out" 2>nul
        del "%TEMP%\epf_reclaim_probe.sql" >nul 2>&1
        set "RECLAIM_END_LINE="
        for /f "usebackq delims=" %%L in ("%TEMP%\epf_reclaim_probe.out") do (
            if not "%%L"=="" set "RECLAIM_END_LINE=%%L"
        )
        del "%TEMP%\epf_reclaim_probe.out" >nul 2>&1
        for /f "tokens=1,* delims=|" %%A in ("!RECLAIM_END_LINE!") do (
            set "RECLAIM_END_STATUS=%%A"
            set "RECLAIM_END_MSG=%%B"
        )
        if "!RECLAIM_END_STATUS!"=="" set "RECLAIM_END_STATUS=MISSING"
        if /i "!RECLAIM_END_STATUS!"=="MISSING" (
            echo.
            call :log "[WARN]  ============================================================"
            call :log "[WARN]    Reclaim was requested but no RECLAIM_END row was found in"
            call :log "[WARN]    epf_purge_log. The reclaim may not have run, or it may"
            call :log "[WARN]    have been killed. The AFTER snapshot below may not"
            call :log "[WARN]    reflect the intended final state."
            call :log "[WARN]  ============================================================"
        ) else if /i "!RECLAIM_END_STATUS!"=="ERROR" (
            echo.
            call :log "[WARN]  ============================================================"
            call :log "[WARN]    Reclaim ended with status=ERROR. AFTER snapshot may not"
            call :log "[WARN]    reflect the intended final state."
            call :log "[WARN]    Reclaim message: !RECLAIM_END_MSG!"
            call :log "[WARN]  ============================================================"
        )
    )

    echo [INFO]  Capturing post-reclaim space snapshot and comparison...
    REM Bake the depth value directly into an inline anonymous PL/SQL block.
    REM Previously this section wrote DEFINE depth + @09_space_compare.sql,
    REM but 09_space_compare.sql contains its own DEFINE depth = ALL which
    REM silently overwrote the wrapper's DEFINE -- so --depth was ignored and
    REM the report always covered every module. The Linux wrapper avoids the
    REM bug by inlining the call; we mirror that approach here.
    > "%TEMP%\epf_space_compare.sql" echo SET SERVEROUTPUT ON SIZE UNLIMITED
    >> "%TEMP%\epf_space_compare.sql" echo SET LINESIZE 200
    >> "%TEMP%\epf_space_compare.sql" echo SET HEADING OFF FEEDBACK OFF
    >> "%TEMP%\epf_space_compare.sql" echo DECLARE
    >> "%TEMP%\epf_space_compare.sql" echo     l_run_id RAW^(16^);
    >> "%TEMP%\epf_space_compare.sql" echo BEGIN
    >> "%TEMP%\epf_space_compare.sql" echo     SELECT run_id INTO l_run_id FROM ^(
    >> "%TEMP%\epf_space_compare.sql" echo         SELECT run_id FROM oppayments.epf_purge_log
    >> "%TEMP%\epf_space_compare.sql" echo         WHERE operation = 'RUN_END'
    >> "%TEMP%\epf_space_compare.sql" echo         ORDER BY log_timestamp DESC
    >> "%TEMP%\epf_space_compare.sql" echo     ^) WHERE ROWNUM = 1;
    >> "%TEMP%\epf_space_compare.sql" echo     DELETE FROM oppayments.epf_purge_space_snapshot
    >> "%TEMP%\epf_space_compare.sql" echo     WHERE run_id = l_run_id AND snapshot_phase = 'AFTER';
    >> "%TEMP%\epf_space_compare.sql" echo     COMMIT;
    >> "%TEMP%\epf_space_compare.sql" echo     oppayments.epf_purge_pkg.capture_space_snapshot^(l_run_id, 'AFTER'^);
    >> "%TEMP%\epf_space_compare.sql" echo     oppayments.epf_purge_pkg.print_space_comparison^(l_run_id, '%PURGE_DEPTH%'^);
    >> "%TEMP%\epf_space_compare.sql" echo END;
    >> "%TEMP%\epf_space_compare.sql" echo /
    >> "%TEMP%\epf_space_compare.sql" echo EXIT;
    powershell -Command "& { $fs=[IO.FileStream]::new('!LOG_FILE!','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S '!USERNAME!/!PASSWORD!@!TNS_NAME!' '@%TEMP%\epf_space_compare.sql' 2>&1 | ForEach-Object { $_; $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
    del "%TEMP%\epf_space_compare.sql" >nul 2>&1

    REM ----- Post-reclaim "recreate errors" warning banner -----
    REM The new drop-and-recreate path writes RECLAIM_END status='WARNING'
    REM when one or more indexes / constraints failed to recreate. The
    REM message contains a "[FAILED: ...]" list for the operator.
    if /i "%RECLAIM_SPACE%"=="Y" (
        > "%TEMP%\epf_squeeze_probe.sql" echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 4000 TRIMSPOOL ON
        >> "%TEMP%\epf_squeeze_probe.sql" echo SELECT message FROM ^( SELECT message FROM oppayments.epf_purge_log WHERE operation = 'RECLAIM_END' AND status = 'WARNING' ORDER BY log_timestamp DESC ^) WHERE ROWNUM = 1;
        >> "%TEMP%\epf_squeeze_probe.sql" echo EXIT;
        sqlplus -S "!USERNAME!/!PASSWORD!@!TNS_NAME!" @"%TEMP%\epf_squeeze_probe.sql" > "%TEMP%\epf_squeeze_probe.out" 2>nul
        del "%TEMP%\epf_squeeze_probe.sql" >nul 2>&1
        set "SQUEEZE_HIT="
        for /f "usebackq delims=" %%L in ("%TEMP%\epf_squeeze_probe.out") do (
            if not "%%L"=="" set "SQUEEZE_HIT=%%L"
        )
        del "%TEMP%\epf_squeeze_probe.out" >nul 2>&1
        if not "!SQUEEZE_HIT!"=="" (
            echo.
            call :log "[WARN]  ============================================================"
            call :log "[WARN]    RECLAIM COMPLETED WITH RECREATE ERRORS"
            call :log "[WARN]    !SQUEEZE_HIT!"
            call :log "[WARN]    --"
            call :log "[WARN]    One or more indexes / constraints failed to recreate."
            call :log "[WARN]    Inspect the [FAILED: ...] list in the message above and"
            set "EPF_MSG=[WARN]    recreate them manually, OR re-run the reclaim (it captures"
            call :log_msg
            set "EPF_MSG=[WARN]    fresh DDL each run and will retry the failed objects)."
            call :log_msg
            call :log "[WARN]  ============================================================"
        )
    )
)

REM ============================================================================
REM Shrink UNDO and TEMP tablespaces (runs in ALL modes: purge-only, reclaim,
REM both). When reclaim ran, its Step 14 already did this; we skip the
REM duplicate. For purge-only, this is the only place UNDO/TEMP get shrunk.
REM ============================================================================
if /i not "%RECLAIM_SPACE%"=="Y" (
    if not "!SYS_PASSWORD!"=="" (
        if /i not "%DRY_RUN%"=="Y" (
            echo [INFO]  Shrinking UNDO and TEMP tablespaces...
            (
                echo SET SERVEROUTPUT ON SIZE UNLIMITED
                echo SET FEEDBACK OFF
                echo DECLARE
                echo     v_undo_ts   VARCHAR2^(128^);
                echo     v_undo_file VARCHAR2^(513^);
                echo     v_undo_gb   NUMBER;
                echo     v_ok        BOOLEAN := FALSE;
                echo BEGIN
                echo     BEGIN
                echo         EXECUTE IMMEDIATE 'SELECT value FROM v$parameter WHERE name = ''undo_tablespace''' INTO v_undo_ts;
                echo         EXECUTE IMMEDIATE 'SELECT file_name, ROUND^(bytes/1024/1024/1024, 2^) FROM dba_data_files WHERE tablespace_name = :1 FETCH FIRST 1 ROW ONLY' INTO v_undo_file, v_undo_gb USING v_undo_ts;
                echo     EXCEPTION WHEN OTHERS THEN
                echo         DBMS_OUTPUT.PUT_LINE^('  Could not identify UNDO: ' ^|^| SQLERRM^);
                echo         GOTO skip_undo;
                echo     END;
                echo     DBMS_OUTPUT.PUT_LINE^('  UNDO: ' ^|^| v_undo_ts ^|^| ' ^(' ^|^| v_undo_gb ^|^| ' GB^)'^);
                echo     IF v_undo_gb ^< 2 THEN
                echo         DBMS_OUTPUT.PUT_LINE^('  UNDO already small. Skipping.'^);
                echo         GOTO skip_undo;
                echo     END IF;
                echo     FOR tg IN 1..4 LOOP
                echo         BEGIN
                echo             EXECUTE IMMEDIATE 'ALTER DATABASE DATAFILE ''' ^|^| v_undo_file ^|^| ''' RESIZE ' ^|^| tg ^|^| 'G';
                echo             DBMS_OUTPUT.PUT_LINE^('  UNDO resized to ' ^|^| tg ^|^| ' GB ^(freed ' ^|^| ROUND^(v_undo_gb - tg, 2^) ^|^| ' GB^)'^);
                echo             v_ok := TRUE;
                echo             EXIT;
                echo         EXCEPTION WHEN OTHERS THEN NULL;
                echo         END;
                echo     END LOOP;
                echo     IF NOT v_ok THEN
                echo         DBMS_OUTPUT.PUT_LINE^('  UNDO resize failed ^(active undo prevents shrink^). Skipping.'^);
                echo     END IF;
                echo     ^<^<skip_undo^>^> NULL;
                echo END;
                echo /
                echo BEGIN
                echo     FOR f IN ^(SELECT file_name, bytes, ROUND^(bytes/1024/1024/1024, 2^) AS size_gb FROM dba_temp_files WHERE bytes ^> 1073741824 ORDER BY file_name^) LOOP
                echo         BEGIN
                echo             EXECUTE IMMEDIATE 'ALTER DATABASE TEMPFILE ''' ^|^| f.file_name ^|^| ''' RESIZE 1G';
                echo             DBMS_OUTPUT.PUT_LINE^('  TEMP ' ^|^| f.file_name ^|^| ': ' ^|^| f.size_gb ^|^| ' GB -^> 1 GB'^);
                echo         EXCEPTION WHEN OTHERS THEN
                echo             DBMS_OUTPUT.PUT_LINE^('  TEMP ' ^|^| f.file_name ^|^| ' could not shrink: ' ^|^| SQLERRM^);
                echo         END;
                echo     END LOOP;
                echo END;
                echo /
                echo EXIT;
            ) > "%TEMP%\epf_undo_temp_shrink.sql"
            powershell -Command "& { $fs=[IO.FileStream]::new('!LOG_FILE!','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S 'sys/!SYS_PASSWORD!@!TNS_NAME! AS SYSDBA' '@%TEMP%\epf_undo_temp_shrink.sql' 2>&1 | ForEach-Object { $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
            del "%TEMP%\epf_undo_temp_shrink.sql" >nul 2>&1
            call :log "[OK]    UNDO/TEMP shrink completed"
        )
    )
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
REM The monitor process now writes to its own MONITOR_LOG_FILE, so there is
REM no sharing-violation risk here. We still use the FileStream pattern for
REM consistency with the rest of the wrapper.
if /i "%DROP_PACKAGE_AFTER%"=="Y" (
    echo.
    echo [INFO]  Dropping PL/SQL package...
    > "%TEMP%\epf_droppkg.sql" echo @"%SQL_DIR%\04_drop_epf_purge_pkg.sql"
    >> "%TEMP%\epf_droppkg.sql" echo EXIT;
    powershell -Command "& { $fs=[IO.FileStream]::new('!LOG_FILE!','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S '!USERNAME!/!PASSWORD!@!TNS_NAME!' '@%TEMP%\epf_droppkg.sql' 2>&1 | ForEach-Object { $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
    del "%TEMP%\epf_droppkg.sql" >nul 2>&1
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
    powershell -Command "& { $fs=[IO.FileStream]::new('!LOG_FILE!','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S '!USERNAME!/!PASSWORD!@!TNS_NAME!' '@%TEMP%\epf_droplogs.sql' 2>&1 | ForEach-Object { $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
    del "%TEMP%\epf_droplogs.sql" >nul 2>&1
    call :log "[OK]    Purge log tables dropped"
)

REM ============================================================================
REM Final summary
REM ============================================================================
REM write_final_summary prints a self-contained, user-friendly recap at the
REM very bottom of the log file: configuration, space reclaimed, durations.
REM
REM Both run regardless of dry-run / reclaim flags so the log is uniform.
call :write_final_summary

REM ============================================================================
REM Done
REM ============================================================================
call :log "."
call :log "[OK]    EPF Data Purge completed."
call :log "        Main log:    %LOG_FILE%"
call :log "        Monitor log: %MONITOR_LOG_FILE%"
call :log "Finished: %DATE% %TIME%"

del "%TEMP%\epf_clob_tables.out" >nul 2>&1
endlocal
exit /b 0

REM ============================================================================
REM write_final_summary -- user-friendly end-of-run summary written to log
REM ============================================================================
REM Reads the same epf_purge_log the monitor reads, summarises the latest
REM run into a few human-friendly lines, and writes them via :log so they
REM appear at the bottom of the log file (and on the console).
:write_final_summary
call :log "."
call :log "================================================================================"
if /i "%RECLAIM_ONLY%"=="Y" (
    call :log "  EPF SPACE RECLAIM - FINAL RUN SUMMARY"
) else (
    call :log "  EPF DATA PURGE - FINAL RUN SUMMARY"
)
call :log "================================================================================"
call :log "  [Configuration]"
call :log "    TNS Name:       %TNS_NAME%"
call :log "    Username:       %USERNAME%"
if /i not "%RECLAIM_ONLY%"=="Y" (
    call :log "    Retention:      %RETENTION_DAYS% days"
    call :log "    Depth:          %PURGE_DEPTH%"
    call :log "    Mode:           %PURGE_MODE%"
    call :log "    Batch Size:     %BATCH_SIZE%"
    call :log "    Dry Run:        %DRY_RUN%"
    call :log "    Reclaim:        %RECLAIM_SPACE%"
) else (
    call :log "    Reclaim-Only:   Y"
)
call :log "  --------------------------------------------------------------------------------"
if "%TNS_NAME%"=="" goto :write_final_summary_done
if "%PASSWORD%"=="" goto :write_final_summary_done

REM ---- Single PL/SQL block: result + space + durations ----
REM The purge and reclaim may have DIFFERENT run_ids (when the reclaim script's
REM run_id attach hits EXCEPTION WHEN OTHERS and falls back to SYS_GUID).
REM We find each independently so both sections always appear.
> "%TEMP%\epf_final_summary.sql" echo SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 200 TRIMSPOOL ON SERVEROUTPUT ON SIZE UNLIMITED
>> "%TEMP%\epf_final_summary.sql" echo DECLARE
>> "%TEMP%\epf_final_summary.sql" echo     l_purge_run_id  RAW^(16^);
>> "%TEMP%\epf_final_summary.sql" echo     l_reclaim_run_id RAW^(16^);
>> "%TEMP%\epf_final_summary.sql" echo     l_total_rows    NUMBER := 0;
>> "%TEMP%\epf_final_summary.sql" echo     l_total_errors  NUMBER := 0;
>> "%TEMP%\epf_final_summary.sql" echo     l_purge_start   TIMESTAMP;
>> "%TEMP%\epf_final_summary.sql" echo     l_purge_end     TIMESTAMP;
>> "%TEMP%\epf_final_summary.sql" echo     l_purge_secs    NUMBER;
>> "%TEMP%\epf_final_summary.sql" echo     l_reclaim_secs  NUMBER;
>> "%TEMP%\epf_final_summary.sql" echo     l_total_secs    NUMBER;
>> "%TEMP%\epf_final_summary.sql" echo     l_has_reclaim   BOOLEAN := FALSE;
>> "%TEMP%\epf_final_summary.sql" echo     l_reclaim_start_msg VARCHAR2^(4000^);
>> "%TEMP%\epf_final_summary.sql" echo     l_reclaim_end_msg   VARCHAR2^(4000^);
>> "%TEMP%\epf_final_summary.sql" echo     l_df_before     NUMBER;
>> "%TEMP%\epf_final_summary.sql" echo     l_df_after      NUMBER;
>> "%TEMP%\epf_final_summary.sql" echo     l_hwm_before    NUMBER;
>> "%TEMP%\epf_final_summary.sql" echo     l_hwm_after     NUMBER;
>> "%TEMP%\epf_final_summary.sql" echo     l_used_before   NUMBER;
>> "%TEMP%\epf_final_summary.sql" echo     l_used_after    NUMBER;
>> "%TEMP%\epf_final_summary.sql" echo     l_df_pct        NUMBER;
>> "%TEMP%\epf_final_summary.sql" echo     l_used_pct      NUMBER;
>> "%TEMP%\epf_final_summary.sql" echo BEGIN
>> "%TEMP%\epf_final_summary.sql" echo     -- Find purge run_id (latest RUN_END)
>> "%TEMP%\epf_final_summary.sql" echo     BEGIN
>> "%TEMP%\epf_final_summary.sql" echo         SELECT run_id INTO l_purge_run_id FROM ^( SELECT run_id FROM oppayments.epf_purge_log WHERE operation = 'RUN_END' ORDER BY log_timestamp DESC ^) WHERE ROWNUM = 1;
>> "%TEMP%\epf_final_summary.sql" echo     EXCEPTION WHEN NO_DATA_FOUND THEN l_purge_run_id := NULL;
>> "%TEMP%\epf_final_summary.sql" echo     END;
>> "%TEMP%\epf_final_summary.sql" echo     -- Find reclaim run_id (latest RECLAIM_END; may equal purge_run_id or differ)
>> "%TEMP%\epf_final_summary.sql" echo     BEGIN
>> "%TEMP%\epf_final_summary.sql" echo         SELECT run_id INTO l_reclaim_run_id FROM ^( SELECT run_id FROM oppayments.epf_purge_log WHERE operation = 'RECLAIM_END' ORDER BY log_timestamp DESC ^) WHERE ROWNUM = 1;
>> "%TEMP%\epf_final_summary.sql" echo     EXCEPTION WHEN NO_DATA_FOUND THEN l_reclaim_run_id := NULL;
>> "%TEMP%\epf_final_summary.sql" echo     END;
>> "%TEMP%\epf_final_summary.sql" echo     IF l_purge_run_id IS NULL AND l_reclaim_run_id IS NULL THEN
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('  [Result]'^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('    No completed run found.'^);
>> "%TEMP%\epf_final_summary.sql" echo         RETURN;
>> "%TEMP%\epf_final_summary.sql" echo     END IF;
>> "%TEMP%\epf_final_summary.sql" echo     -- Purge timing
>> "%TEMP%\epf_final_summary.sql" echo     IF l_purge_run_id IS NOT NULL THEN
>> "%TEMP%\epf_final_summary.sql" echo         BEGIN
>> "%TEMP%\epf_final_summary.sql" echo             SELECT log_timestamp INTO l_purge_start FROM oppayments.epf_purge_log WHERE run_id = l_purge_run_id AND operation = 'RUN_START' AND ROWNUM = 1;
>> "%TEMP%\epf_final_summary.sql" echo             SELECT log_timestamp INTO l_purge_end FROM oppayments.epf_purge_log WHERE run_id = l_purge_run_id AND operation = 'RUN_END' AND ROWNUM = 1;
>> "%TEMP%\epf_final_summary.sql" echo             l_purge_secs := ROUND^(EXTRACT^(DAY FROM ^(l_purge_end - l_purge_start^)^)*86400 + EXTRACT^(HOUR FROM ^(l_purge_end - l_purge_start^)^)*3600 + EXTRACT^(MINUTE FROM ^(l_purge_end - l_purge_start^)^)*60 + EXTRACT^(SECOND FROM ^(l_purge_end - l_purge_start^)^), 0^);
>> "%TEMP%\epf_final_summary.sql" echo         EXCEPTION WHEN NO_DATA_FOUND THEN l_purge_secs := NULL;
>> "%TEMP%\epf_final_summary.sql" echo         END;
>> "%TEMP%\epf_final_summary.sql" echo         SELECT NVL^(SUM^(CASE WHEN status='SUCCESS' THEN rows_affected ELSE 0 END^), 0^), NVL^(SUM^(CASE WHEN status='ERROR' THEN 1 ELSE 0 END^), 0^) INTO l_total_rows, l_total_errors FROM oppayments.epf_purge_log WHERE run_id = l_purge_run_id AND operation = 'DELETE' AND module IN ^('PAYMENTS','AUDIT_LOGS','TECH_LOGS','BANK_STATEMENTS','FILE_INTEGRATION'^);
>> "%TEMP%\epf_final_summary.sql" echo     END IF;
>> "%TEMP%\epf_final_summary.sql" echo     -- Reclaim data (use reclaim_run_id; falls back to purge_run_id when they match)
>> "%TEMP%\epf_final_summary.sql" echo     BEGIN
>> "%TEMP%\epf_final_summary.sql" echo         SELECT message INTO l_reclaim_start_msg FROM oppayments.epf_purge_log WHERE run_id = NVL^(l_reclaim_run_id, l_purge_run_id^) AND operation = 'RECLAIM_START' AND ROWNUM = 1;
>> "%TEMP%\epf_final_summary.sql" echo         SELECT REPLACE^(message, CHR^(10^), ' '^), elapsed_seconds INTO l_reclaim_end_msg, l_reclaim_secs FROM ^(SELECT message, elapsed_seconds FROM oppayments.epf_purge_log WHERE run_id = NVL^(l_reclaim_run_id, l_purge_run_id^) AND operation = 'RECLAIM_END' ORDER BY log_timestamp DESC^) WHERE ROWNUM = 1;
>> "%TEMP%\epf_final_summary.sql" echo         l_reclaim_secs := ROUND^(l_reclaim_secs, 0^);
>> "%TEMP%\epf_final_summary.sql" echo         l_has_reclaim := TRUE;
>> "%TEMP%\epf_final_summary.sql" echo         l_hwm_before := TO_NUMBER^(REGEXP_SUBSTR^(l_reclaim_end_msg, 'HWM ^(\d+\.?\d*^)', 1, 1, NULL, 1^)^);
>> "%TEMP%\epf_final_summary.sql" echo         l_hwm_after  := TO_NUMBER^(REGEXP_SUBSTR^(l_reclaim_end_msg, 'HWM \d+\.?\d*-^>^(\d+\.?\d*^)', 1, 1, NULL, 1^)^);
>> "%TEMP%\epf_final_summary.sql" echo         l_df_before  := TO_NUMBER^(REGEXP_SUBSTR^(l_reclaim_end_msg, 'datafile ^(\d+\.?\d*^)', 1, 1, NULL, 1^)^);
>> "%TEMP%\epf_final_summary.sql" echo         l_df_after   := TO_NUMBER^(REGEXP_SUBSTR^(l_reclaim_end_msg, 'datafile \d+\.?\d*-^>^(\d+\.?\d*^)', 1, 1, NULL, 1^)^);
>> "%TEMP%\epf_final_summary.sql" echo         l_used_before := TO_NUMBER^(REGEXP_SUBSTR^(l_reclaim_start_msg, 'used=^(\d+\.?\d*^)', 1, 1, NULL, 1^)^);
>> "%TEMP%\epf_final_summary.sql" echo         BEGIN
>> "%TEMP%\epf_final_summary.sql" echo             SELECT ROUND^(SUM^(size_mb^) / 1024, 2^) INTO l_used_after FROM oppayments.epf_purge_space_snapshot WHERE run_id = NVL^(l_purge_run_id, l_reclaim_run_id^) AND snapshot_phase = 'AFTER';
>> "%TEMP%\epf_final_summary.sql" echo         EXCEPTION WHEN OTHERS THEN l_used_after := NULL;
>> "%TEMP%\epf_final_summary.sql" echo         END;
>> "%TEMP%\epf_final_summary.sql" echo     EXCEPTION WHEN NO_DATA_FOUND THEN l_has_reclaim := FALSE;
>> "%TEMP%\epf_final_summary.sql" echo     END;
>> "%TEMP%\epf_final_summary.sql" echo     -- Total duration
>> "%TEMP%\epf_final_summary.sql" echo     DECLARE l_final_ts TIMESTAMP;
>> "%TEMP%\epf_final_summary.sql" echo         l_reclaim_start_ts TIMESTAMP;
>> "%TEMP%\epf_final_summary.sql" echo     BEGIN
>> "%TEMP%\epf_final_summary.sql" echo         SELECT MAX^(log_timestamp^) INTO l_final_ts FROM oppayments.epf_purge_log WHERE run_id IN ^(NVL^(l_purge_run_id, l_reclaim_run_id^), NVL^(l_reclaim_run_id, l_purge_run_id^)^);
>> "%TEMP%\epf_final_summary.sql" echo         IF l_purge_start IS NOT NULL THEN
>> "%TEMP%\epf_final_summary.sql" echo             l_total_secs := ROUND^(EXTRACT^(DAY FROM ^(l_final_ts - l_purge_start^)^)*86400 + EXTRACT^(HOUR FROM ^(l_final_ts - l_purge_start^)^)*3600 + EXTRACT^(MINUTE FROM ^(l_final_ts - l_purge_start^)^)*60 + EXTRACT^(SECOND FROM ^(l_final_ts - l_purge_start^)^), 0^);
>> "%TEMP%\epf_final_summary.sql" echo         ELSIF l_has_reclaim THEN
>> "%TEMP%\epf_final_summary.sql" echo             BEGIN
>> "%TEMP%\epf_final_summary.sql" echo                 SELECT log_timestamp INTO l_reclaim_start_ts FROM oppayments.epf_purge_log WHERE run_id = NVL^(l_reclaim_run_id, l_purge_run_id^) AND operation = 'RECLAIM_START' AND ROWNUM = 1;
>> "%TEMP%\epf_final_summary.sql" echo                 l_total_secs := ROUND^(EXTRACT^(DAY FROM ^(l_final_ts - l_reclaim_start_ts^)^)*86400 + EXTRACT^(HOUR FROM ^(l_final_ts - l_reclaim_start_ts^)^)*3600 + EXTRACT^(MINUTE FROM ^(l_final_ts - l_reclaim_start_ts^)^)*60 + EXTRACT^(SECOND FROM ^(l_final_ts - l_reclaim_start_ts^)^), 0^);
>> "%TEMP%\epf_final_summary.sql" echo             EXCEPTION WHEN NO_DATA_FOUND THEN NULL;
>> "%TEMP%\epf_final_summary.sql" echo             END;
>> "%TEMP%\epf_final_summary.sql" echo         END IF;
>> "%TEMP%\epf_final_summary.sql" echo     END;
>> "%TEMP%\epf_final_summary.sql" echo     IF l_purge_secs IS NOT NULL THEN
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('  [Purge Result]'^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('    Rows Deleted:   ' ^|^| TRIM^(TO_CHAR^(l_total_rows, '999,999,999,999'^)^)^);
>> "%TEMP%\epf_final_summary.sql" echo         IF l_total_errors ^> 0 THEN
>> "%TEMP%\epf_final_summary.sql" echo             DBMS_OUTPUT.PUT_LINE^('    Errors:         ' ^|^| l_total_errors^);
>> "%TEMP%\epf_final_summary.sql" echo         END IF;
>> "%TEMP%\epf_final_summary.sql" echo     END IF;
>> "%TEMP%\epf_final_summary.sql" echo     IF l_has_reclaim THEN
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('  [Reclaim Result]'^);
>> "%TEMP%\epf_final_summary.sql" echo         IF l_df_before IS NOT NULL AND l_df_after IS NOT NULL THEN
>> "%TEMP%\epf_final_summary.sql" echo             l_df_pct := ROUND^(^(l_df_before - l_df_after^) / NULLIF^(l_df_before, 0^) * 100, 1^);
>> "%TEMP%\epf_final_summary.sql" echo             DBMS_OUTPUT.PUT_LINE^('    Datafile:       ' ^|^| l_df_before ^|^| ' GB -^> ' ^|^| l_df_after ^|^| ' GB ^(-' ^|^| l_df_pct ^|^| '%%^)'^);
>> "%TEMP%\epf_final_summary.sql" echo         END IF;
>> "%TEMP%\epf_final_summary.sql" echo         IF l_hwm_before IS NOT NULL AND l_hwm_after IS NOT NULL THEN
>> "%TEMP%\epf_final_summary.sql" echo             DBMS_OUTPUT.PUT_LINE^('    HWM:            ' ^|^| l_hwm_before ^|^| ' GB -^> ' ^|^| l_hwm_after ^|^| ' GB'^);
>> "%TEMP%\epf_final_summary.sql" echo         END IF;
>> "%TEMP%\epf_final_summary.sql" echo         IF l_used_before IS NOT NULL AND l_used_after IS NOT NULL THEN
>> "%TEMP%\epf_final_summary.sql" echo             l_used_pct := ROUND^(^(l_used_before - l_used_after^) / NULLIF^(l_used_before, 0^) * 100, 1^);
>> "%TEMP%\epf_final_summary.sql" echo             DBMS_OUTPUT.PUT_LINE^('    Tablespace:     ' ^|^| l_used_before ^|^| ' GB -^> ' ^|^| l_used_after ^|^| ' GB ^(-' ^|^| l_used_pct ^|^| '%%^)'^);
>> "%TEMP%\epf_final_summary.sql" echo         END IF;
>> "%TEMP%\epf_final_summary.sql" echo     END IF;
>> "%TEMP%\epf_final_summary.sql" echo     DBMS_OUTPUT.PUT_LINE^('  --------------------------------------------------------------------------------'^);
>> "%TEMP%\epf_final_summary.sql" echo     IF l_purge_secs IS NOT NULL AND l_has_reclaim THEN
>> "%TEMP%\epf_final_summary.sql" echo         IF l_total_secs IS NOT NULL THEN
>> "%TEMP%\epf_final_summary.sql" echo             DBMS_OUTPUT.PUT_LINE^('    Total Duration: ' ^|^| l_total_secs ^|^| 's'^);
>> "%TEMP%\epf_final_summary.sql" echo         END IF;
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('      Purge:        ' ^|^| l_purge_secs ^|^| 's'^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('      Reclaim:      ' ^|^| l_reclaim_secs ^|^| 's'^);
>> "%TEMP%\epf_final_summary.sql" echo     ELSIF l_purge_secs IS NOT NULL THEN
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('    Purge Duration: ' ^|^| l_purge_secs ^|^| 's'^);
>> "%TEMP%\epf_final_summary.sql" echo     ELSIF l_has_reclaim AND l_reclaim_secs IS NOT NULL THEN
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('    Reclaim Duration: ' ^|^| l_reclaim_secs ^|^| 's'^);
>> "%TEMP%\epf_final_summary.sql" echo     END IF;
>> "%TEMP%\epf_final_summary.sql" echo     IF l_has_reclaim AND l_hwm_after IS NOT NULL AND l_used_after IS NOT NULL AND l_hwm_after - l_used_after ^> 1 THEN
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('  ********************************************************************************'^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('  *** HWM NOT FULLY RECLAIMED ***'^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^(''^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('  Current HWM:  ' ^|^| l_hwm_after ^|^| ' GB   ^|   Actual Used:  ' ^|^| l_used_after ^|^| ' GB   ^|   Gap: ~' ^|^| ROUND^(l_hwm_after - l_used_after, 2^) ^|^| ' GB'^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^(''^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('  Some segments could not be relocated in this pass. Re-running reclaim'^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('  may move them and further reduce the datafile.'^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^(''^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('  To reclaim more space, run:'^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('    .\epf_purge.bat --reclaim-only --tns !TNS_NAME! -y'^);
>> "%TEMP%\epf_final_summary.sql" echo         DBMS_OUTPUT.PUT_LINE^('  ********************************************************************************'^);
>> "%TEMP%\epf_final_summary.sql" echo     END IF;
>> "%TEMP%\epf_final_summary.sql" echo END;
>> "%TEMP%\epf_final_summary.sql" echo /
>> "%TEMP%\epf_final_summary.sql" echo EXIT;
powershell -Command "& { $fs=[IO.FileStream]::new('!LOG_FILE!','Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; try { sqlplus -S '!USERNAME!/!PASSWORD!@!TNS_NAME!' '@%TEMP%\epf_final_summary.sql' 2>&1 | ForEach-Object { $_; $w.WriteLine($_) } } finally { $w.Close(); $fs.Close() } }"
del "%TEMP%\epf_final_summary.sql" >nul 2>&1

:write_final_summary_done
call :log "================================================================================"
exit /b 0

REM ============================================================================
REM Normalize PURGE_DEPTH: uppercase, strip spaces, ALL overrides all
REM ============================================================================
REM Accepts comma-separated values like "PAYMENTS,LOGS" or single values.
REM If "ALL" appears anywhere in the list, normalizes to just "ALL".
:normalize_depth
set "PURGE_DEPTH=!PURGE_DEPTH: =!"
for /f "usebackq delims=" %%U in (`powershell -NoProfile -Command "'!PURGE_DEPTH!'.ToUpper()"`) do set "PURGE_DEPTH=%%U"
REM If ALL is anywhere in the comma-separated list, collapse to ALL
echo ,!PURGE_DEPTH!, | findstr /I ",ALL," >nul 2>&1
if !ERRORLEVEL! equ 0 set "PURGE_DEPTH=ALL"
exit /b 0

REM ============================================================================
REM Normalize PURGE_MODE: uppercase, validate
REM ============================================================================
:normalize_mode
for /f "usebackq delims=" %%U in (`powershell -NoProfile -Command "'!PURGE_MODE!'.ToUpper()"`) do set "PURGE_MODE=%%U"
if /i not "!PURGE_MODE!"=="FULL" if /i not "!PURGE_MODE!"=="CLOB_ONLY" if /i not "!PURGE_MODE!"=="CLOB_N_LOGS" (
    echo [WARN]  Invalid purge mode '!PURGE_MODE!', defaulting to FULL
    set "PURGE_MODE=FULL"
)
exit /b 0

REM ============================================================================
REM Build scope summary from PURGE_DEPTH (supports comma-separated modules)
REM ============================================================================
REM Sets: EPF_SCOPE_GB, EPF_SCOPE_EST, EPF_SCOPE_TABLES
REM Requires: EPF_PAY_GB, EPF_LOG_GB, EPF_BST_GB, EPF_EST_PAY/LOG/BST/ALL,
REM           EPF_COVERAGE_GB to be populated by capture_module_sizes.
:build_scope_summary
set "EPF_SCOPE_GB=0"
set "EPF_SCOPE_EST=0"
set /a "EPF_SCOPE_TABLES=0"
if /i "!PURGE_DEPTH!"=="ALL" (
    set "EPF_SCOPE_GB=!EPF_COVERAGE_GB!"
    set "EPF_SCOPE_EST=!EPF_EST_ALL!"
    set "EPF_SCOPE_TABLES=27"
    exit /b 0
)
REM Build sums via PowerShell (cmd has no float arithmetic)
set "_S_GB=0"
set "_S_EST=0"
set /a "_S_TBL=0"
echo ,!PURGE_DEPTH!, | findstr /I ",PAYMENTS," >nul 2>&1
if !ERRORLEVEL! equ 0 (
    set "_S_GB=!_S_GB!+!EPF_PAY_GB!"
    set "_S_EST=!_S_EST!+!EPF_EST_PAY!"
    set /a "_S_TBL=!_S_TBL!+22"
)
echo ,!PURGE_DEPTH!, | findstr /I ",LOGS," >nul 2>&1
if !ERRORLEVEL! equ 0 (
    set "_S_GB=!_S_GB!+!EPF_LOG_GB!"
    set "_S_EST=!_S_EST!+!EPF_EST_LOG!"
    set /a "_S_TBL=!_S_TBL!+3"
)
echo ,!PURGE_DEPTH!, | findstr /I ",BANK_STATEMENTS," >nul 2>&1
if !ERRORLEVEL! equ 0 (
    set "_S_GB=!_S_GB!+!EPF_BST_GB!"
    set "_S_EST=!_S_EST!+!EPF_EST_BST!"
    set /a "_S_TBL=!_S_TBL!+2"
)
for /f "usebackq delims=" %%V in (`powershell -NoProfile -Command "[math]::Round((!_S_GB!), 2)"`) do set "EPF_SCOPE_GB=%%V"
for /f "usebackq delims=" %%V in (`powershell -NoProfile -Command "[math]::Round((!_S_EST!), 2)"`) do set "EPF_SCOPE_EST=%%V"
set "EPF_SCOPE_TABLES=!_S_TBL!"
exit /b 0

REM ============================================================================
REM Log helper: writes message to both console and log file
REM ============================================================================
REM File append uses PowerShell FileStream with FileShare.ReadWrite so it
REM coexists with the live monitor process (which holds the same file open
REM with the same share mode). Plain cmd `>>` opens with FILE_SHARE_READ
REM only, which is incompatible with the monitor's writer share -- when both
REM hit the file at the same instant, cmd raises a parser-level "process
REM cannot access the file" error that `2>nul` does not reliably suppress.
REM
REM Cost: each :log call spawns one powershell.exe (~100 ms). The wrapper
REM makes ~30 such calls in a normal run (~3 s total overhead). High-volume
REM sqlplus output streams already use the FileStream pattern inline, so
REM they do not pay this per-line cost.
REM
REM Retry loop covers transient sharing violations from indexers/AV.
:log
echo(%~1
set "EPF_LOG_LINE=%~1"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$msg=$env:EPF_LOG_LINE; $r=0; while($r -lt 5){ try { $fs=[IO.FileStream]::new($env:LOG_FILE,'Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; $w.WriteLine($msg); $w.Close(); $fs.Close(); break } catch { $r++; Start-Sleep -Milliseconds 100 } }" 2>nul
exit /b 0

REM ----------------------------------------------------------------------------
REM :log_msg — like :log but reads EPF_MSG instead of a parameter.
REM Use this when the message contains pipe (|) or parentheses that would be
REM mangled by 'call' double-parse inside () blocks.
REM   set "EPF_MSG=text with | and ( special chars )"
REM   call :log_msg
REM ----------------------------------------------------------------------------
:log_msg
echo(!EPF_MSG!
set "EPF_LOG_LINE=!EPF_MSG!"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$msg=$env:EPF_LOG_LINE; $r=0; while($r -lt 5){ try { $fs=[IO.FileStream]::new($env:LOG_FILE,'Append','Write','ReadWrite'); $w=[IO.StreamWriter]::new($fs,[Text.Encoding]::UTF8); $w.AutoFlush=$true; $w.WriteLine($msg); $w.Close(); $fs.Close(); break } catch { $r++; Start-Sleep -Milliseconds 100 } }" 2>nul
exit /b 0

REM ============================================================================
REM Capture module sizes (DB connectivity required)
REM ============================================================================
REM Populates EPF_PAY_GB / EPF_LOG_GB / EPF_BST_GB / EPF_TOTAL_GB / EPF_DATAFILE_GB
REM and EPF_EST_PAY / EPF_EST_LOG / EPF_EST_BST / EPF_EST_ALL / EPF_OTHER_GB / EPF_OTHER_PCT
REM by parsing the EPF_SIZES| line emitted by sql/12_capture_module_sizes.sql.
REM On failure, sets EPF_SIZE_ERR with a diagnostic hint (if available).
:capture_module_sizes
if "%TNS_NAME%"=="" exit /b 1
if "%PASSWORD%"=="" exit /b 1
set "EPF_PAY_GB="
set "EPF_LOG_GB="
set "EPF_BST_GB="
set "EPF_TOTAL_GB="
set "EPF_DATAFILE_GB="
set "EPF_EST_PAY="
set "EPF_EST_LOG="
set "EPF_EST_BST="
set "EPF_EST_ALL="
set "EPF_OTHER_GB="
set "EPF_OTHER_PCT="
set "EPF_COVERAGE_GB="
set "EPF_CLOB_PAY_GB="
set "EPF_CLOB_LOG_GB="
set "EPF_CLOB_BST_GB="
set "EPF_CLOB_TOTAL_GB="
set "EPF_CLOB_PCT="
set "EPF_CLOB_DIR_DISP="
set "EPF_CLOB_FILE_DISP="
set "EPF_CLOB_TX_AUD="
set "EPF_SIZE_ERR="
sqlplus -S "%USERNAME%/%PASSWORD%@%TNS_NAME%" @"%SQL_DIR%\12_capture_module_sizes.sql" "%RETENTION_DAYS%" > "%TEMP%\epf_sizes.out" 2>&1
for /f "tokens=1-18 delims=|" %%A in ('findstr /B "EPF_SIZES" "%TEMP%\epf_sizes.out" 2^>nul') do (
    set "EPF_PAY_GB=%%B"
    set "EPF_LOG_GB=%%C"
    set "EPF_BST_GB=%%D"
    set "EPF_TOTAL_GB=%%E"
    set "EPF_DATAFILE_GB=%%F"
    set "EPF_EST_PAY=%%G"
    set "EPF_EST_LOG=%%H"
    set "EPF_EST_BST=%%I"
    set "EPF_EST_ALL=%%J"
    set "EPF_OTHER_GB=%%K"
    set "EPF_OTHER_PCT=%%L"
    set "EPF_COVERAGE_GB=%%M"
    set "EPF_CLOB_PAY_GB=%%N"
    set "EPF_CLOB_LOG_GB=%%O"
    set "EPF_CLOB_BST_GB=%%P"
    set "EPF_CLOB_TOTAL_GB=%%Q"
    set "EPF_CLOB_PCT=%%R"
)
for /f "tokens=1-4 delims=|" %%A in ('findstr /B "EPF_CLOB_DETAIL" "%TEMP%\epf_sizes.out" 2^>nul') do (
    set "EPF_CLOB_DIR_DISP=%%B"
    set "EPF_CLOB_FILE_DISP=%%C"
    set "EPF_CLOB_TX_AUD=%%D"
)
REM Capture dynamic per-table CLOB lines into temp file for display loop
set "EPF_CLOB_TABLE_COUNT=0"
del "%TEMP%\epf_clob_tables.out" >nul 2>&1
for /f "tokens=2-4 delims=|" %%A in ('findstr /B "EPF_CLOB_TABLE|" "%TEMP%\epf_sizes.out" 2^>nul') do (
    set /a "EPF_CLOB_TABLE_COUNT+=1"
    echo %%A ^(%%B GB^) [%%C]>>"%TEMP%\epf_clob_tables.out"
)
if not defined EPF_TOTAL_GB (
    REM Capture diagnostic hint: EPF_ERROR line from SQL, or first ORA-/SP2- line
    for /f "tokens=1,* delims=|" %%A in ('findstr /B "EPF_ERROR" "%TEMP%\epf_sizes.out" 2^>nul') do (
        set "EPF_SIZE_ERR=%%B"
    )
    if not defined EPF_SIZE_ERR (
        for /f "delims=" %%L in ('findstr /I /R "ORA- SP2- ERROR" "%TEMP%\epf_sizes.out" 2^>nul') do (
            if not defined EPF_SIZE_ERR set "EPF_SIZE_ERR=%%L"
        )
    )
)
del "%TEMP%\epf_sizes.out" >nul 2>&1
REM epf_clob_tables.out is read later during the Purge Mode prompt; cleaned up at script exit
if not defined EPF_TOTAL_GB exit /b 1
exit /b 0

REM ============================================================================
REM Compute recommended max iterations from datafile size
REM ============================================================================
REM Heuristic: max(2000, 50 * datafile_gb), capped at 20000.
REM cmd has no float math, so we strip the decimal portion of EPF_DATAFILE_GB
REM (a slight under-estimate, harmless).
:compute_recommended_max_iter
set "EPF_RECOMMENDED_MAX_ITER=2000"
if not defined EPF_DATAFILE_GB exit /b 0
if "!EPF_DATAFILE_GB!"=="0" exit /b 0
if "!EPF_DATAFILE_GB!"=="0.00" exit /b 0
for /f "tokens=1 delims=." %%A in ("!EPF_DATAFILE_GB!") do set "DF_INT=%%A"
if "!DF_INT!"=="" exit /b 0
set /a "REC=50 * DF_INT" 2>nul
if !REC! LSS 2000 set "REC=2000"
if !REC! GTR 20000 set "REC=20000"
set "EPF_RECOMMENDED_MAX_ITER=!REC!"
exit /b 0

REM ============================================================================
REM Start live progress monitor (callable subroutine)
REM ============================================================================
REM Spawns the monitor in a separate console window. Sets MONITOR_PID in the
REM caller's scope (cmd :label calls share the parent environment) so
REM stop_monitor / cleanup can find it later. Requires USERNAME, PASSWORD,
REM TNS_NAME, LOG_FILE, MONITOR_SCRIPT to be set before the call.
:start_monitor
set "MONITOR_PID="
if not exist "!MONITOR_SCRIPT!" (
    echo [WARN]  Monitor script not found: !MONITOR_SCRIPT!
    echo [WARN]  Continuing without live monitor. Tail %LOG_FILE% for log.
    exit /b 0
)
if "!PASSWORD!"=="" (
    echo [WARN]  Live monitor needs OPPAYMENTS password to poll epf_purge_log.
    echo [WARN]  Continuing without live monitor. Tail %LOG_FILE% for log.
    exit /b 0
)
> "%TEMP%\epf_monitor_launcher.bat" echo @echo off
>> "%TEMP%\epf_monitor_launcher.bat" echo title EPF Live Monitor
>> "%TEMP%\epf_monitor_launcher.bat" echo echo EPF Live progress monitor.
>> "%TEMP%\epf_monitor_launcher.bat" echo echo Connection: %USERNAME%/******@%TNS_NAME%
>> "%TEMP%\epf_monitor_launcher.bat" echo echo Log file:   %MONITOR_LOG_FILE%
>> "%TEMP%\epf_monitor_launcher.bat" echo powershell -ExecutionPolicy Bypass -File "%MONITOR_SCRIPT%" -ConnStr "%USERNAME%/%PASSWORD%@%TNS_NAME%" -PollSec 10 -MaxWaitMin 360 -LogFile "%MONITOR_LOG_FILE%"
>> "%TEMP%\epf_monitor_launcher.bat" echo echo [Monitor exited. Press any key to close this window.]
>> "%TEMP%\epf_monitor_launcher.bat" echo pause ^>nul

echo [INFO]  Opening live progress monitor in a separate console window...
echo [INFO]  This window will keep showing summary lines only.
echo [INFO]  Monitor output written to: %MONITOR_LOG_FILE%

powershell -Command "& { try { $p = Start-Process cmd -ArgumentList '/c','%TEMP%\epf_monitor_launcher.bat' -PassThru -ErrorAction Stop; $p.Id | Out-File -FilePath '%TEMP%\epf_monitor_pid.txt' -Encoding ascii } catch { $_.Exception.Message | Out-File -FilePath '%TEMP%\epf_monitor_err.txt' -Encoding ascii } }" 2>nul

if exist "%TEMP%\epf_monitor_err.txt" (
    set /p MONITOR_ERR=<"%TEMP%\epf_monitor_err.txt"
    echo [WARN]  Failed to open monitor window: !MONITOR_ERR!
    echo [WARN]  Continuing without live monitor. Tail %MONITOR_LOG_FILE% for log.
    del "%TEMP%\epf_monitor_err.txt" >nul 2>&1
    exit /b 0
)
if exist "%TEMP%\epf_monitor_pid.txt" (
    set /p MONITOR_PID=<"%TEMP%\epf_monitor_pid.txt"
    echo [OK]    Live monitor opened in separate window ^(cmd PID: !MONITOR_PID!^).
) else (
    echo [WARN]  Monitor may not have started ^(no PID captured^).
    echo [WARN]  Continuing without live monitor. Tail %LOG_FILE% for log.
)
del "%TEMP%\epf_monitor_pid.txt" >nul 2>&1
exit /b 0

REM ============================================================================
REM Reclaim warning banner + confirmation prompt
REM ============================================================================
REM The drop-and-recreate reclaim path drops every PK/UK/FK constraint and
REM every non-LOB index in OPPAYMENTS + OP, compacts the data tablespace,
REM resizes datafiles, then recreates everything from captured DDL. During
REM the window the schema lives without indexes and without PK uniqueness
REM enforcement, so the application MUST be quiesced for the duration.
REM
REM Sets RECLAIM_CONFIRMED=Y if the user confirms (or --assume-yes), else N.
:reclaim_warning_banner
set "RECLAIM_CONFIRMED=N"
echo.
echo   ============================================================
echo   TABLESPACE RECLAIM - drop indexes / compact / recreate
echo   ============================================================
echo   This reclaim path will, against the OPPAYMENTS + OP schemas:
echo.
echo     1. Capture DDL for every PK/UK/FK constraint and every non-LOB
echo        index ^(non-constraint indexes only at this step^).
echo     2. DROP every FK constraint, then every PK/UK constraint
echo        ^(Oracle drops their backing index too^), then the remaining
echo        non-constraint indexes.
echo     3. SHRINK SPACE CASCADE on every table.
echo     4. Resize the data tablespace datafile^(s^) down to actual HWM.
echo     5. If a separate INDEX tablespace is detected, resize it too.
echo     6. Recreate every index, PK/UK and FK from the captured DDL.
echo        Indexes go back to their original tablespace.
echo     7. Final resize to capture true post-recreate footprint.
echo.
echo   IMPORTANT:
echo     * Indexes and PK uniqueness are NOT enforced during the window.
echo       The application must be quiesced ^(no writes^) for the duration.
echo     * If the run fails AFTER drops and BEFORE recreates complete, the
echo       schema is in a partial state. Recovery: re-run the reclaim
echo       ^(captured DDL is rebuilt fresh from the DB each run^) or restore
echo       from backup.
echo     * Tablespaces are auto-detected from OPPAYMENTS + OP metadata.
echo       No need to name them; works for any tablespace name and for
echo       both shared and separated data/index layouts.
echo   ============================================================
echo.
if /i "%ASSUME_YES%"=="Y" (
    echo [INFO]  --assume-yes set; proceeding without prompt.
    set "RECLAIM_CONFIRMED=Y"
    exit /b 0
)
REM No interactive confirmation. The banner above is informational only --
REM the operator already opted in by passing --reclaim or --reclaim-only,
REM so a second Y/N prompt was just friction.
set "RECLAIM_CONFIRMED=Y"
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
echo   --no-dry-run      Force dry-run off (overrides config file / earlier flag)
echo   --optimize-db     Run DB optimization before purge (enlarge redo logs, gather stats)
echo                     Needs DBA/SYS creds. ~4 GB temp disk space. Idempotent.
echo   --no-optimize-db  Force optimize-db off
echo   --reclaim         After purge, run online space reclaim (SHRINK + squeeze + resize)
echo                     No downtime required. Needs DBA/SYS creds.
echo   --no-reclaim      Force reclaim off
echo   --reclaim-only    Skip purge entirely, run online reclaim only
echo   --max-iterations N Reclaim squeeze cap. Default = max(2000, 50*datafile_gb),
echo                     capped at 20000. Wrapper recommends a value based on the
echo                     OPPAYMENTS tablespace datafile size queried at startup.
echo   --no-stall-check  Disable stall detection during reclaim (always run all iterations)
echo   --allow-offline-index-rebuild  Permit DROP+CREATE INDEX fallback when an
echo                     index refuses to relocate via REBUILD ONLINE due to
echo                     locality bias. The index is briefly unavailable while
echo                     being recreated. Safe for clones / outage windows;
echo                     do NOT pass this in prod with concurrent users.
echo   --drop-pkg        Drop the PL/SQL package after execution
echo   --no-drop-pkg     Force drop-pkg off
echo   --drop-logs       Drop purge log tables (epf_purge_log, epf_purge_space_snapshot)
echo   --truncate-logs   Clear all purge run history from the DB log tables before
echo                     starting (keeps tables, removes rows). Useful for fresh runs.
echo   --no-truncate-logs Force truncate-logs off
echo   --show-sizes      DEPRECATED: module sizes are now always shown automatically.
echo                     Flag accepted for back-compat; does nothing.
echo   --help, -h        Show this help message
echo.
echo Notes:
echo   * Any flag passed on the command line skips its corresponding interactive
echo     prompt. Pass --tns + --password (or set EPF_PURGE_PASSWORD) to skip ALL
echo     prompts and run unattended in one command.
echo.
echo Environment Variables:
echo   EPF_PURGE_PASSWORD   Database password (overrides config and --password)
echo   EPF_SYS_PASSWORD     SYS / DBA password (overrides config and --sys-password).
echo                        Use this for unattended runs with --reclaim or --optimize-db.
echo.
echo Examples:
echo   epf_purge.bat
echo   epf_purge.bat --tns EPFPROD --user oppayments --retention 90 --dry-run
echo   epf_purge.bat --config ..\config\epf_purge.conf
echo.
exit /b 0
