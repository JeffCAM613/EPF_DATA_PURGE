# ============================================================================
# EPF Data Purge - Live Progress Monitor (PowerShell)
# ============================================================================
# Runs alongside the purge in a separate process.  Polls epf_purge_log via
# SQL*Plus every N seconds and prints new entries to stdout in real-time.
# Output is NOT buffered (each poll is a separate SQL*Plus invocation).
#
# Automatically exits when it sees RECLAIM_END, a top-level ERROR,
# or when the wrapper script kills it after the run completes.
#
# Usage:
#   powershell -File epf_monitor.ps1 -ConnStr "user/pass@tns" -PollSec 10
# ============================================================================
param(
    [Parameter(Mandatory=$true)]
    [string]$ConnStr,         # SQL*Plus connection string: user/pass@tns

    [int]$PollSec = 10,       # Poll interval in seconds
    [int]$MaxWaitMin = 360,   # Max idle wait in minutes before timeout

    [string]$LogFile = ""     # Optional: also append to this file
)

$ErrorActionPreference = "SilentlyContinue"

# Open log file with FileShare.ReadWrite so the main process (which also holds
# the file open with ReadWrite sharing) does not cause a sharing violation.
# The previous Out-File approach used FileShare.Read internally, which silently
# failed when the main purge process already held a write handle.
$logStream = $null
$logWriter = $null
if ($LogFile -ne "") {
    try {
        $logStream = [IO.FileStream]::new($LogFile, 'Append', 'Write', 'ReadWrite')
        $logWriter = [IO.StreamWriter]::new($logStream, [Text.Encoding]::UTF8)
        $logWriter.AutoFlush = $true
    } catch {
        Write-Host "[MONITOR] WARNING: Could not open log file for writing: $_"
        $logWriter = $null
        $logStream = $null
    }
}


# Write-Host is the standard PowerShell way to print to the host (console)
# and works reliably when the monitor runs in its own console window. The
# previous raw-StreamWriter / Console.Out.Flush experiments were only needed
# when the monitor shared a parent cmd's console via Start-Process
# -NoNewWindow (which had handle-inheritance buffering bugs). With the new
# layout (separate console window on Windows, suppressed stdout on Linux),
# those workarounds aren't needed.
function Write-Log($msg) {
    Write-Host $msg
    if ($script:logWriter) {
        try { $script:logWriter.WriteLine($msg) } catch { }
    }
}

# Hard-timeout sqlplus runner. Returns the array of non-empty stdout lines, or
# $null on timeout / process failure. Replaces the prior "$sql | sqlplus -S
# $Conn" pattern that had no timeout: a single stalled sqlplus call (network
# blip, blocked session, hung session-info query) would freeze the polling
# loop indefinitely until the operator killed the window. With this wrapper,
# the worst case per poll is a $TimeoutSec wait followed by a clean retry.
#
# Notes:
#   -L: do not re-prompt for credentials on logon failure; exit immediately.
#       Without -L, a stale password or TNS hiccup makes sqlplus hang on the
#       "Username:" prompt forever (it reads stdin, which we never close).
#   Async stdout read (ReadToEndAsync) prevents pipe-fill deadlock if the
#   query somehow returns more data than the OS pipe buffer.
#   Temp-file SQL avoids the $sql|sqlplus stdin pipe, which on PowerShell 5.1
#   has been observed to occasionally stall on Windows.
function Invoke-Sqlplus {
    param(
        [Parameter(Mandatory=$true)] [string]$Sql,
        [Parameter(Mandatory=$true)] [string]$Conn,
        [int]$TimeoutSec = 60
    )
    $tag    = [Guid]::NewGuid().ToString("N").Substring(0, 8)
    $tmpSql = Join-Path $env:TEMP ("epf_mon_" + $tag + ".sql")
    $proc   = $null
    try {
        [IO.File]::WriteAllText($tmpSql, $Sql, [Text.Encoding]::ASCII)

        $psi = New-Object System.Diagnostics.ProcessStartInfo
        $psi.FileName               = "sqlplus"
        $psi.Arguments              = "-L -S `"$Conn`" `"@$tmpSql`""
        $psi.UseShellExecute        = $false
        $psi.RedirectStandardOutput = $true
        $psi.RedirectStandardError  = $true
        $psi.CreateNoWindow         = $true

        $proc = [System.Diagnostics.Process]::Start($psi)
        $stdoutTask = $proc.StandardOutput.ReadToEndAsync()
        $null       = $proc.StandardError.ReadToEndAsync()  # drain to avoid stderr-pipe deadlock

        if (-not $proc.WaitForExit($TimeoutSec * 1000)) {
            try { $proc.Kill() } catch { }
            try { $proc.WaitForExit(2000) | Out-Null } catch { }
            return $null
        }

        $stdout = $stdoutTask.Result
        if (-not $stdout) { return @() }
        return ($stdout -split "`r?`n") | Where-Object { $_.Trim() -ne "" }
    }
    catch {
        return $null
    }
    finally {
        if ($proc) { try { $proc.Dispose() } catch { } }
        if (Test-Path -LiteralPath $tmpSql) {
            Remove-Item -LiteralPath $tmpSql -ErrorAction SilentlyContinue
        }
    }
}

# Track consecutive timeouts so a stuck sqlplus surfaces as a visible warning
# rather than a silent gap in the log.
$script:consecutiveTimeouts = 0
function Note-PollResult($result, $context) {
    if ($null -eq $result) {
        $script:consecutiveTimeouts++
        # Warn on first timeout, then every 5th, to avoid spamming the log.
        if ($script:consecutiveTimeouts -eq 1 -or
            ($script:consecutiveTimeouts % 5) -eq 0) {
            Write-Log ("[MONITOR] WARN: sqlplus poll timed out (" + $context +
                ", consecutive=" + $script:consecutiveTimeouts +
                "). Will retry on next interval.")
        }
    }
    else {
        $script:consecutiveTimeouts = 0
    }
}

Write-Log "============================================================"
Write-Log "  EPF PURGE - LIVE MONITOR"
Write-Log "  Poll interval: ${PollSec}s   Max wait: ${MaxWaitMin} min"
Write-Log "  Started:       $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Log "============================================================"
Write-Log ""

$lastLogId = 0
$runId = ""
$foundRun = $false
$done = $false
$idleSince = Get-Date
$waitMsgCount = 0
$monitorStartTime = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss")

while (-not $done) {
    # Build a SQL query that fetches new log entries
    if (-not $foundRun) {
        # Find the latest run_id that is either:
        #   (a) still in progress (no RECLAIM_END and no top-level ERROR), OR
        #   (b) started after this monitor was launched
        # This prevents the monitor from picking up a completed old run,
        # replaying its RECLAIM_END / ERROR termination event, and exiting
        # before the new run even starts.
        $sql = @"
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 300 TRIMOUT ON TRIMSPOOL ON
SELECT RAWTOHEX(run_id) FROM (
    SELECT run_id FROM oppayments.epf_purge_log
    WHERE operation = 'RUN_START'
      AND (
          -- Run started after the monitor launched (definitely current)
          log_timestamp >= TO_TIMESTAMP('$monitorStartTime', 'YYYY-MM-DD HH24:MI:SS')
          OR
          -- Run has not completed yet (still in progress)
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
"@
        $result = Invoke-Sqlplus -Sql $sql -Conn $ConnStr -TimeoutSec 60
        Note-PollResult $result "discover-run-id"
        if ($result) {
            $runId = ($result | Select-Object -First 1).Trim()
            if ($runId -and $runId.Length -ge 16) {
                $foundRun = $true
                $idleSince = Get-Date
                Write-Log "[MONITOR] Tracking run_id: $runId"
                Write-Log ""
            }
        }
        if (-not $foundRun) {
            $waitMsgCount++
            if ($waitMsgCount % 6 -eq 1) {
                Write-Log "[MONITOR] Waiting for purge to start..."
            }
        }
    }

    if ($foundRun) {
        # Fetch new log entries since last_log_id
        $sql = @"
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
WHERE run_id = HEXTORAW('$runId')
  AND log_id > $lastLogId
ORDER BY log_id;
EXIT;
"@
        $rows = Invoke-Sqlplus -Sql $sql -Conn $ConnStr -TimeoutSec 60
        Note-PollResult $rows "fetch-new-log-entries"
        if ($null -eq $rows) { $rows = @() }

        $anyNew = $false
        foreach ($row in $rows) {
            $parts = $row.Trim().Split('|')
            if ($parts.Count -lt 8) { continue }

            $logId      = [int]$parts[0].Trim()
            $ts         = $parts[1].Trim()
            $module     = $parts[2].Trim()
            $operation  = $parts[3].Trim()
            $batchNum   = $parts[4].Trim()
            $rowsAff    = $parts[5].Trim()
            $status     = $parts[6].Trim()
            $message    = $parts[7].Trim()
            $elapsed    = if ($parts.Count -gt 8) { $parts[8].Trim() } else { "" }
            $tableName  = if ($parts.Count -gt 9) { $parts[9].Trim() } else { "" }

            $anyNew = $true
            if ($logId -gt $lastLogId) { $lastLogId = $logId }
            $idleSince = Get-Date

            # Format output based on entry type
            if ($operation -eq "RUN_START") {
                Write-Log "[$ts] ** PURGE STARTED ** $message"
            }
            elseif ($operation -eq "RUN_END") {
                if ($status -eq "ERROR") {
                    Write-Log "[$ts] *** PURGE FAILED *** $message"
                    $done = $true
                }
                else {
                    Write-Log "[$ts] ** PURGE COMPLETED ** $message (total: ${elapsed}s)"
                    Write-Log "[$ts] Waiting for reclaim to start..."
                    # Don't exit -- reclaim may follow. Monitor continues until
                    # RECLAIM_END, idle timeout, or the wrapper script kills it.
                }
            }
            elseif ($operation -eq "RECLAIM_START") {
                Write-Log ""
                Write-Log "[$ts] ** RECLAIM STARTED ** $message"
            }
            elseif ($operation -eq "RECLAIM_END") {
                if ($status -eq "ERROR") {
                    Write-Log "[$ts] *** RECLAIM FAILED *** $message"
                }
                else {
                    Write-Log "[$ts] ** RECLAIM COMPLETED ** $message (total: ${elapsed}s)"
                }
                $done = $true
            }
            elseif ($operation -eq "SHRINK_DONE") {
                Write-Log "[$ts] RECLAIM      Phase 1 SHRINK done: $message (${elapsed}s)"
            }
            elseif ($operation -eq "SHRINK_PROGRESS" -or $operation -eq "SQUEEZE_PROGRESS") {
                Write-Log "[$ts] RECLAIM      $message (${elapsed}s)"
            }
            elseif ($operation -eq "DELETE" -and $batchNum -ne "") {
                # Batch progress
                $mod = $module.PadRight(12)
                Write-Log "[$ts] $mod batch $($batchNum.PadLeft(5))  $message  (${elapsed}s)"
            }
            elseif ($operation -eq "DELETE" -and $tableName -ne "" -and $batchNum -eq "" -and [int]$rowsAff -gt 0) {
                # Per-table total
                $mod = $module.PadRight(12)
                $tbl = $tableName.PadRight(48)
                $ra  = $rowsAff.PadLeft(14)
                Write-Log "[$ts] $mod TOTAL  $tbl $ra"
            }
            elseif ($status -eq "ERROR") {
                Write-Log "[$ts] *** ERROR *** $module - $message"
                $done = $true
            }
            elseif ($operation -match "DRY_RUN_COUNT|INFO|INIT") {
                $mod = $module.PadRight(12)
                Write-Log "[$ts] $mod $message"
            }
            else {
                if ($message -ne "") {
                    $mod = ($module + " ").Substring(0, [Math]::Min(12, $module.Length + 1)).PadRight(12)
                    Write-Log "[$ts] $mod $message"
                }
            }
        }

        # If no new rows, check for newer run_id (handles cancelled/restarted runs)
        if (-not $anyNew -and $lastLogId -gt 0) {
            $sqlRecheck = @"
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 300 TRIMOUT ON TRIMSPOOL ON
SELECT RAWTOHEX(run_id) FROM (
    SELECT run_id FROM oppayments.epf_purge_log
    WHERE operation = 'RUN_START'
    ORDER BY log_timestamp DESC
) WHERE ROWNUM = 1;
EXIT;
"@
            $recheck = Invoke-Sqlplus -Sql $sqlRecheck -Conn $ConnStr -TimeoutSec 60
            Note-PollResult $recheck "recheck-latest-run-id"
            $latestId = $null
            if ($recheck) { $latestId = ($recheck | Select-Object -First 1).Trim() }
            if ($latestId -and $latestId.Length -ge 16 -and $latestId -ne $runId) {
                Write-Log ""
                Write-Log "[MONITOR] Newer run detected: $latestId (was $runId)"
                Write-Log "[MONITOR] Switching to new run_id..."
                Write-Log ""
                $runId = $latestId
                $lastLogId = 0
                $idleSince = Get-Date
            }
            else {
                # Same run — check for missed completion
                $sqlCheck = @"
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 50 TRIMOUT ON TRIMSPOOL ON
SELECT COUNT(*) FROM oppayments.epf_purge_log
WHERE run_id = HEXTORAW('$runId')
  AND (operation = 'RECLAIM_END' OR (module = 'ORCHESTRATOR' AND status = 'ERROR'));
EXIT;
"@
                $endRows = Invoke-Sqlplus -Sql $sqlCheck -Conn $ConnStr -TimeoutSec 60
                Note-PollResult $endRows "check-end-marker"
                if ($endRows) {
                    $endCount = ($endRows | Select-Object -First 1).Trim()
                    if ($endCount -and [int]$endCount -gt 0) {
                        $done = $true
                    }
                }
            }
        }
    }

    if ($done) { break }

    # Check timeout
    $idleSeconds = ((Get-Date) - $idleSince).TotalSeconds
    if ($idleSeconds -gt ($MaxWaitMin * 60)) {
        Write-Log "[MONITOR] Timeout: no activity for $([Math]::Round($idleSeconds / 60)) minutes. Exiting."
        break
    }

    Start-Sleep -Seconds $PollSec
}

Write-Log ""
Write-Log "[MONITOR] Monitor stopped at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Log "============================================================"

# Clean up log file handles
if ($logWriter) { try { $logWriter.Close() } catch { } }
if ($logStream) { try { $logStream.Close() } catch { } }
