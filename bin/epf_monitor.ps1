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

function Write-Log($msg) {
    # [Console]::WriteLine bypasses Write-Host's host buffer, which can
    # otherwise hold output for many seconds when stdout is captured by a
    # parent cmd.exe process via Start-Process -NoNewWindow. The explicit
    # Flush() guarantees the line is on disk/screen before we move on.
    [Console]::Out.WriteLine($msg)
    [Console]::Out.Flush()
    if ($script:logWriter) {
        try { $script:logWriter.WriteLine($msg) } catch { }
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

while (-not $done) {
    # Build a SQL query that fetches new log entries
    if (-not $foundRun) {
        # Try to find the latest run_id
        $sql = @"
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 300 TRIMOUT ON TRIMSPOOL ON
SELECT RAWTOHEX(run_id) FROM (
    SELECT run_id FROM oppayments.epf_purge_log
    WHERE operation = 'RUN_START'
    ORDER BY log_timestamp DESC
) WHERE ROWNUM = 1;
EXIT;
"@
        $result = ($sql | sqlplus -S "$ConnStr" 2>$null) | Where-Object { $_.Trim() -ne "" }
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
        $rows = ($sql | sqlplus -S "$ConnStr" 2>$null) | Where-Object { $_.Trim() -ne "" }

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
            $latestId = (($sqlRecheck | sqlplus -S "$ConnStr" 2>$null) | Where-Object { $_.Trim() -ne "" } | Select-Object -First 1)
            if ($latestId) { $latestId = $latestId.Trim() }
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
                $endCount = (($sqlCheck | sqlplus -S "$ConnStr" 2>$null) | Where-Object { $_.Trim() -ne "" } | Select-Object -First 1).Trim()
                if ($endCount -and [int]$endCount -gt 0) {
                    $done = $true
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
