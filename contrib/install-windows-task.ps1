# Registers a Task Scheduler task that starts the rnfs daemon when you log on to Windows, with no window.
#
#   powershell -ExecutionPolicy Bypass -File contrib\install-windows-task.ps1            # install and start
#   powershell -ExecutionPolicy Bypass -File contrib\install-windows-task.ps1 -Stop      # stop the daemon
#   powershell -ExecutionPolicy Bypass -File contrib\install-windows-task.ps1 -Uninstall # stop it and remove
#
# The task runs %USERPROFILE%\.rnfs\daemon-task.ps1 (written here) in a hidden PowerShell. It appends the
# daemon's output to %USERPROFILE%\.rnfs\daemon.log as it is written (not rotated: empty it now and then),
# and restarts the daemon 10 s after it exits with an error, giving up after 5 such exits in 10 minutes.
# Stopping it (-Stop, -Uninstall, or logging off) ends the daemon without a clean shutdown; rnfs saves its
# state every 30 s, so at most that much is lost. For a clean stop, run `rnfs daemon` in a terminal instead
# and press Ctrl+C. (Stop-ScheduledTask by itself would stop only the hidden PowerShell, not the daemon.)
#
# It assumes rnfs.exe in %USERPROFILE%\.local\bin (where pipx and the README put it); pass -Rnfs to use
# another path.

param(
    [string]$Rnfs = (Join-Path $env:USERPROFILE ".local\bin\rnfs.exe"),
    [switch]$Stop,
    [switch]$Uninstall
)

$ErrorActionPreference = "Stop"
$TaskName = "rnfs daemon"
$RnfsDir = Join-Path $env:USERPROFILE ".rnfs"
$Log = Join-Path $RnfsDir "daemon.log"
$Wrapper = Join-Path $RnfsDir "daemon-task.ps1"

# Stops the task and every process it started. Task Scheduler ends only the process it launched (the hidden
# PowerShell), so its descendants (cmd.exe, rnfs.exe and any process rnfs.exe started) are looked up first,
# from the task's command line and the processes' parent IDs, and stopped after it.
function Stop-RnfsTask {
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if (-not $task) { return $false }
    $arguments = [string]$task.Actions[0].Arguments
    $tree = @()
    if ($arguments.Trim()) {
        try {
            $processes = @(Get-CimInstance Win32_Process)
            $tree = @($processes | Where-Object {
                $_.Name -eq "powershell.exe" -and $_.ProcessId -ne $PID -and $_.CommandLine -and
                $_.CommandLine.Contains($arguments)
            })
            $ids = @($tree | ForEach-Object { $_.ProcessId })
            for ($i = 0; $i -lt $tree.Count; $i++) {  # breadth first: $tree grows as children are found
                $parent = $tree[$i]
                foreach ($child in $processes) {
                    # (a child started before its "parent" has a parent ID that has been reused)
                    if ($child.ParentProcessId -eq $parent.ProcessId -and $ids -notcontains $child.ProcessId -and
                            $child.CreationDate -ge $parent.CreationDate) {
                        $tree += $child
                        $ids += $child.ProcessId
                    }
                }
            }
        } catch {
            Write-Warning "Couldn't list the task's processes ($_); stopping only the task."
        }
    }
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    foreach ($process in $tree) {  # the PowerShell first, so it can't restart rnfs
        Stop-Process -Id $process.ProcessId -Force -ErrorAction SilentlyContinue
    }
    return ($tree.Count -gt 0)
}

if ($Stop -or $Uninstall) {
    if (Stop-RnfsTask) { Write-Host "Stopped the rnfs daemon." }
    if ($Uninstall) {
        if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
            Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        }
        if (Test-Path -LiteralPath $Wrapper) { Remove-Item -LiteralPath $Wrapper -Force }
        Write-Host "Removed the '$TaskName' task."
    }
    exit 0
}

if (-not (Test-Path -LiteralPath $Rnfs -PathType Leaf)) {
    throw "rnfs not found at $Rnfs. Install it first, or pass -Rnfs <path to rnfs.exe>."
}
$Rnfs = (Resolve-Path -LiteralPath $Rnfs).ProviderPath
# cmd.exe runs the daemon (below) and would expand %NAME% in these paths
foreach ($path in @($Rnfs, $Log)) {
    if ($path.Contains("%")) { throw "Can't run rnfs from a path containing %: $path" }
}
[void][System.IO.Directory]::CreateDirectory($RnfsDir)

# A string as a single-quoted PowerShell string literal. A quote inside it is doubled, which is how a
# single-quoted string escapes it; PowerShell also treats the typographic quotes U+2018 to U+201B as single
# quotes, so those are doubled too. Otherwise a path like C:\Users\O'Brien would break out of the quotes.
function ConvertTo-QuotedLiteral([string]$Text) {
    "'" + ($Text -replace "['\u2018\u2019\u201A\u201B]", '$0$0') + "'"
}

# cmd.exe appends the daemon's output to the log as raw bytes (PowerShell 5.1's *>> would write UTF-16 and
# wrap each stderr line in an error record). /s /c "..." runs the text between the outer quotes as it is;
# /d skips AutoRun commands and /v:off keeps ! literal.
$CmdArguments = '/d /v:off /s /c ""' + $Rnfs + '" daemon >> "' + $Log + '" 2>&1"'
$Script = @'
# Written by rnfs's contrib\install-windows-task.ps1; the "rnfs daemon" task runs it at logon.
# Runs the daemon, appending its output to the log, and restarts it 10 s after it exits with an error,
# giving up after 5 such exits in 10 minutes. A clean exit (code 0) ends the task; code 75 is the daemon asked to
# restart (POST /api/restart), which starts it again at once and counts as no failure.
$log = __RNFS_LOG__
$arguments = __RNFS_CMD_ARGUMENTS__
$failures = @()
# Tells the daemon who started it (daemon.json's started_by), and so that it can restart by exiting with 75
$env:RNFS_STARTED_BY = "windows_task"
while ($true) {
    $info = New-Object System.Diagnostics.ProcessStartInfo
    $info.FileName = Join-Path $env:SystemRoot "System32\cmd.exe"
    $info.Arguments = $arguments
    $info.UseShellExecute = $false
    $info.CreateNoWindow = $true
    $process = [System.Diagnostics.Process]::Start($info)
    $process.WaitForExit()
    $code = $process.ExitCode
    if ($code -eq 0) { exit 0 }
    if ($code -eq 75) { continue }
    $now = Get-Date
    $failures = @($failures | Where-Object { ($now - $_).TotalMinutes -lt 10 }) + $now
    if ($failures.Count -ge 5) {
        [System.IO.File]::AppendAllText($log, "$($now.ToString('s')) rnfs exited with code $code, 5 times in 10 minutes; not restarting it`r`n")
        exit 1
    }
    [System.IO.File]::AppendAllText($log, "$($now.ToString('s')) rnfs exited with code $code; restarting it in 10 s`r`n")
    Start-Sleep -Seconds 10
}
'@
$Script = $Script.Replace("__RNFS_LOG__", (ConvertTo-QuotedLiteral $Log))
$Script = $Script.Replace("__RNFS_CMD_ARGUMENTS__", (ConvertTo-QuotedLiteral $CmdArguments))

# Replacing a running task: stop it (and its daemon) first
[void](Stop-RnfsTask)
# UTF-8 with a BOM: without one, Windows PowerShell 5.1 would read non-ASCII characters in the paths as ANSI
[System.IO.File]::WriteAllText($Wrapper, $Script, (New-Object System.Text.UTF8Encoding $true))

# A path can't contain a double quote on Windows, so quoting it for -File is enough
$Action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$Wrapper`""
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
# Keep running on battery and never time out (restarts after a crash are the wrapper's job: Task Scheduler's
# own restart settings only cover a task that fails to start)
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings `
    -Description "rnfs: content-addressed file sharing over Reticulum" -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName
Write-Host "Registered and started '$TaskName'. Logs: $Log"
