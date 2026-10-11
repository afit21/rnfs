# Registers a Task Scheduler task that starts the rnfs daemon when you log on to Windows, with no window.
#
#   powershell -ExecutionPolicy Bypass -File contrib\install-windows-task.ps1            # install and start
#   powershell -ExecutionPolicy Bypass -File contrib\install-windows-task.ps1 -Uninstall # remove
#
# Logs go to %USERPROFILE%\.rnfs\daemon.log. Stopping the task (Stop-ScheduledTask, or logging off) ends the
# daemon without a clean shutdown; rnfs saves its state every 30 s, so at most that much is lost. For a clean
# stop, run `rnfs daemon` in a terminal instead and press Ctrl+C.
#
# It assumes `pipx install`, which puts rnfs.exe in %USERPROFILE%\.local\bin; pass -Rnfs to use another path.

param(
    [string]$Rnfs = (Join-Path $env:USERPROFILE ".local\bin\rnfs.exe"),
    [switch]$Uninstall
)

$ErrorActionPreference = "Stop"
$TaskName = "rnfs daemon"

if ($Uninstall) {
    Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Removed the '$TaskName' task."
    exit 0
}

if (-not (Test-Path $Rnfs)) {
    Write-Error "rnfs not found at $Rnfs. Install it with pipx first, or pass -Rnfs <path to rnfs.exe>."
}

$RnfsDir = Join-Path $env:USERPROFILE ".rnfs"
New-Item -ItemType Directory -Force -Path $RnfsDir | Out-Null
$Log = Join-Path $RnfsDir "daemon.log"

# A path as a single-quoted PowerShell string literal. A quote inside it is doubled, which is how a
# single-quoted string escapes it; PowerShell also treats the typographic quotes U+2018 to U+201B as single
# quotes, so those are doubled too. Otherwise a path like C:\Users\O'Brien would break out of the quotes.
function ConvertTo-QuotedLiteral([string]$Text) {
    "'" + ($Text -replace "['\u2018\u2019\u201A\u201B]", '$0$0') + "'"
}

# A hidden PowerShell runs the daemon so no console window stays open, and appends its output to the log
$Command = "& $(ConvertTo-QuotedLiteral $Rnfs) daemon *>> $(ConvertTo-QuotedLiteral $Log)"
$Action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -WindowStyle Hidden -Command `"$Command`""
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
# Keep running on battery, never time out, and restart a few times if it crashes
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)

Register-ScheduledTask -TaskName $TaskName -Action $Action -Trigger $Trigger -Settings $Settings `
    -Description "rnfs: content-addressed file sharing over Reticulum" -Force | Out-Null
Start-ScheduledTask -TaskName $TaskName
Write-Host "Registered and started '$TaskName'. Logs: $Log"
