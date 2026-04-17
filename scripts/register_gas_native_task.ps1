# Registers a daily Windows Task Scheduler job to run the native TSO gas
# backfill scripts. Run from an *elevated* PowerShell prompt (Run as Admin).
#
# Usage (edit time as you like):
#   powershell -ExecutionPolicy Bypass -File .\scripts\register_gas_native_task.ps1
#
# To change the trigger time, edit $runAtLocal below. We suggest 04:30 local so
# ENTSOG, GIE, ENTSO-E, and the four TSO portals (GRTGaz, AGGM, THE, Energinet)
# have published the previous gas-day (which closes at 06:00 CET) by run time.
#
# To remove the task later:
#   Unregister-ScheduledTask -TaskName "Gas Native Daily Backfill" -Confirm:$false

$ErrorActionPreference = "Stop"

# --- Config ---
$TaskName     = "Gas Native Daily Backfill"
$runAtLocal   = "04:30"               # 24h local time
$repoRoot     = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$pythonExe    = (Get-Command python.exe).Path
$scriptPath   = Join-Path $repoRoot "python\gas_native_daily.py"
$logDir       = Join-Path $repoRoot "python\logs"

if (-not (Test-Path $scriptPath)) {
    throw "Orchestrator not found at $scriptPath"
}
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

Write-Host "Task name   : $TaskName"
Write-Host "Python      : $pythonExe"
Write-Host "Script      : $scriptPath"
Write-Host "Working dir : $repoRoot"
Write-Host "Trigger     : every day at $runAtLocal (local time)"

$action    = New-ScheduledTaskAction `
    -Execute $pythonExe `
    -Argument "`"$scriptPath`"" `
    -WorkingDirectory $repoRoot

$trigger   = New-ScheduledTaskTrigger -Daily -At $runAtLocal

$principal = New-ScheduledTaskPrincipal `
    -UserId $env:UserName `
    -LogonType S4U `
    -RunLevel Limited

$settings  = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1)

Register-ScheduledTask `
    -TaskName   $TaskName `
    -Action     $action `
    -Trigger    $trigger `
    -Principal  $principal `
    -Settings   $settings `
    -Description "Daily backfill of FR/AT/DE/DK gas demand from native TSO sources (Bruegel methodology)." `
    -Force | Out-Null

Write-Host ""
Write-Host "OK. Task registered." -ForegroundColor Green
Write-Host "To run once now:"
Write-Host "  Start-ScheduledTask -TaskName `"$TaskName`""
Write-Host "To inspect:"
Write-Host "  Get-ScheduledTaskInfo -TaskName `"$TaskName`""
Write-Host "Logs will appear in: $logDir"
