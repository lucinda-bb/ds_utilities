# ---------- CONFIG ----------
$PythonExe = "C:\Users\Local Admin\AppData\Local\Programs\Python\Python311\python.exe"
$RobocopyScript = "C:\2026 Utilities Management\Functions\copy_acquisuite_logs.ps1"
$PythonScript   = "C:\2026 Utilities Management\Functions\load_acquisuite_any_csv.py"

$LogDir = "C:\2026 Utilities Management\Robocopy_logs"
if (!(Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

$LogFile = Join-Path $LogDir ("pipeline_" + (Get-Date -Format "yyyy-MM-dd_HH-mm") + ".log")

# ---------- LOGGING ----------
Start-Transcript -Path $LogFile -Append

Write-Host "=== PIPELINE START $(Get-Date) ==="

# ---------- STEP 1: ROBOCOPY ----------
Write-Host "Running robocopy..."
powershell -NoProfile -ExecutionPolicy Bypass -File $RobocopyScript
$rc = $LASTEXITCODE
Write-Host "Robocopy exit code: $rc"

# Robocopy codes 0–7 are OK
if ($rc -ge 8) {
    Write-Error "Robocopy failed with exit code $rc"
    Stop-Transcript
    exit $rc
}

# ---------- STEP 2: PYTHON PARSE ----------
Write-Host "Running Python parser..."
& $PythonExe $PythonScript
$pyExit = $LASTEXITCODE
Write-Host "Python exit code: $pyExit"

if ($pyExit -ne 0) {
    Write-Error "Python script failed"
    Stop-Transcript
    exit $pyExit
}

Write-Host "=== PIPELINE COMPLETE $(Get-Date) ==="
Stop-Transcript
