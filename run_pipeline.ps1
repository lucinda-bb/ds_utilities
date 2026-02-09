# ---------- CONFIG ----------
# tell program where to find python and the scripts
$PythonExe = "C:\Users\Local Admin\AppData\Roaming\Microsoft\Windows\Start Menu\Programs\Python\Python 3.14\Python 3.14.lnk"
$RobocopyScript = "C:\2026 Utilities Management\Functions\copy_aquisuite_logs.ps1"
$PythonScript   = "C:\2026 Utilities Management\Functions\load_acquisuite_any_csv.py"
$LogDir = "C:\2026 Utilities Management\logs\Pipeline_logs"

# create log directory if it doesn't exist, and set up log file path
if (!(Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }
$LogFile = Join-Path $LogDir ("pipeline_" + (Get-Date -Format "yyyy-MM-dd_HH-mm") + ".log")

# ---------- LOGGING ----------
# Start transcript to capture all terminal output in a log file
Start-Transcript -Path $LogFile -Append

# add the pipeline start date to the terminal/log
Write-Host "=== PIPELINE START $(Get-Date) ==="

# ---------- STEP 1: ROBOCOPY ----------
# run the robocopy script and capture its exit code
# the exit code indicates the status of the file copy operation
Write-Host "Running robocopy..."
powershell -NoProfile -ExecutionPolicy Bypass -File $RobocopyScript
$rc = $LASTEXITCODE
Write-Host "Robocopy exit code: $rc"

# Robocopy codes 0–7 are OK
# if robocopy returns an 8 or higher, that indicates an error
if ($rc -ge 8) {
    Write-Error "Robocopy failed with exit code $rc"
    Stop-Transcript
    exit $rc
}

# ---------- STEP 2: PYTHON PARSE ----------
# go to the local file folder with all of the copied logs, and check for new logs
# just uploaded by robocopy. If a log is found that has not been parsed yet into the 
# postgres database, then the script will parse through the log and upload the data to postgres. If no new logs are found, then the script will exit without doing anything.

Write-Host "Running Python parser..."
& $PythonExe $PythonScript

# ask for the exit code of the python script, which indicates whether the parsing and database upload was successful
$pyExit = $LASTEXITCODE
Write-Host "Python exit code: $pyExit"

if ($pyExit -ne 0) {
    Write-Error "Python script failed"
    Stop-Transcript
    exit $pyExit
}

Write-Host "=== PIPELINE COMPLETE $(Get-Date) ==="
Stop-Transcript

# script complete, new log file with all files transferred will be available in the robocopy_logs folder