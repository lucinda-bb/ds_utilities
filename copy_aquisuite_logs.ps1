# use powershell robocopy to copy new meter log files from network to local folder
# and then create a log file

# where the new files land on the network drive
$SourceRoot = "Z:\Deep_Springs_College_Acquisuite_001EC60524FE\acquisuite_2024_to_present"

# where to copy the files locally
$Dest       = "C:\Deep_Springs_Aquisuite_Database"

# make the destination folder if it doesn't exist, and set up the log file path
if (!(Test-Path $Dest)) {
    New-Item -ItemType Directory -Path $Dest | Out-Null
}

# make a log file in the robocopy log folder with a record of all the files that were copied, 
# and the date they were copied on. also show files that are skipped
$LogFile = "C:\2026 Utilities Management\logs\Robocopy_logs\robocopy_$(Get-Date -Format 'yyyy-MM-dd_HH-mm').log.csv"

# copy all log.csv files from the source to the destination, 
# Log the output to a log file and also print it to the terminal (/TEE).
robocopy "$SourceRoot" "$Dest" *.log.csv /S /XO /R:1 /W:2 /LOG:$LogFile /TEE

# check the exit code returned by robocopy
# if the exit code is 8 or higher, that indicates an error occurred during the copy process.

if ($LASTEXITCODE -ge 8) {
    exit $LASTEXITCODE
} else {
    exit 0
}