$SourceRoot = "Z:\Deep_Springs_College_Acquisuite_001EC60524FE\acquisuite_2024_to_present"


$Dest       = "C:\Deep_Springs_Aquisuite_Database"

if (!(Test-Path $Dest)) {
    New-Item -ItemType Directory -Path $Dest | Out-Null
}
$LogFile = "C:\2026 Utilities Management\Robocopy_logs\robocopy_$(Get-Date -Format 'yyyy-MM-dd_HH-mm').log.csv"

robocopy "$SourceRoot" "$Dest" *.log.csv /S /XO /R:1 /W:2 /LOG:$LogFile /TEE


if ($LASTEXITCODE -ge 8) {
    exit $LASTEXITCODE
} else {
    exit 0
}