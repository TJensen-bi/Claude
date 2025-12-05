# ============================================
# Example Usage Scripts
# ============================================

# Example 1: Using direct parameters
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-full-table.tmsl.json" `
    -TenantId "your-tenant-id" `
    -ClientId "your-client-id" `
    -ClientSecret "your-client-secret"

# Example 2: Using config file
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-specific-partition.tmsl.json" `
    -ConfigFile ".\config\config.json"

# Example 3: Using Azure Key Vault (requires Az.KeyVault module and authentication)
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-multiple-partitions.tmsl.json" `
    -UseKeyVault `
    -KeyVaultName "tfa-kv-auth-DAP-0001"

# Example 4: Refresh specific partition with custom log path
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-specific-partition.tmsl.json" `
    -ConfigFile ".\config\config.json" `
    -LogPath "D:\Logs\powerbi-refresh.log"

# Example 5: Run with increased timeout (for large datasets)
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-full-table.tmsl.json" `
    -ConfigFile ".\config\config.json" `
    -TimeoutSeconds 600

# ============================================
# Scheduled Task Example (Windows Task Scheduler)
# ============================================

# Create a scheduled task to run the script daily at 2 AM
$Action = New-ScheduledTaskAction -Execute "PowerShell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"C:\Scripts\PowerShell-XMLA\Invoke-PowerBIXmlaCommand.ps1`" -WorkspaceName `"Finance Workspace`" -DatasetName `"Financial Model`" -TmslFile `"C:\Scripts\PowerShell-XMLA\examples\refresh-full-table.tmsl.json`" -ConfigFile `"C:\Scripts\PowerShell-XMLA\config\config.json`""

$Trigger = New-ScheduledTaskTrigger -Daily -At 2am

$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -RunOnlyIfNetworkAvailable

Register-ScheduledTask -TaskName "PowerBI-DailyRefresh" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -Description "Daily Power BI semantic model refresh"

# ============================================
# Error Handling Example
# ============================================

try {
    $Result = .\Invoke-PowerBIXmlaCommand.ps1 `
        -WorkspaceName "Finance Workspace" `
        -DatasetName "Financial Model" `
        -TmslFile ".\examples\refresh-full-table.tmsl.json" `
        -ConfigFile ".\config\config.json" `
        -ErrorAction Stop

    if ($LASTEXITCODE -eq 0) {
        Write-Host "Refresh completed successfully" -ForegroundColor Green
        # Send success notification (email, Teams, etc.)
    }
    else {
        Write-Host "Refresh failed with exit code: $LASTEXITCODE" -ForegroundColor Red
        # Send failure notification
    }
}
catch {
    Write-Host "Script execution failed: $($_.Exception.Message)" -ForegroundColor Red
    # Send failure notification
}
