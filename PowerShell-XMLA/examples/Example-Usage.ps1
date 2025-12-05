# ============================================
# Example Usage - Power BI XMLA with Service Principal
# ============================================

# ============================================
# Example 1: Basic Usage with Direct Parameters
# ============================================

.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-full-table.tmsl.json" `
    -TenantId "your-tenant-id" `
    -ClientId "your-client-id" `
    -ClientSecret "your-client-secret"


# ============================================
# Example 2: Using Environment Variables (Recommended)
# ============================================

# Set environment variables once (in PowerShell profile or system environment)
$env:POWERBI_TENANT_ID = "your-tenant-id"
$env:POWERBI_CLIENT_ID = "your-client-id"
$env:POWERBI_CLIENT_SECRET = "your-client-secret"

# Then use them in the script
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-full-table.tmsl.json" `
    -TenantId $env:POWERBI_TENANT_ID `
    -ClientId $env:POWERBI_CLIENT_ID `
    -ClientSecret $env:POWERBI_CLIENT_SECRET


# ============================================
# Example 3: Refresh Specific Partition
# ============================================

.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-specific-partition.tmsl.json" `
    -TenantId $env:POWERBI_TENANT_ID `
    -ClientId $env:POWERBI_CLIENT_ID `
    -ClientSecret $env:POWERBI_CLIENT_SECRET


# ============================================
# Example 4: Refresh Multiple Partitions
# ============================================

.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-multiple-partitions.tmsl.json" `
    -TenantId $env:POWERBI_TENANT_ID `
    -ClientId $env:POWERBI_CLIENT_ID `
    -ClientSecret $env:POWERBI_CLIENT_SECRET


# ============================================
# Example 5: Schedule with Windows Task Scheduler
# ============================================

# Create a scheduled task to run daily at 2 AM
$Action = New-ScheduledTaskAction -Execute "PowerShell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"C:\Scripts\PowerShell-XMLA\Invoke-PowerBIXmlaCommand.ps1`" -WorkspaceName `"Finance Workspace`" -DatasetName `"Financial Model`" -TmslFile `"C:\Scripts\PowerShell-XMLA\examples\refresh-full-table.tmsl.json`" -TenantId `"$env:POWERBI_TENANT_ID`" -ClientId `"$env:POWERBI_CLIENT_ID`" -ClientSecret `"$env:POWERBI_CLIENT_SECRET`""

$Trigger = New-ScheduledTaskTrigger -Daily -At 2am

$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

Register-ScheduledTask `
    -TaskName "PowerBI-DailyRefresh" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -User "SYSTEM" `
    -Description "Daily Power BI semantic model refresh"


# ============================================
# Example 6: Error Handling
# ============================================

try {
    .\Invoke-PowerBIXmlaCommand.ps1 `
        -WorkspaceName "Finance Workspace" `
        -DatasetName "Financial Model" `
        -TmslFile ".\examples\refresh-full-table.tmsl.json" `
        -TenantId $env:POWERBI_TENANT_ID `
        -ClientId $env:POWERBI_CLIENT_ID `
        -ClientSecret $env:POWERBI_CLIENT_SECRET `
        -ErrorAction Stop

    if ($LASTEXITCODE -eq 0) {
        Write-Host "Refresh completed successfully" -ForegroundColor Green
    }
    else {
        Write-Host "Refresh failed with exit code: $LASTEXITCODE" -ForegroundColor Red
    }
}
catch {
    Write-Host "Script execution failed: $($_.Exception.Message)" -ForegroundColor Red
}
