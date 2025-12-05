# Power BI XMLA Automation with Service Principal

Simple PowerShell script for automating Power BI TMSL commands using service principal authentication - no interactive login required.

## What Changed

This script takes the original XMLA script and adds automatic authentication using a service principal, so you don't need to manually log in every time.

### Added
- Service principal authentication (OAuth token)
- Three new parameters: `TenantId`, `ClientId`, `ClientSecret`
- Automatic token acquisition using MSAL.PS

### Original Script Flow (Unchanged)
- Load Analysis Services DLLs
- Connect to XMLA endpoint
- Load TMSL JSON file
- Execute TMSL command
- Cleanup and disconnect

---

## Prerequisites

### 1. Install MSAL.PS Module

```powershell
Install-Module -Name MSAL.PS -Scope CurrentUser -Force
```

### 2. Install SQL Server 2022 Client Libraries

If not already installed, download from: [SQL Server 2022 Feature Pack](https://www.microsoft.com/en-us/download/details.aspx?id=104781)

Required components:
- Microsoft Analysis Services (AMO)
- Microsoft Analysis Services ADOMD.NET

### 3. Create Azure AD Service Principal

```bash
# Using Azure CLI
az ad sp create-for-rbac --name "PowerBI-XMLA-Automation"
```

Note down:
- **Tenant ID** (tenant)
- **Client ID** (appId)
- **Client Secret** (password)

### 4. Configure Power BI Access

**Enable Service Principal in Power BI:**
1. Go to [Power BI Admin Portal](https://app.powerbi.com/admin-portal)
2. **Tenant settings** → **Developer settings**
3. Enable **"Allow service principals to use Power BI APIs"**
4. Apply to your security group containing the service principal

**Enable XMLA Endpoint:**
1. **Capacity settings** (Premium/PPU/Embedded required)
2. Enable **XMLA Endpoint** set to **Read Write**

**Grant Workspace Access:**
1. Add service principal to workspace as **Admin** or **Member**
2. Grant **Build** permission on the dataset

---

## Usage

### Basic Usage

```powershell
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-full-table.tmsl.json" `
    -TenantId "your-tenant-id" `
    -ClientId "your-client-id" `
    -ClientSecret "your-client-secret"
```

### Store Credentials Securely

Instead of typing credentials each time, store them in environment variables:

```powershell
# Set environment variables (one time)
$env:POWERBI_TENANT_ID = "your-tenant-id"
$env:POWERBI_CLIENT_ID = "your-client-id"
$env:POWERBI_CLIENT_SECRET = "your-client-secret"

# Use in script
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-full-table.tmsl.json" `
    -TenantId $env:POWERBI_TENANT_ID `
    -ClientId $env:POWERBI_CLIENT_ID `
    -ClientSecret $env:POWERBI_CLIENT_SECRET
```

---

## TMSL Examples

### Example 1: Refresh Full Table

**File: `refresh-full-table.tmsl.json`**
```json
{
  "refresh": {
    "type": "full",
    "objects": [
      {
        "database": "YourDatasetName",
        "table": "YourTableName"
      }
    ]
  }
}
```

### Example 2: Refresh Specific Partition

**File: `refresh-partition.tmsl.json`**
```json
{
  "refresh": {
    "type": "full",
    "objects": [
      {
        "database": "YourDatasetName",
        "table": "Finanspostering",
        "partition": "2025Q206"
      }
    ]
  }
}
```

### Example 3: Refresh Multiple Partitions

**File: `refresh-multiple.tmsl.json`**
```json
{
  "refresh": {
    "type": "full",
    "commitMode": "transactional",
    "objects": [
      {
        "database": "YourDatasetName",
        "table": "Finanspostering",
        "partition": "2025Q206"
      },
      {
        "database": "YourDatasetName",
        "table": "Finanspostering",
        "partition": "2025Q207"
      }
    ]
  }
}
```

---

## Schedule with Task Scheduler

Create a scheduled task to run automatically:

```powershell
$Action = New-ScheduledTaskAction -Execute "PowerShell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"C:\Scripts\PowerShell-XMLA\Invoke-PowerBIXmlaCommand.ps1`" -WorkspaceName `"Finance Workspace`" -DatasetName `"Financial Model`" -TmslFile `"C:\Scripts\PowerShell-XMLA\examples\refresh-full-table.tmsl.json`" -TenantId `"$env:POWERBI_TENANT_ID`" -ClientId `"$env:POWERBI_CLIENT_ID`" -ClientSecret `"$env:POWERBI_CLIENT_SECRET`""

$Trigger = New-ScheduledTaskTrigger -Daily -At 2am

Register-ScheduledTask -TaskName "PowerBI-DailyRefresh" `
    -Action $Action `
    -Trigger $Trigger `
    -User "SYSTEM"
```

---

## Troubleshooting

### Error: "MSAL.PS module is not installed"

**Solution:**
```powershell
Install-Module -Name MSAL.PS -Force
```

### Error: "Failed to acquire access token"

**Common causes:**
- Incorrect Tenant ID, Client ID, or Client Secret
- Service principal doesn't exist in Azure AD
- Service principal not enabled in Power BI tenant settings

**Check:**
```bash
# Verify service principal exists
az ad sp list --display-name "PowerBI-XMLA-Automation"
```

### Error: "Access denied" when connecting to XMLA

**Solution:**
- Add service principal to workspace as **Admin** or **Member**
- Grant **Build** permission on dataset
- Ensure XMLA endpoint is enabled (Premium/PPU/Embedded required)

### Error: "Required assembly not found"

**Solution:**
Install SQL Server 2022 Client Libraries from the link above.

---

## What's Different from Original Script

| Original Script | New Script |
|----------------|------------|
| Interactive login required | Automatic service principal login |
| Manual authentication | OAuth token authentication |
| Parameters: 3 | Parameters: 6 (added auth params) |
| `$server.Connect($Conn)` | `$server.Connect($ConnectionString)` with token |

**Key Addition:**
```powershell
# NEW: Service principal authentication section
$TokenResponse = Get-MsalToken `
    -ClientId $ClientId `
    -ClientSecret $SecureClientSecret `
    -TenantId $TenantId `
    -Scopes "$Resource/.default"

$AccessToken = $TokenResponse.AccessToken

# NEW: Connection with token
$ConnectionString = "DataSource=$Conn;Password=$AccessToken"
$server.Connect($ConnectionString)
```

Everything else remains exactly the same as the original script.

---

## Security Best Practices

1. **Never hardcode credentials** in the script
2. **Use environment variables** or secure credential storage
3. **Rotate secrets regularly** (every 90 days recommended)
4. **Limit service principal permissions** to only what's needed
5. **Monitor usage** in Azure AD sign-in logs

---

## Resources

- [Power BI XMLA Documentation](https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-connect-tools)
- [TMSL Reference](https://learn.microsoft.com/en-us/analysis-services/tmsl/tabular-model-scripting-language-tmsl-reference)
- [MSAL.PS Module](https://github.com/AzureAD/MSAL.PS)

---

**Version:** 2.0.0 (with service principal authentication)
