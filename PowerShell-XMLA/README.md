# Power BI XMLA Automation with Service Principal

Production-ready PowerShell solution for automating Power BI semantic model operations via XMLA endpoint using service principal authentication.

## 🎯 Overview

This solution eliminates the need for interactive login when executing TMSL (Tabular Model Scripting Language) commands against Power BI datasets. It uses Azure AD service principal authentication to enable fully automated, secure operations on Power BI semantic models.

### Key Features

✅ **Secure Service Principal Authentication** - No interactive login required
✅ **Multiple Credential Sources** - Support for direct parameters, config files, and Azure Key Vault
✅ **Comprehensive Error Handling** - Detailed logging and troubleshooting information
✅ **Production-Ready** - Timeout protection, retry logic, and proper cleanup
✅ **Flexible TMSL Execution** - Support for all TMSL operations (refresh, process, etc.)
✅ **Enterprise-Grade Logging** - Color-coded console output and detailed log files

---

## 📋 Prerequisites

### Required Software

1. **PowerShell 5.1 or higher**
   ```powershell
   $PSVersionTable.PSVersion
   ```

2. **MSAL.PS PowerShell Module**
   ```powershell
   Install-Module -Name MSAL.PS -Scope CurrentUser -Force
   ```

3. **SQL Server 2022 Client Libraries** (Analysis Services DLLs)
   - Download: [Microsoft SQL Server 2022 Feature Pack](https://www.microsoft.com/en-us/download/details.aspx?id=104781)
   - Required components:
     - Microsoft Analysis Services (AMO)
     - Microsoft Analysis Services ADOMD.NET

4. **Azure Key Vault Module** (optional, for Key Vault credential storage)
   ```powershell
   Install-Module -Name Az.KeyVault -Scope CurrentUser -Force
   ```

### Azure AD Service Principal Setup

#### 1. Create Service Principal

```bash
# Using Azure CLI
az ad sp create-for-rbac --name "PowerBI-XMLA-Automation" --role Contributor

# Note down the output:
# - appId (Client ID)
# - password (Client Secret)
# - tenant (Tenant ID)
```

Or via Azure Portal:
1. Navigate to **Azure Active Directory** → **App registrations** → **New registration**
2. Name: `PowerBI-XMLA-Automation`
3. Click **Register**
4. Note the **Application (client) ID** and **Directory (tenant) ID**
5. Go to **Certificates & secrets** → **New client secret**
6. Note the **secret value** (shown only once)

#### 2. Configure Power BI Service Principal Access

**Enable Service Principal in Power BI Admin Portal:**

1. Go to [Power BI Admin Portal](https://app.powerbi.com/admin-portal)
2. Navigate to **Tenant settings** → **Developer settings**
3. Enable **"Allow service principals to use Power BI APIs"**
4. Choose **Specific security groups** and add a group containing your service principal
5. Click **Apply**

**Enable XMLA Read-Write:**

1. In Power BI Admin Portal → **Capacity settings**
2. Select your capacity (Premium or Embedded)
3. Under **Workloads**, enable **XMLA Endpoint** and set to **Read Write**

**Important:** XMLA endpoint requires Power BI Premium, Premium Per User, or Embedded capacity.

#### 3. Grant Workspace Access

Add the service principal to your Power BI workspace:

1. Open your workspace in Power BI Service
2. Click **Access**
3. Add your service principal (search by name)
4. Assign **Admin** or **Member** role (required for XMLA write operations)

#### 4. Grant Dataset Permissions

The service principal needs **Build** permission on datasets:

1. Navigate to the dataset settings
2. Go to **Manage permissions**
3. Add the service principal with **Build** permission

---

## 🚀 Quick Start

### Option 1: Using Configuration File (Recommended)

1. **Create configuration file:**

   ```bash
   cp config/config.example.json config/config.json
   ```

2. **Edit `config/config.json` with your credentials:**

   ```json
   {
     "authentication": {
       "tenant_id": "your-tenant-id",
       "client_id": "your-client-id",
       "client_secret": "your-client-secret"
     },
     "powerbi": {
       "workspace_name": "Finance Workspace",
       "dataset_name": "Financial Model"
     }
   }
   ```

3. **Create or customize a TMSL file:**

   ```bash
   cp examples/refresh-full-table.tmsl.json my-refresh.tmsl.json
   ```

   Edit the dataset and table names:
   ```json
   {
     "refresh": {
       "type": "full",
       "objects": [
         {
           "database": "Financial Model",
           "table": "Finanspostering"
         }
       ]
     }
   }
   ```

4. **Run the script:**

   ```powershell
   .\Invoke-PowerBIXmlaCommand.ps1 `
       -WorkspaceName "Finance Workspace" `
       -DatasetName "Financial Model" `
       -TmslFile ".\my-refresh.tmsl.json" `
       -ConfigFile ".\config\config.json"
   ```

### Option 2: Using Azure Key Vault

1. **Store credentials in Key Vault:**

   ```bash
   az keyvault secret set --vault-name "tfa-kv-auth-DAP-0001" --name "tenant-id" --value "your-tenant-id"
   az keyvault secret set --vault-name "tfa-kv-auth-DAP-0001" --name "ta-DAP-SPrincipal01-id" --value "your-client-id"
   az keyvault secret set --vault-name "tfa-kv-auth-DAP-0001" --name "ta-DAP-SPrincipal01-secret" --value "your-client-secret"
   ```

2. **Authenticate to Azure (one-time per session):**

   ```powershell
   Connect-AzAccount
   ```

3. **Run the script:**

   ```powershell
   .\Invoke-PowerBIXmlaCommand.ps1 `
       -WorkspaceName "Finance Workspace" `
       -DatasetName "Financial Model" `
       -TmslFile ".\examples\refresh-full-table.tmsl.json" `
       -UseKeyVault `
       -KeyVaultName "tfa-kv-auth-DAP-0001"
   ```

### Option 3: Using Direct Parameters

```powershell
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-full-table.tmsl.json" `
    -TenantId "your-tenant-id" `
    -ClientId "your-client-id" `
    -ClientSecret "your-client-secret"
```

---

## 📚 Usage Examples

### Refresh Full Table

```powershell
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-full-table.tmsl.json" `
    -ConfigFile ".\config\config.json"
```

### Refresh Specific Partition

```powershell
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-specific-partition.tmsl.json" `
    -ConfigFile ".\config\config.json"
```

### Refresh Multiple Partitions (Transactional)

```powershell
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-multiple-partitions.tmsl.json" `
    -ConfigFile ".\config\config.json"
```

### Recalculate Only (No Data Refresh)

```powershell
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-calculate-only.tmsl.json" `
    -ConfigFile ".\config\config.json"
```

### Custom Log Path and Timeout

```powershell
.\Invoke-PowerBIXmlaCommand.ps1 `
    -WorkspaceName "Finance Workspace" `
    -DatasetName "Financial Model" `
    -TmslFile ".\examples\refresh-full-table.tmsl.json" `
    -ConfigFile ".\config\config.json" `
    -LogPath "D:\Logs\powerbi-refresh.log" `
    -TimeoutSeconds 600
```

---

## ⚙️ Automation & Scheduling

### Windows Task Scheduler

Create a scheduled task to run refreshes automatically:

```powershell
$Action = New-ScheduledTaskAction -Execute "PowerShell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"C:\Scripts\PowerShell-XMLA\Invoke-PowerBIXmlaCommand.ps1`" -WorkspaceName `"Finance Workspace`" -DatasetName `"Financial Model`" -TmslFile `"C:\Scripts\PowerShell-XMLA\examples\refresh-full-table.tmsl.json`" -ConfigFile `"C:\Scripts\PowerShell-XMLA\config\config.json`""

$Trigger = New-ScheduledTaskTrigger -Daily -At 2am

$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

Register-ScheduledTask `
    -TaskName "PowerBI-DailyRefresh" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -User "SYSTEM" `
    -Description "Daily Power BI semantic model refresh"
```

### Azure Automation

1. Create an Automation Account in Azure
2. Import the PowerShell script as a Runbook
3. Configure schedule triggers
4. Use Managed Identity or stored credentials

### Azure Data Factory / Synapse

Execute the script from a pipeline using:
- **Execute Pipeline** activity with a self-hosted integration runtime
- **Web Activity** to trigger Azure Automation runbook

---

## 📖 TMSL Command Reference

### Refresh Types

| Type | Description |
|------|-------------|
| `full` | Complete refresh (data + calculations) |
| `automatic` | Power BI decides the optimal refresh type |
| `dataOnly` | Refresh data only, skip calculations |
| `calculate` | Recalculate only, no data refresh |
| `clearValues` | Clear all values |

### Commit Modes

| Mode | Description |
|------|-------------|
| `transactional` | All-or-nothing (default) - rollback on any failure |
| `partialBatch` | Allow partial success - continue on errors |

### Example TMSL Templates

**Refresh entire dataset:**
```json
{
  "refresh": {
    "type": "full",
    "objects": [
      {
        "database": "YourDatasetName"
      }
    ]
  }
}
```

**Refresh specific table:**
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

**Refresh specific partition:**
```json
{
  "refresh": {
    "type": "full",
    "objects": [
      {
        "database": "YourDatasetName",
        "table": "YourTableName",
        "partition": "2025Q2"
      }
    ]
  }
}
```

**Advanced options:**
```json
{
  "refresh": {
    "type": "full",
    "commitMode": "transactional",
    "maxParallelism": 10,
    "retryCount": 2,
    "objects": [
      {
        "database": "YourDatasetName",
        "table": "YourTableName"
      }
    ]
  }
}
```

---

## 🔧 Troubleshooting

### Common Issues

#### 1. "Failed to acquire access token: AADSTS7000215"

**Error:** Service principal not found in tenant

**Solution:**
- Verify the Tenant ID is correct
- Ensure the service principal exists in Azure AD
- Check that the Client ID matches the service principal

#### 2. "Failed to connect to XMLA endpoint: Access denied"

**Error:** Service principal lacks permissions

**Solution:**
- Add service principal to workspace as Admin or Member
- Enable service principals in Power BI tenant settings
- Grant Build permission on the dataset

#### 3. "MSAL.PS module is not installed"

**Error:** Missing MSAL.PS module

**Solution:**
```powershell
Install-Module -Name MSAL.PS -Scope CurrentUser -Force
Import-Module MSAL.PS
```

#### 4. "Required assembly not found"

**Error:** SQL Server Client Libraries not installed

**Solution:**
- Download and install [SQL Server 2022 Feature Pack](https://www.microsoft.com/en-us/download/details.aspx?id=104781)
- Install both AMO and ADOMD.NET components

#### 5. "TMSL file contains invalid JSON"

**Error:** Malformed TMSL file

**Solution:**
- Validate JSON syntax using a JSON validator
- Ensure all quotes are properly escaped
- Check for trailing commas

#### 6. "XMLA read-write is not enabled"

**Error:** XMLA endpoint is read-only or disabled

**Solution:**
- Requires Power BI Premium, PPU, or Embedded
- Enable XMLA Read-Write in capacity settings
- Wait 5-10 minutes for changes to propagate

#### 7. Key Vault Access Denied

**Error:** Cannot retrieve secrets from Key Vault

**Solution:**
- Ensure you're authenticated: `Connect-AzAccount`
- Verify you have Get permission for secrets
- Check Key Vault firewall settings

---

## 🔒 Security Best Practices

### Credential Storage

1. **Never hardcode credentials** in scripts
2. **Use Azure Key Vault** for production environments
3. **Encrypt config files** if using file-based credentials
4. **Limit service principal permissions** to minimum required
5. **Rotate secrets regularly** (recommended: every 90 days)

### Access Control

1. **Use security groups** for service principal management
2. **Enable conditional access** policies where applicable
3. **Monitor service principal usage** in Azure AD sign-in logs
4. **Implement least privilege** access model

### Logging

1. **Review logs regularly** for suspicious activity
2. **Secure log files** with appropriate file permissions
3. **Implement log rotation** to prevent disk space issues
4. **Consider centralized logging** (Azure Monitor, Splunk, etc.)

---

## 📁 Project Structure

```
PowerShell-XMLA/
├── Invoke-PowerBIXmlaCommand.ps1    # Main script
├── README.md                         # This file
├── config/
│   ├── .gitignore                   # Prevent committing secrets
│   └── config.example.json          # Configuration template
└── examples/
    ├── Example-Usage.ps1            # Usage examples
    ├── refresh-full-table.tmsl.json
    ├── refresh-specific-partition.tmsl.json
    ├── refresh-multiple-partitions.tmsl.json
    ├── refresh-calculate-only.tmsl.json
    └── refresh-data-only.tmsl.json
```

---

## 🆚 Comparison with Python Implementation

This PowerShell solution provides the same functionality as the existing Python implementation (`Semantic model api/`) but is designed for Windows-centric environments.

| Feature | PowerShell | Python |
|---------|-----------|---------|
| Service Principal Auth | ✅ MSAL.PS | ✅ MSAL Python |
| Azure Key Vault | ✅ Az.KeyVault | ✅ azure-keyvault |
| XMLA Endpoint | ✅ Native AMO/TOM | ❌ REST API only |
| Direct TMSL Execution | ✅ Yes | ❌ No (uses REST) |
| Windows Integration | ✅ Excellent | ⚠️ Limited |
| Cross-Platform | ⚠️ Windows only | ✅ Yes |
| Scheduled Tasks | ✅ Task Scheduler | ✅ Cron/Systemd |

**When to use PowerShell:**
- Windows Server environments
- Need direct TMSL execution
- Integration with Windows workflows
- Prefer native Windows authentication

**When to use Python:**
- Cross-platform requirements
- Already using Python ecosystem
- Azure Data Factory/Synapse notebooks
- Need advanced REST API features

---

## 📄 License

This project is part of the TJensen-bi/Claude repository.

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

---

## 📞 Support

For issues and questions:
- Open an issue on [GitHub](https://github.com/TJensen-bi/Claude/issues)
- Review the troubleshooting section above
- Check Power BI XMLA documentation

---

## 🔗 Additional Resources

- [Power BI XMLA Endpoint Documentation](https://learn.microsoft.com/en-us/power-bi/enterprise/service-premium-connect-tools)
- [TMSL Reference](https://learn.microsoft.com/en-us/analysis-services/tmsl/tabular-model-scripting-language-tmsl-reference)
- [MSAL.PS Documentation](https://github.com/AzureAD/MSAL.PS)
- [Analysis Services PowerShell](https://learn.microsoft.com/en-us/analysis-services/powershell/analysis-services-powershell-reference)

---

**Version:** 2.0.0
**Last Updated:** 2025-12-05
**Author:** TJensen-bi
