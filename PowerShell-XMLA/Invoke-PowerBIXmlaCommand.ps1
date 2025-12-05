<#
.SYNOPSIS
    Execute TMSL commands against Power BI XMLA endpoint using Service Principal authentication.

.DESCRIPTION
    This script provides secure, automated execution of TMSL (Tabular Model Scripting Language)
    commands against Power BI semantic models via the XMLA endpoint. It uses service principal
    authentication to eliminate the need for interactive login.

.PARAMETER WorkspaceName
    The name of the Power BI workspace containing the dataset.

.PARAMETER DatasetName
    The name of the dataset (for logging purposes only - not used in connection).

.PARAMETER TmslFile
    Path to the TMSL JSON file to execute.

.PARAMETER ConfigFile
    Path to the configuration file containing authentication settings. If not specified,
    uses Azure Key Vault credentials.

.PARAMETER TenantId
    Azure AD Tenant ID (overrides config file).

.PARAMETER ClientId
    Service Principal Application (Client) ID (overrides config file).

.PARAMETER ClientSecret
    Service Principal Client Secret (overrides config file).

.PARAMETER KeyVaultName
    Azure Key Vault name for retrieving secrets (default: tfa-kv-auth-DAP-0001).

.PARAMETER LogPath
    Path to the log file (default: C:\Scripts\PowerBi\Logs\tmsl_log.txt).

.PARAMETER UseKeyVault
    Switch to use Azure Key Vault for credentials instead of parameters or config file.

.EXAMPLE
    # Using parameters
    .\Invoke-PowerBIXmlaCommand.ps1 -WorkspaceName "Finance Workspace" `
        -DatasetName "Financial Model" `
        -TmslFile "C:\Scripts\PowerBi\tmsl\refresh.json" `
        -TenantId "your-tenant-id" `
        -ClientId "your-client-id" `
        -ClientSecret "your-secret"

.EXAMPLE
    # Using config file
    .\Invoke-PowerBIXmlaCommand.ps1 -WorkspaceName "Finance Workspace" `
        -DatasetName "Financial Model" `
        -TmslFile "C:\Scripts\PowerBi\tmsl\refresh.json" `
        -ConfigFile "C:\Scripts\PowerBi\config\config.json"

.EXAMPLE
    # Using Azure Key Vault
    .\Invoke-PowerBIXmlaCommand.ps1 -WorkspaceName "Finance Workspace" `
        -DatasetName "Financial Model" `
        -TmslFile "C:\Scripts\PowerBi\tmsl\refresh.json" `
        -UseKeyVault `
        -KeyVaultName "tfa-kv-auth-DAP-0001"

.NOTES
    Author: TJensen-bi
    Version: 2.0.0
    Requires:
        - MSAL.PS PowerShell module (Install-Module MSAL.PS)
        - SQL Server 2022 Client Libraries (Analysis Services DLLs)
        - Service Principal with proper Power BI permissions
        - For Key Vault: Az.KeyVault module and appropriate access

.LINK
    https://github.com/TJensen-bi/Claude
#>

[CmdletBinding(DefaultParameterSetName = 'Parameters')]
param(
    [Parameter(Mandatory = $true)]
    [string]$WorkspaceName,

    [Parameter(Mandatory = $true)]
    [string]$DatasetName,

    [Parameter(Mandatory = $true)]
    [ValidateScript({ Test-Path $_ -PathType Leaf })]
    [string]$TmslFile,

    [Parameter(ParameterSetName = 'ConfigFile')]
    [ValidateScript({ Test-Path $_ -PathType Leaf })]
    [string]$ConfigFile,

    [Parameter(ParameterSetName = 'Parameters')]
    [string]$TenantId,

    [Parameter(ParameterSetName = 'Parameters')]
    [string]$ClientId,

    [Parameter(ParameterSetName = 'Parameters')]
    [string]$ClientSecret,

    [Parameter(ParameterSetName = 'KeyVault')]
    [switch]$UseKeyVault,

    [Parameter(ParameterSetName = 'KeyVault')]
    [string]$KeyVaultName = "tfa-kv-auth-DAP-0001",

    [string]$LogPath = "C:\Scripts\PowerBi\Logs\tmsl_log.txt",

    [int]$TimeoutSeconds = 300
)

#Requires -Version 5.1

# ============================================
# STRICT MODE AND ERROR HANDLING
# ============================================
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

# ============================================
# LOGGING FUNCTIONS
# ============================================
function Initialize-LogFile {
    param([string]$Path)

    $LogDir = Split-Path -Path $Path -Parent
    if (-not (Test-Path $LogDir)) {
        New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
    }

    # Archive old log if it exists and is > 5MB
    if (Test-Path $Path) {
        $LogSize = (Get-Item $Path).Length
        if ($LogSize -gt 5MB) {
            $ArchivePath = "$Path.$(Get-Date -Format 'yyyyMMdd_HHmmss').old"
            Move-Item -Path $Path -Destination $ArchivePath -Force
        }
    }

    Clear-Content $Path -ErrorAction SilentlyContinue
}

function Write-Log {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Message,

        [Parameter(Mandatory = $false)]
        [ValidateSet('INFO', 'WARNING', 'ERROR', 'SUCCESS', 'DEBUG')]
        [string]$Level = 'INFO'
    )

    $Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $LogMessage = "$Timestamp [$Level] $Message"

    # Console output with colors
    switch ($Level) {
        'ERROR'   { Write-Host $LogMessage -ForegroundColor Red }
        'WARNING' { Write-Host $LogMessage -ForegroundColor Yellow }
        'SUCCESS' { Write-Host $LogMessage -ForegroundColor Green }
        'DEBUG'   { Write-Host $LogMessage -ForegroundColor Gray }
        default   { Write-Host $LogMessage -ForegroundColor White }
    }

    # File output
    Add-Content -Path $LogPath -Value $LogMessage
}

# ============================================
# CREDENTIAL MANAGEMENT
# ============================================
function Get-ServicePrincipalCredentials {
    param(
        [string]$ConfigFilePath,
        [string]$VaultName,
        [bool]$UseVault,
        [string]$DirectTenantId,
        [string]$DirectClientId,
        [string]$DirectClientSecret
    )

    Write-Log "Retrieving service principal credentials..." -Level INFO

    # Priority 1: Direct parameters
    if ($DirectTenantId -and $DirectClientId -and $DirectClientSecret) {
        Write-Log "Using credentials from parameters" -Level DEBUG
        return @{
            TenantId     = $DirectTenantId
            ClientId     = $DirectClientId
            ClientSecret = $DirectClientSecret
        }
    }

    # Priority 2: Azure Key Vault
    if ($UseVault) {
        Write-Log "Retrieving credentials from Azure Key Vault: $VaultName" -Level INFO

        try {
            # Check if Az.KeyVault module is available
            if (-not (Get-Module -ListAvailable -Name Az.KeyVault)) {
                throw "Az.KeyVault module is not installed. Install with: Install-Module Az.KeyVault"
            }

            Import-Module Az.KeyVault -ErrorAction Stop

            # Retrieve secrets
            $TenantIdSecret = Get-AzKeyVaultSecret -VaultName $VaultName -Name "tenant-id" -AsPlainText
            $ClientIdSecret = Get-AzKeyVaultSecret -VaultName $VaultName -Name "ta-DAP-SPrincipal01-id" -AsPlainText
            $ClientSecretValue = Get-AzKeyVaultSecret -VaultName $VaultName -Name "ta-DAP-SPrincipal01-secret" -AsPlainText

            if (-not $TenantIdSecret -or -not $ClientIdSecret -or -not $ClientSecretValue) {
                throw "One or more secrets are missing or empty in Key Vault"
            }

            Write-Log "Successfully retrieved credentials from Key Vault" -Level SUCCESS

            return @{
                TenantId     = $TenantIdSecret
                ClientId     = $ClientIdSecret
                ClientSecret = $ClientSecretValue
            }
        }
        catch {
            Write-Log "Failed to retrieve credentials from Key Vault: $($_.Exception.Message)" -Level ERROR
            throw
        }
    }

    # Priority 3: Config file
    if ($ConfigFilePath) {
        Write-Log "Loading credentials from config file: $ConfigFilePath" -Level INFO

        try {
            $Config = Get-Content -Path $ConfigFilePath -Raw | ConvertFrom-Json

            $TenantId = $Config.authentication.tenant_id
            $ClientId = $Config.authentication.client_id
            $ClientSecret = $Config.authentication.client_secret

            if (-not $TenantId -or -not $ClientId -or -not $ClientSecret) {
                throw "Config file is missing required authentication fields"
            }

            Write-Log "Successfully loaded credentials from config file" -Level SUCCESS

            return @{
                TenantId     = $TenantId
                ClientId     = $ClientId
                ClientSecret = $ClientSecret
            }
        }
        catch {
            Write-Log "Failed to load config file: $($_.Exception.Message)" -Level ERROR
            throw
        }
    }

    throw "No valid credential source provided. Use -TenantId/-ClientId/-ClientSecret, -ConfigFile, or -UseKeyVault"
}

# ============================================
# AUTHENTICATION
# ============================================
function Get-PowerBIXmlaToken {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TenantId,

        [Parameter(Mandatory = $true)]
        [string]$ClientId,

        [Parameter(Mandatory = $true)]
        [string]$ClientSecret
    )

    Write-Log "Acquiring OAuth token for Power BI XMLA endpoint..." -Level INFO

    try {
        # Check if MSAL.PS module is available
        if (-not (Get-Module -ListAvailable -Name MSAL.PS)) {
            throw "MSAL.PS module is not installed. Install with: Install-Module MSAL.PS"
        }

        Import-Module MSAL.PS -ErrorAction Stop

        # Define the resource/scope for Power BI XMLA
        # This is critical - XMLA endpoint requires this specific resource
        $Resource = "https://analysis.windows.net/powerbi/api"
        $Scope = "$Resource/.default"

        Write-Log "Token request - TenantId: $TenantId, ClientId: $ClientId, Resource: $Resource" -Level DEBUG

        # Convert client secret to secure string
        $SecureClientSecret = ConvertTo-SecureString $ClientSecret -AsPlainText -Force

        # Get access token using MSAL
        $TokenResponse = Get-MsalToken `
            -ClientId $ClientId `
            -ClientSecret $SecureClientSecret `
            -TenantId $TenantId `
            -Scopes $Scope `
            -ErrorAction Stop

        if ([string]::IsNullOrEmpty($TokenResponse.AccessToken)) {
            throw "Token response did not contain an access token"
        }

        # Log token expiration (but not the token itself)
        $ExpiresOn = $TokenResponse.ExpiresOn.LocalDateTime.ToString("yyyy-MM-dd HH:mm:ss")
        Write-Log "Access token acquired successfully (expires: $ExpiresOn)" -Level SUCCESS

        return $TokenResponse.AccessToken
    }
    catch {
        Write-Log "Failed to acquire access token: $($_.Exception.Message)" -Level ERROR

        # Provide helpful troubleshooting info
        if ($_.Exception.Message -like "*AADSTS*") {
            Write-Log "Azure AD error detected. Common causes:" -Level WARNING
            Write-Log "  - Invalid client ID or tenant ID" -Level WARNING
            Write-Log "  - Incorrect client secret" -Level WARNING
            Write-Log "  - Service principal not properly configured in Azure AD" -Level WARNING
            Write-Log "  - Service principal lacks required API permissions" -Level WARNING
        }

        throw
    }
}

# ============================================
# ANALYSIS SERVICES CONNECTION
# ============================================
function Initialize-AnalysisServicesAssemblies {
    Write-Log "Loading Analysis Services assemblies..." -Level INFO

    $RequiredAssemblies = @(
        "C:\Program Files\Microsoft SQL Server\160\SDK\Assemblies\Microsoft.AnalysisServices.Core.dll",
        "C:\Program Files\Microsoft SQL Server\160\SDK\Assemblies\Microsoft.AnalysisServices.dll",
        "C:\Program Files\Microsoft SQL Server\160\SDK\Assemblies\Microsoft.AnalysisServices.Tabular.dll",
        "C:\Program Files\Microsoft.NET\ADOMD.NET\160\Microsoft.AnalysisServices.AdomdClient.dll"
    )

    foreach ($Assembly in $RequiredAssemblies) {
        if (-not (Test-Path $Assembly)) {
            throw "Required assembly not found: $Assembly. Please install SQL Server 2022 Client Libraries."
        }

        try {
            Add-Type -Path $Assembly -ErrorAction Stop
            Write-Log "Loaded: $(Split-Path -Leaf $Assembly)" -Level DEBUG
        }
        catch {
            # Assembly might already be loaded, which is fine
            if ($_.Exception.Message -notlike "*already loaded*") {
                throw "Failed to load assembly $Assembly : $($_.Exception.Message)"
            }
        }
    }

    Write-Log "All Analysis Services assemblies loaded successfully" -Level SUCCESS
}

function Connect-PowerBIXmlaEndpoint {
    param(
        [Parameter(Mandatory = $true)]
        [string]$WorkspaceName,

        [Parameter(Mandatory = $true)]
        [string]$AccessToken
    )

    $ConnectionString = "powerbi://api.powerbi.com/v1.0/myorg/$WorkspaceName"
    Write-Log "Connecting to XMLA endpoint: $ConnectionString" -Level INFO

    try {
        $Server = New-Object Microsoft.AnalysisServices.Tabular.Server
        $Server.ConnectionString = "DataSource=$ConnectionString;Password=$AccessToken"

        # Set timeout
        $Server.ServerTimeout = $TimeoutSeconds * 1000  # Convert to milliseconds

        $Server.Connect()

        Write-Log "Successfully connected to XMLA endpoint" -Level SUCCESS
        Write-Log "Server version: $($Server.Version)" -Level DEBUG
        Write-Log "Server mode: $($Server.ServerMode)" -Level DEBUG

        return $Server
    }
    catch {
        Write-Log "Failed to connect to XMLA endpoint: $($_.Exception.Message)" -Level ERROR

        # Provide helpful troubleshooting info
        Write-Log "Common connection issues:" -Level WARNING
        Write-Log "  - Workspace name is incorrect or doesn't exist" -Level WARNING
        Write-Log "  - Service principal doesn't have access to the workspace" -Level WARNING
        Write-Log "  - XMLA read-write is not enabled for the workspace" -Level WARNING
        Write-Log "  - Service principal is not enabled in Power BI tenant settings" -Level WARNING

        throw
    }
}

# ============================================
# TMSL EXECUTION
# ============================================
function Invoke-TmslCommand {
    param(
        [Parameter(Mandatory = $true)]
        [Microsoft.AnalysisServices.Tabular.Server]$Server,

        [Parameter(Mandatory = $true)]
        [string]$TmslFilePath
    )

    Write-Log "Loading TMSL file: $TmslFilePath" -Level INFO

    try {
        $TmslJson = Get-Content -Path $TmslFilePath -Raw -ErrorAction Stop

        # Validate JSON
        try {
            $null = $TmslJson | ConvertFrom-Json
        }
        catch {
            throw "TMSL file contains invalid JSON: $($_.Exception.Message)"
        }

        Write-Log "TMSL file loaded and validated successfully" -Level SUCCESS
        Write-Log "TMSL content preview (first 200 chars): $($TmslJson.Substring(0, [Math]::Min(200, $TmslJson.Length)))..." -Level DEBUG
    }
    catch {
        Write-Log "Failed to load TMSL file: $($_.Exception.Message)" -Level ERROR
        throw
    }

    Write-Log "Executing TMSL command..." -Level INFO
    $StartTime = Get-Date

    try {
        $Response = $Server.Execute($TmslJson)

        $Duration = (Get-Date) - $StartTime
        Write-Log "TMSL execution completed in $($Duration.TotalSeconds) seconds" -Level SUCCESS

        # Process server messages
        if ($Response.Messages -and $Response.Messages.Count -gt 0) {
            Write-Log "Server returned $($Response.Messages.Count) message(s):" -Level INFO

            foreach ($Message in $Response.Messages) {
                $MessageLevel = switch ($Message.Severity) {
                    'Error'   { 'ERROR' }
                    'Warning' { 'WARNING' }
                    default   { 'INFO' }
                }

                Write-Log "  [SERVER] $($Message.Description)" -Level $MessageLevel
            }
        }
        else {
            Write-Log "TMSL command executed successfully with no server messages" -Level SUCCESS
        }

        return $Response
    }
    catch {
        $Duration = (Get-Date) - $StartTime
        Write-Log "TMSL execution failed after $($Duration.TotalSeconds) seconds: $($_.Exception.Message)" -Level ERROR

        # Try to extract more detailed error information
        if ($_.Exception.InnerException) {
            Write-Log "Inner exception: $($_.Exception.InnerException.Message)" -Level ERROR
        }

        throw
    }
}

# ============================================
# MAIN EXECUTION
# ============================================
function Main {
    $ExitCode = 0
    $Server = $null

    try {
        Write-Log "========================================" -Level INFO
        Write-Log "Power BI XMLA Command Execution Started" -Level INFO
        Write-Log "========================================" -Level INFO
        Write-Log "Workspace: $WorkspaceName" -Level INFO
        Write-Log "Dataset: $DatasetName" -Level INFO
        Write-Log "TMSL File: $TmslFile" -Level INFO
        Write-Log "PowerShell Version: $($PSVersionTable.PSVersion)" -Level DEBUG
        Write-Log "User: $env:USERNAME" -Level DEBUG
        Write-Log "Computer: $env:COMPUTERNAME" -Level DEBUG

        # Step 1: Get credentials
        $Credentials = Get-ServicePrincipalCredentials `
            -ConfigFilePath $ConfigFile `
            -VaultName $KeyVaultName `
            -UseVault $UseKeyVault `
            -DirectTenantId $TenantId `
            -DirectClientId $ClientId `
            -DirectClientSecret $ClientSecret

        # Step 2: Acquire OAuth token
        $AccessToken = Get-PowerBIXmlaToken `
            -TenantId $Credentials.TenantId `
            -ClientId $Credentials.ClientId `
            -ClientSecret $Credentials.ClientSecret

        # Step 3: Load Analysis Services assemblies
        Initialize-AnalysisServicesAssemblies

        # Step 4: Connect to XMLA endpoint
        $Server = Connect-PowerBIXmlaEndpoint `
            -WorkspaceName $WorkspaceName `
            -AccessToken $AccessToken

        # Step 5: Execute TMSL command
        $Response = Invoke-TmslCommand `
            -Server $Server `
            -TmslFilePath $TmslFile

        Write-Log "========================================" -Level SUCCESS
        Write-Log "Script completed successfully" -Level SUCCESS
        Write-Log "========================================" -Level SUCCESS
    }
    catch {
        Write-Log "========================================" -Level ERROR
        Write-Log "Script failed with error" -Level ERROR
        Write-Log "Error: $($_.Exception.Message)" -Level ERROR
        Write-Log "Stack trace: $($_.ScriptStackTrace)" -Level DEBUG
        Write-Log "========================================" -Level ERROR
        $ExitCode = 1
    }
    finally {
        # Cleanup
        if ($null -ne $Server) {
            try {
                $Server.Disconnect()
                Write-Log "Disconnected from XMLA endpoint" -Level INFO
            }
            catch {
                Write-Log "Error during disconnect: $($_.Exception.Message)" -Level WARNING
            }
        }

        # Clear sensitive variables
        if ($AccessToken) { Clear-Variable -Name AccessToken -ErrorAction SilentlyContinue }
        if ($Credentials) { Clear-Variable -Name Credentials -ErrorAction SilentlyContinue }
        if ($ClientSecret) { Clear-Variable -Name ClientSecret -ErrorAction SilentlyContinue }
    }

    exit $ExitCode
}

# ============================================
# SCRIPT ENTRY POINT
# ============================================
Initialize-LogFile -Path $LogPath
Main
