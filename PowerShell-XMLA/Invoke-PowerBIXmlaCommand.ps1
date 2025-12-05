param(
    [Parameter(Mandatory=$true)]
    [string]$WorkspaceName,

    [Parameter(Mandatory=$true)]
    [string]$DatasetName,

    [Parameter(Mandatory=$true)]
    [string]$TmslFile,

    [Parameter(Mandatory=$true)]
    [string]$TenantId,

    [Parameter(Mandatory=$true)]
    [string]$ClientId,

    [Parameter(Mandatory=$true)]
    [string]$ClientSecret
)

# ============================================
# LOGGING
# ============================================
$LogFile = "C:\Scripts\PowerBi\tmsl_log.txt"

function Log($Message) {
    $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Add-Content -Path $LogFile -Value "$ts    $Message"
}

# Start log
Clear-Content $LogFile -ErrorAction SilentlyContinue
Log "----- Script started -----"
Log "Workspace: $WorkspaceName"
Log "Dataset: $DatasetName"
Log "TMSL file: $TmslFile"


# ============================================
# SERVICE PRINCIPAL AUTHENTICATION
# ============================================
try {
    Log "Acquiring access token using service principal..."

    # Import MSAL module (install with: Install-Module MSAL.PS)
    Import-Module MSAL.PS -ErrorAction Stop

    # Power BI XMLA endpoint requires this specific resource
    $Resource = "https://analysis.windows.net/powerbi/api"
    $Scope = "$Resource/.default"

    # Convert client secret to secure string
    $SecureClientSecret = ConvertTo-SecureString $ClientSecret -AsPlainText -Force

    # Get access token
    $TokenResponse = Get-MsalToken `
        -ClientId $ClientId `
        -ClientSecret $SecureClientSecret `
        -TenantId $TenantId `
        -Scopes $Scope `
        -ErrorAction Stop

    $AccessToken = $TokenResponse.AccessToken

    if ([string]::IsNullOrEmpty($AccessToken)) {
        throw "Failed to acquire access token"
    }

    Log "Access token acquired successfully"
}
catch {
    Log "ERROR acquiring access token: $($_.Exception.Message)"
    exit 1
}


# ============================================
# LOAD ANALYSIS SERVICES DLLs
# ============================================
try {
    # AMO/TOM DLLs (SQL 2022 client libraries)
    Add-Type -Path "C:\Program Files\Microsoft SQL Server\160\SDK\Assemblies\Microsoft.AnalysisServices.Core.dll"
    Add-Type -Path "C:\Program Files\Microsoft SQL Server\160\SDK\Assemblies\Microsoft.AnalysisServices.dll"
    Add-Type -Path "C:\Program Files\Microsoft SQL Server\160\SDK\Assemblies\Microsoft.AnalysisServices.Tabular.dll"

    # ADOMD.NET client (SQL 2022)
    Add-Type -Path "C:\Program Files\Microsoft.NET\ADOMD.NET\160\Microsoft.AnalysisServices.AdomdClient.dll"

    Log "Loaded AMO/TOM/ADOMD assemblies successfully."
}
catch {
    Log "ERROR loading assemblies: $($_.Exception.Message)"
    exit 1
}


# ============================================
# CONNECT TO XMLA ENDPOINT WITH SERVICE PRINCIPAL
# ============================================
$Conn = "powerbi://api.powerbi.com/v1.0/myorg/$WorkspaceName"
Log "Connecting to XMLA endpoint: $Conn"

try {
    $server = New-Object Microsoft.AnalysisServices.Tabular.Server

    # Use access token for authentication
    $ConnectionString = "DataSource=$Conn;Password=$AccessToken"
    $server.Connect($ConnectionString)

    Log "Connected successfully using service principal."
}
catch {
    Log "ERROR connecting to XMLA endpoint: $($_.Exception.Message)"
    exit 1
}


# ============================================
# LOAD TMSL JSON FILE
# ============================================
try {
    $TmslJson = Get-Content -Path $TmslFile -Raw
    Log "Loaded TMSL file successfully."
}
catch {
    Log "ERROR loading TMSL file: $($_.Exception.Message)"
    $server.Disconnect()
    exit 1
}


# ============================================
# EXECUTE TMSL COMMAND
# ============================================
try {
    Log "Executing TMSL command..."
    $response = $server.Execute($TmslJson)

    # Log any messages from the server
    if ($response.Messages) {
        foreach ($msg in $response.Messages) {
            Log ("SERVER: " + $msg.Description)
        }
    }

    Log "TMSL execution completed successfully."
}
catch {
    Log "ERROR executing TMSL: $($_.Exception.Message)"
    $server.Disconnect()
    exit 1
}


# ============================================
# CLEANUP
# ============================================
$server.Disconnect()
Log "Disconnected from XMLA."
Log "----- Script finished successfully -----"

exit 0
