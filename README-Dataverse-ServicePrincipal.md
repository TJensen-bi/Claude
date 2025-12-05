# Service Principal Access to Dataverse Tables

## Overview

This guide explains how to configure service principal (application user) access to specific Dataverse tables, particularly when using Finance & Operations 365 as a data source.

## Can a Service Principal Access Dataverse Tables?

**Yes!** A service principal can access Dataverse tables in Power Apps. This is called **Application User** authentication in Dataverse and is commonly used for server-to-server scenarios.

## Setup Process

### 1. Register an App in Azure AD/Entra ID

- Create an app registration in Azure portal
- Generate a client secret or certificate
- Note the Application (client) ID and Tenant ID

### 2. Create an Application User in Dataverse

- Go to Power Platform Admin Center
- Navigate to your environment → Settings → Users + permissions → Application users
- Click "New app user" and select your Azure AD app registration
- Assign a Business Unit

### 3. Assign Security Roles

- The application user needs appropriate security roles
- Permissions work the same as regular users
- **Important**: Use custom security roles for granular access control

### 4. Authenticate Programmatically

- Use the service principal credentials (client ID + secret/certificate)
- Authenticate against the Dataverse Web API
- Libraries like `msal` (Microsoft Authentication Library) make this straightforward

## Controlling Access to Specific Tables

To grant selective table access to a service principal, use **custom security roles** with granular table-level permissions.

### Steps to Grant Selective Table Access:

#### 1. Create a Custom Security Role

- Go to Power Platform Admin Center → Your environment → Settings → Users + permissions → Security roles
- Click "New role" (or copy an existing role as a starting point)
- Give it a descriptive name (e.g., "F&O Integration Service Principal")

#### 2. Configure Table-Level Permissions

For each table, you can set permissions at different levels:

**Permission Types:**
- Create
- Read
- Write
- Delete
- Append
- Append To
- Assign
- Share

**Scope Levels:**
- None
- User
- Business Unit
- Parent: Child Business Units
- Organization

**Example Configuration:**
- Table A: Read (Organization level), Write (Organization level)
- Table B: Read only (Organization level)
- Table C: No access

#### 3. Assign the Custom Role to the Application User

- After creating the application user (service principal)
- Assign ONLY the custom security role you created
- Remove any default or overly permissive roles

## F&O 365 Integration Context

When using **Finance & Operations as a source**, you typically have one of these scenarios:

### Dual-Write Tables

- F&O data syncs to Dataverse via dual-write
- These appear as regular Dataverse tables (usually prefixed with `msdyn_`)
- You can control access to these tables just like any other Dataverse table
- **Common F&O dual-write tables:**
  - `msdyn_customers`
  - `msdyn_vendors`
  - `msdyn_products`
  - `msdyn_salesorders`
  - And many more...

### Virtual Tables

- F&O entities exposed as virtual tables in Dataverse
- Require additional configuration and the "Microsoft Dynamics 365 virtual entity" privilege
- Access controlled through both Dataverse security roles AND F&O security
- Data remains in F&O, queried in real-time

## Best Practice Security Pattern

Follow this security pattern when configuring service principal access:

1. **Identify exact tables needed** - List them out explicitly
2. **Create custom security role** - Include ONLY those tables
3. **Use minimum required permissions** - Read vs Write (principle of least privilege)
4. **Use Organization scope only if truly needed** - Consider more restrictive scopes
5. **Assign role to service principal** - Remove any excessive default roles
6. **Test with the service principal credentials** - Verify access before production use
7. **Enable auditing** - Track service principal actions on sensitive tables

## Additional Security Considerations

### Column-Level Security
- Use Field Security Profiles to restrict access to specific columns within tables
- Useful for sensitive data like SSN, salary information, etc.

### Hierarchical Security
- Consider if business unit boundaries matter for your use case
- Restrict data access based on organizational hierarchy

### Audit Logging
- Enable auditing on sensitive tables to track service principal actions
- Monitor for unauthorized access attempts
- Review audit logs regularly

## Common Use Cases

- **Automated data integration/ETL processes** - Sync data between systems
- **Backend services** - Services that need to read/write Dataverse data
- **CI/CD pipelines** - Automated deployment and testing
- **Power Automate flows** - Flows using service principal connections
- **F&O to Dataverse synchronization** - Custom integration scenarios

## Authentication Example

```python
from msal import ConfidentialClientApplication
import requests

# Configuration
tenant_id = "your-tenant-id"
client_id = "your-client-id"
client_secret = "your-client-secret"
dataverse_url = "https://your-org.crm.dynamics.com"

# Authenticate
authority = f"https://login.microsoftonline.com/{tenant_id}"
app = ConfidentialClientApplication(
    client_id,
    authority=authority,
    client_credential=client_secret
)

# Get token
scopes = [f"{dataverse_url}/.default"]
result = app.acquire_token_for_client(scopes=scopes)
access_token = result['access_token']

# Call Dataverse API
headers = {
    'Authorization': f'Bearer {access_token}',
    'OData-MaxVersion': '4.0',
    'OData-Version': '4.0',
    'Accept': 'application/json'
}

response = requests.get(
    f"{dataverse_url}/api/data/v9.2/accounts",
    headers=headers
)

print(response.json())
```

## Key Advantages

- **No user dependency** - Access isn't tied to a specific user's credentials
- **Automation-friendly** - Perfect for automated scenarios
- **Secure** - Can be tightly scoped to specific tables and operations
- **Auditable** - All actions can be tracked and monitored
- **Scalable** - Works well for high-volume integration scenarios

## Resources

- [Microsoft Docs: Application users in Dataverse](https://learn.microsoft.com/en-us/power-platform/admin/manage-application-users)
- [Security roles and privileges](https://learn.microsoft.com/en-us/power-platform/admin/security-roles-privileges)
- [Dual-write overview](https://learn.microsoft.com/en-us/dynamics365/fin-ops-core/dev-itpro/data-entities/dual-write/dual-write-overview)
- [Virtual tables for Finance and Operations](https://learn.microsoft.com/en-us/dynamics365/fin-ops-core/dev-itpro/power-platform/virtual-entities-overview)

---

**Last Updated:** 2025-12-05
