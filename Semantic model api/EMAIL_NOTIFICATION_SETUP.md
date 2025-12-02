# Email Notification Setup Guide

This guide explains how to configure email notifications for the Power BI Partition Refresh script.

## Overview

The script uses **Microsoft Graph API** to send email notifications via Microsoft 365/Outlook accounts. Notifications are sent when:
- Refresh completes successfully
- Refresh fails
- A fatal error occurs during execution

## Prerequisites

1. An Azure Active Directory (Entra ID) app registration with Mail.Send permissions
2. Access to Azure Key Vault to store credentials
3. Microsoft 365/Outlook account for sending emails

## Step 1: Create Azure AD App Registration

1. Go to Azure Portal → Azure Active Directory → App registrations
2. Click "New registration"
3. Name it (e.g., "PowerBI-Email-Notifier")
4. Select "Accounts in this organizational directory only"
5. Click "Register"

### Configure API Permissions

1. Go to "API permissions" in your app registration
2. Click "Add a permission" → "Microsoft Graph" → "Application permissions"
3. Add the following permission:
   - `Mail.Send` (Send mail as any user)
4. Click "Grant admin consent" for your organization

### Create Client Secret

1. Go to "Certificates & secrets"
2. Click "New client secret"
3. Add a description and expiration period
4. Copy the secret value (you won't be able to see it again)

### Get Application Details

Note down these values from the "Overview" page:
- Application (client) ID
- Directory (tenant) ID

## Step 2: Configure Mailbox Permissions

The app needs permission to send emails on behalf of a user or shared mailbox:

### Option A: Using a User Account
1. Go to Azure AD → Users
2. Find the user account that will send emails
3. The app will send emails as this user

### Option B: Using a Shared Mailbox (Recommended)
1. Create a shared mailbox in Microsoft 365 Admin Center
2. The app will send emails from this shared mailbox

**Note:** The sender email should be configured in Key Vault (see Step 3)

## Step 3: Add Secrets to Azure Key Vault

Add the following secrets to your Azure Key Vault (`tfa-kv-auth-DAP-0001`):

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `pbi-notification-recipients` | Comma-separated list of email recipients | `user1@company.com,user2@company.com` |
| `pbi-email-client-id` | Application (client) ID from app registration | `12345678-1234-1234-1234-123456789abc` |
| `pbi-email-client-secret` | Client secret value from app registration | `abc123~XYZ...` |
| `pbi-email-tenant-id` | Directory (tenant) ID | `87654321-4321-4321-4321-210987654321` |
| `pbi-sender-email` | Email address that will send notifications | `powerbi-alerts@company.com` |

### Adding Secrets via Azure Portal

1. Go to Azure Portal → Key Vaults → `tfa-kv-auth-DAP-0001`
2. Click "Secrets" → "Generate/Import"
3. Add each secret with its corresponding value

### Adding Secrets via Azure CLI

```bash
# Set your Key Vault name
VAULT_NAME="tfa-kv-auth-DAP-0001"

# Add email notification secrets
az keyvault secret set --vault-name $VAULT_NAME --name "pbi-notification-recipients" --value "user1@company.com,user2@company.com"
az keyvault secret set --vault-name $VAULT_NAME --name "pbi-email-client-id" --value "YOUR_CLIENT_ID"
az keyvault secret set --vault-name $VAULT_NAME --name "pbi-email-client-secret" --value "YOUR_CLIENT_SECRET"
az keyvault secret set --vault-name $VAULT_NAME --name "pbi-email-tenant-id" --value "YOUR_TENANT_ID"
az keyvault secret set --vault-name $VAULT_NAME --name "pbi-sender-email" --value "powerbi-alerts@company.com"
```

## Step 4: Test the Configuration

Run the script to test email notifications:

```python
python powerbi_partition_refresh.py
```

The script will:
1. Retrieve email configuration from Key Vault
2. Execute the refresh workflow
3. Send an email notification with the results

## Disabling Email Notifications

To disable email notifications temporarily, modify the `main()` function:

```python
# Disable notifications
email_notifier = EmailNotifier(enable_notifications=False)
```

Or remove the `email_notifier` parameter from the workflow call:

```python
manager.safe_refresh_workflow(tables_and_partitions=tables_to_refresh)
```

## Email Notification Content

Notifications include:
- **Subject**: Status (Success/Failed) with timestamp
- **Body** (HTML formatted):
  - Status indicator (✓ or ✗)
  - Timestamp of execution
  - Status message
  - List of tables/partitions refreshed
  - Error details (for failures)

### Example Email Preview

**Success:**
```
Subject: Power BI Refresh Success - 2025-12-02 14:30

✓ Power BI Refresh Success
Timestamp: 2025-12-02 14:30:15

Status Message: Refresh triggered successfully

Tables/Partitions Refreshed:
• Finanspostering - All partitions
```

**Failure:**
```
Subject: Power BI Refresh Failed - 2025-12-02 14:30

✗ Power BI Refresh Failed
Timestamp: 2025-12-02 14:30:15

Status Message: HTTP error while triggering refresh

Error Details:
401 - Unauthorized: Invalid credentials
```

## Troubleshooting

### Email Not Sending

1. **Check Key Vault secrets**: Ensure all required secrets are present and correct
2. **Verify app permissions**: Ensure Mail.Send permission is granted with admin consent
3. **Check logs**: Review the script logs for error messages
4. **Test Graph API**: Try calling Graph API manually to verify credentials

### Permission Errors (401/403)

- Verify the app has Mail.Send permission
- Ensure admin consent was granted
- Check that the client secret hasn't expired
- Verify the tenant ID is correct

### Recipients Not Receiving Emails

- Check spam/junk folders
- Verify email addresses are correct in Key Vault
- Ensure the sender email is valid and has an active mailbox
- Check Exchange Online message trace for delivery issues

## Security Best Practices

1. **Rotate secrets regularly**: Update the client secret before expiration
2. **Use shared mailbox**: Avoid using personal email accounts for sending
3. **Limit recipients**: Only add necessary recipients to minimize exposure
4. **Monitor usage**: Review Key Vault access logs and email sending activity
5. **Least privilege**: Only grant Mail.Send permission, not broader permissions

## Additional Resources

- [Microsoft Graph API - Send Mail](https://learn.microsoft.com/en-us/graph/api/user-sendmail)
- [Azure Key Vault documentation](https://learn.microsoft.com/en-us/azure/key-vault/)
- [App registration and permissions](https://learn.microsoft.com/en-us/azure/active-directory/develop/quickstart-register-app)
