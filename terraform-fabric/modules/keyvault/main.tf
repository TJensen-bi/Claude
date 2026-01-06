###########################################
# Azure Key Vault for Secret Management
###########################################

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "keyvault" {
  name     = "rg-${var.project_name}-keyvault-${var.environment}"
  location = var.location
  tags     = var.tags
}

resource "azurerm_key_vault" "this" {
  name                = "kv-${var.project_name}-${var.environment}"
  location            = azurerm_resource_group.keyvault.location
  resource_group_name = azurerm_resource_group.keyvault.name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = "standard"

  # Enable soft delete and purge protection for production
  soft_delete_retention_days = var.environment == "prod" ? 90 : 7
  purge_protection_enabled   = var.environment == "prod" ? true : false

  # Enable RBAC authorization (recommended over access policies)
  enable_rbac_authorization = true

  # Network rules - restrict in production
  network_acls {
    default_action             = var.environment == "prod" ? "Deny" : "Allow"
    bypass                     = "AzureServices"
    ip_rules                   = var.allowed_ip_ranges
    virtual_network_subnet_ids = var.allowed_subnet_ids
  }

  tags = var.tags
}

###########################################
# RBAC Role Assignments
###########################################

# Grant the deployment service principal access to manage secrets
resource "azurerm_role_assignment" "deployer_secrets_officer" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = var.deployer_principal_id
}

# Grant the GitHub Actions workflow access to read secrets
resource "azurerm_role_assignment" "github_secrets_user" {
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = var.github_oidc_principal_id
}

# Grant the rotation function access to manage secrets
resource "azurerm_role_assignment" "rotation_function_secrets_officer" {
  count                = var.enable_automatic_rotation ? 1 : 0
  scope                = azurerm_key_vault.this.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = var.rotation_function_principal_id
}

###########################################
# Store Fabric Service Principal Secrets
###########################################

resource "azurerm_key_vault_secret" "fabric_tenant_id" {
  name         = "fabric-tenant-id"
  value        = var.fabric_tenant_id
  key_vault_id = azurerm_key_vault.this.id

  content_type = "text/plain"

  tags = merge(var.tags, {
    purpose = "fabric-authentication"
  })

  depends_on = [azurerm_role_assignment.deployer_secrets_officer]
}

resource "azurerm_key_vault_secret" "fabric_client_id" {
  name         = "fabric-client-id"
  value        = var.fabric_client_id
  key_vault_id = azurerm_key_vault.this.id

  content_type = "text/plain"

  tags = merge(var.tags, {
    purpose = "fabric-authentication"
  })

  depends_on = [azurerm_role_assignment.deployer_secrets_officer]
}

resource "azurerm_key_vault_secret" "fabric_client_secret" {
  name         = "fabric-client-secret"
  value        = var.fabric_client_secret
  key_vault_id = azurerm_key_vault.this.id

  content_type = "text/plain"

  # Set expiration to trigger rotation alerts
  expiration_date = var.secret_expiration_date

  tags = merge(var.tags, {
    purpose          = "fabric-authentication"
    rotation_enabled = var.enable_automatic_rotation ? "true" : "false"
  })

  depends_on = [azurerm_role_assignment.deployer_secrets_officer]
}

###########################################
# Diagnostic Settings
###########################################

resource "azurerm_monitor_diagnostic_setting" "keyvault" {
  count                      = var.log_analytics_workspace_id != null ? 1 : 0
  name                       = "keyvault-diagnostics"
  target_resource_id         = azurerm_key_vault.this.id
  log_analytics_workspace_id = var.log_analytics_workspace_id

  enabled_log {
    category = "AuditEvent"
  }

  enabled_log {
    category = "AzurePolicyEvaluationDetails"
  }

  metric {
    category = "AllMetrics"
  }
}
