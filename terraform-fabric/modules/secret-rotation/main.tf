###########################################
# Azure Function for Secret Rotation
###########################################

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "rotation" {
  name     = "rg-${var.project_name}-rotation-${var.environment}"
  location = var.location
  tags     = var.tags
}

###########################################
# Storage Account for Function App
###########################################

resource "azurerm_storage_account" "function" {
  name                     = "st${var.project_name}rot${var.environment}"
  resource_group_name      = azurerm_resource_group.rotation.name
  location                 = azurerm_resource_group.rotation.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  min_tls_version          = "TLS1_2"

  tags = var.tags
}

###########################################
# App Service Plan (Consumption)
###########################################

resource "azurerm_service_plan" "rotation" {
  name                = "asp-${var.project_name}-rotation-${var.environment}"
  resource_group_name = azurerm_resource_group.rotation.name
  location            = azurerm_resource_group.rotation.location
  os_type             = "Linux"
  sku_name            = "Y1" # Consumption plan

  tags = var.tags
}

###########################################
# Application Insights
###########################################

resource "azurerm_application_insights" "rotation" {
  name                = "appi-${var.project_name}-rotation-${var.environment}"
  location            = azurerm_resource_group.rotation.location
  resource_group_name = azurerm_resource_group.rotation.name
  application_type    = "web"

  tags = var.tags
}

###########################################
# Function App with Managed Identity
###########################################

resource "azurerm_linux_function_app" "rotation" {
  name                = "func-${var.project_name}-rotation-${var.environment}"
  resource_group_name = azurerm_resource_group.rotation.name
  location            = azurerm_resource_group.rotation.location

  storage_account_name       = azurerm_storage_account.function.name
  storage_account_access_key = azurerm_storage_account.function.primary_access_key
  service_plan_id            = azurerm_service_plan.rotation.id

  # Enable system-assigned managed identity
  identity {
    type = "SystemAssigned"
  }

  site_config {
    application_stack {
      python_version = "3.11"
    }

    application_insights_connection_string = azurerm_application_insights.rotation.connection_string
    application_insights_key               = azurerm_application_insights.rotation.instrumentation_key
  }

  app_settings = {
    # Key Vault configuration
    KEY_VAULT_NAME = var.key_vault_name

    # Service principal to rotate (the Fabric SP)
    FABRIC_SP_APP_ID = var.fabric_sp_app_id

    # Rotation settings
    SECRET_VALIDITY_DAYS   = var.secret_validity_days
    ROTATION_DAYS_BEFORE   = var.rotation_days_before
    SECRET_NAME_IN_KEYVAULT = "fabric-client-secret"

    # Azure configuration
    AZURE_TENANT_ID = data.azurerm_client_config.current.tenant_id

    # Runtime configuration
    FUNCTIONS_WORKER_RUNTIME       = "python"
    AzureWebJobsFeatureFlags       = "EnableWorkerIndexing"
    SCM_DO_BUILD_DURING_DEPLOYMENT = "true"
  }

  tags = var.tags
}

###########################################
# Grant Function App permissions
###########################################

# Permission to manage secrets in Key Vault
resource "azurerm_role_assignment" "function_keyvault_secrets" {
  scope                = var.key_vault_id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = azurerm_linux_function_app.rotation.identity[0].principal_id
}

# Permission to manage app credentials in Azure AD
# Note: This requires Microsoft Graph API permissions set manually:
# - Application.ReadWrite.All or Application.ReadWrite.OwnedBy

###########################################
# Event Grid Subscription for Expiration Events
###########################################

resource "azurerm_eventgrid_system_topic" "keyvault" {
  name                   = "evgt-${var.project_name}-keyvault-${var.environment}"
  resource_group_name    = azurerm_resource_group.rotation.name
  location               = var.location
  source_arm_resource_id = var.key_vault_id
  topic_type             = "Microsoft.KeyVault.vaults"

  tags = var.tags
}

resource "azurerm_eventgrid_system_topic_event_subscription" "secret_expiring" {
  name                = "secret-near-expiry-subscription"
  system_topic        = azurerm_eventgrid_system_topic.keyvault.name
  resource_group_name = azurerm_resource_group.rotation.name

  # Filter for secret near expiry events
  included_event_types = [
    "Microsoft.KeyVault.SecretNearExpiry"
  ]

  # Only trigger for our Fabric client secret
  advanced_filter {
    string_contains {
      key    = "subject"
      values = ["fabric-client-secret"]
    }
  }

  azure_function_endpoint {
    function_id                       = "${azurerm_linux_function_app.rotation.id}/functions/rotate_secret"
    max_events_per_batch              = 1
    preferred_batch_size_in_kilobytes = 64
  }
}

###########################################
# Timer Trigger for Proactive Rotation
###########################################

# The function also includes a timer trigger that runs daily
# to check and rotate secrets proactively
