output "key_vault_id" {
  description = "The ID of the Key Vault"
  value       = azurerm_key_vault.this.id
}

output "key_vault_name" {
  description = "The name of the Key Vault"
  value       = azurerm_key_vault.this.name
}

output "key_vault_uri" {
  description = "The URI of the Key Vault"
  value       = azurerm_key_vault.this.vault_uri
}

output "resource_group_name" {
  description = "The name of the resource group containing the Key Vault"
  value       = azurerm_resource_group.keyvault.name
}

output "fabric_tenant_id_secret_name" {
  description = "Name of the secret containing Fabric tenant ID"
  value       = azurerm_key_vault_secret.fabric_tenant_id.name
}

output "fabric_client_id_secret_name" {
  description = "Name of the secret containing Fabric client ID"
  value       = azurerm_key_vault_secret.fabric_client_id.name
}

output "fabric_client_secret_secret_name" {
  description = "Name of the secret containing Fabric client secret"
  value       = azurerm_key_vault_secret.fabric_client_secret.name
}
