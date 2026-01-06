output "function_app_id" {
  description = "The ID of the Function App"
  value       = azurerm_linux_function_app.rotation.id
}

output "function_app_name" {
  description = "The name of the Function App"
  value       = azurerm_linux_function_app.rotation.name
}

output "function_app_principal_id" {
  description = "The principal ID of the Function App's managed identity"
  value       = azurerm_linux_function_app.rotation.identity[0].principal_id
}

output "function_app_default_hostname" {
  description = "The default hostname of the Function App"
  value       = azurerm_linux_function_app.rotation.default_hostname
}

output "resource_group_name" {
  description = "The name of the resource group"
  value       = azurerm_resource_group.rotation.name
}

output "application_insights_connection_string" {
  description = "Application Insights connection string for monitoring"
  value       = azurerm_application_insights.rotation.connection_string
  sensitive   = true
}
