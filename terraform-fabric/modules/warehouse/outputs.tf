output "id" {
  description = "The ID of the warehouse"
  value       = fabric_warehouse.this.id
}

output "display_name" {
  description = "The display name of the warehouse"
  value       = fabric_warehouse.this.display_name
}

output "workspace_id" {
  description = "The workspace ID of the warehouse"
  value       = fabric_warehouse.this.workspace_id
}

output "description" {
  description = "The description of the warehouse"
  value       = fabric_warehouse.this.description
}
