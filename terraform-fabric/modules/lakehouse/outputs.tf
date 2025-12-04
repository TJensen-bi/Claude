output "id" {
  description = "The ID of the lakehouse"
  value       = fabric_lakehouse.this.id
}

output "display_name" {
  description = "The display name of the lakehouse"
  value       = fabric_lakehouse.this.display_name
}

output "workspace_id" {
  description = "The workspace ID of the lakehouse"
  value       = fabric_lakehouse.this.workspace_id
}

output "description" {
  description = "The description of the lakehouse"
  value       = fabric_lakehouse.this.description
}
