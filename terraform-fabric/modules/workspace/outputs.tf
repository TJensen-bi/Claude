output "id" {
  description = "The ID of the workspace"
  value       = fabric_workspace.this.id
}

output "display_name" {
  description = "The display name of the workspace"
  value       = fabric_workspace.this.display_name
}

output "capacity_id" {
  description = "The capacity ID of the workspace"
  value       = fabric_workspace.this.capacity_id
}

output "description" {
  description = "The description of the workspace"
  value       = fabric_workspace.this.description
}
