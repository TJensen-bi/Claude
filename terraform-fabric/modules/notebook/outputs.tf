output "id" {
  description = "The ID of the notebook"
  value       = fabric_notebook.this.id
}

output "display_name" {
  description = "The display name of the notebook"
  value       = fabric_notebook.this.display_name
}

output "workspace_id" {
  description = "The workspace ID of the notebook"
  value       = fabric_notebook.this.workspace_id
}

output "description" {
  description = "The description of the notebook"
  value       = fabric_notebook.this.description
}
