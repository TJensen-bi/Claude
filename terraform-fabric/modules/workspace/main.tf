resource "fabric_workspace" "this" {
  display_name = var.display_name
  description  = var.description
  capacity_id  = var.capacity_id
}

# Git integration (optional)
resource "fabric_workspace_git" "this" {
  count = var.enable_git_integration ? 1 : 0

  workspace_id      = fabric_workspace.this.id
  organization_name = var.git_config.organization_name
  project_name      = var.git_config.project_name
  repository_name   = var.git_config.repository_name
  branch_name       = var.git_config.branch_name
  directory_name    = var.git_config.directory_name
}
