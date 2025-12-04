resource "fabric_notebook" "this" {
  display_name = var.display_name
  description  = var.description
  workspace_id = var.workspace_id
}
