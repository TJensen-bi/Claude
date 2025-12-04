resource "fabric_lakehouse" "this" {
  display_name = var.display_name
  description  = var.description
  workspace_id = var.workspace_id
}
