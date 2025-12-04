output "workspace_ids" {
  description = "Map of workspace names to IDs"
  value = {
    for k, v in module.workspace : k => v.id
  }
}

output "workspace_details" {
  description = "Details of all created workspaces"
  value = {
    for k, v in module.workspace : k => {
      id           = v.id
      display_name = v.display_name
      capacity_id  = v.capacity_id
    }
  }
}

output "lakehouse_ids" {
  description = "Map of lakehouse names to IDs"
  value = {
    for k, v in module.lakehouse : k => v.id
  }
}

output "lakehouse_details" {
  description = "Details of all created lakehouses"
  value = {
    for k, v in module.lakehouse : k => {
      id           = v.id
      display_name = v.display_name
      workspace_id = v.workspace_id
    }
  }
}

output "warehouse_ids" {
  description = "Map of warehouse names to IDs"
  value = {
    for k, v in module.warehouse : k => v.id
  }
}

output "warehouse_details" {
  description = "Details of all created warehouses"
  value = {
    for k, v in module.warehouse : k => {
      id           = v.id
      display_name = v.display_name
      workspace_id = v.workspace_id
    }
  }
}

output "notebook_ids" {
  description = "Map of notebook names to IDs"
  value = {
    for k, v in module.notebook : k => v.id
  }
}

output "notebook_details" {
  description = "Details of all created notebooks"
  value = {
    for k, v in module.notebook : k => {
      id           = v.id
      display_name = v.display_name
      workspace_id = v.workspace_id
    }
  }
}

output "role_assignments" {
  description = "Details of all workspace role assignments"
  value = {
    for k, v in fabric_workspace_role_assignment.this : k => {
      workspace_id = v.workspace_id
      principal_id = v.principal_id
      role         = v.role
    }
  }
}
