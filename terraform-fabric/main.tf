###########################################
# Microsoft Fabric Workspace Module
###########################################

module "workspace" {
  source   = "./modules/workspace"
  for_each = var.workspaces

  display_name = each.value.display_name
  description  = each.value.description
  capacity_id  = var.capacity_id
  environment  = var.environment

  enable_git_integration = var.enable_git_integration
  git_config             = var.git_config

  tags = merge(
    var.tags,
    {
      workspace = each.key
    }
  )
}

###########################################
# Microsoft Fabric Lakehouse Module
###########################################

module "lakehouse" {
  source   = "./modules/lakehouse"
  for_each = var.lakehouses

  display_name = each.value.display_name
  description  = each.value.description
  workspace_id = each.value.workspace_id
  environment  = var.environment

  tags = merge(
    var.tags,
    {
      lakehouse = each.key
    }
  )

  depends_on = [module.workspace]
}

###########################################
# Microsoft Fabric Warehouse Module
###########################################

module "warehouse" {
  source   = "./modules/warehouse"
  for_each = var.warehouses

  display_name = each.value.display_name
  description  = each.value.description
  workspace_id = each.value.workspace_id
  environment  = var.environment

  tags = merge(
    var.tags,
    {
      warehouse = each.key
    }
  )

  depends_on = [module.workspace]
}

###########################################
# Microsoft Fabric Notebook Module
###########################################

module "notebook" {
  source   = "./modules/notebook"
  for_each = var.notebooks

  display_name = each.value.display_name
  description  = each.value.description
  workspace_id = each.value.workspace_id
  environment  = var.environment

  tags = merge(
    var.tags,
    {
      notebook = each.key
    }
  )

  depends_on = [module.workspace]
}

###########################################
# Workspace Role Assignments
###########################################

resource "fabric_workspace_role_assignment" "this" {
  for_each = var.workspace_role_assignments

  workspace_id = each.value.workspace_id
  principal_id = each.value.principal_id
  role         = each.value.role

  depends_on = [module.workspace]
}
