terraform {
  required_version = ">= 1.8.0"

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.6"
    }
  }

  # Configure remote state for dev environment
  # backend "azurerm" {
  #   resource_group_name  = "terraform-state-rg"
  #   storage_account_name = "tfstatedev"
  #   container_name       = "tfstate"
  #   key                  = "dev/fabric.terraform.tfstate"
  # }
}

provider "fabric" {
  # Authentication via environment variables or Azure CLI
}

# Use root module with dev-specific variables
module "fabric" {
  source = "../../"

  environment = var.environment
  location    = var.location
  capacity_id = var.capacity_id

  workspaces                 = var.workspaces
  lakehouses                 = var.lakehouses
  warehouses                 = var.warehouses
  notebooks                  = var.notebooks
  workspace_role_assignments = var.workspace_role_assignments

  tags = merge(
    var.tags,
    {
      environment = "dev"
    }
  )

  enable_git_integration = var.enable_git_integration
  git_config             = var.git_config
}

# Output from dev environment
output "workspace_ids" {
  value = module.fabric.workspace_ids
}

output "lakehouse_ids" {
  value = module.fabric.lakehouse_ids
}

output "warehouse_ids" {
  value = module.fabric.warehouse_ids
}

output "notebook_ids" {
  value = module.fabric.notebook_ids
}
