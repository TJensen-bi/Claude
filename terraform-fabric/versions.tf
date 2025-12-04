terraform {
  required_version = ">= 1.8.0"

  required_providers {
    fabric = {
      source  = "microsoft/fabric"
      version = "~> 1.6"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }

  # Uncomment and configure for remote state
  # backend "azurerm" {
  #   resource_group_name  = "terraform-state-rg"
  #   storage_account_name = "tfstate"
  #   container_name       = "tfstate"
  #   key                  = "fabric.terraform.tfstate"
  # }
}

provider "fabric" {
  # Authentication is handled via environment variables or Azure CLI
  # FABRIC_TENANT_ID
  # FABRIC_CLIENT_ID
  # FABRIC_CLIENT_SECRET
  # Or use managed identity in Azure pipelines
}

provider "azurerm" {
  features {}
  # Additional Azure resources if needed
}
