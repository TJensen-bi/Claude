# Terraform Microsoft Fabric Infrastructure

A comprehensive, production-ready Terraform project for managing Microsoft Fabric resources following HashiCorp best practices.

## Overview

This project provides a modular, reusable Terraform configuration for deploying and managing Microsoft Fabric resources including workspaces, lakehouses, warehouses, and notebooks across multiple environments.

## Features

- **Modular Architecture**: Reusable modules for each resource type
- **Multi-Environment Support**: Separate configurations for dev and prod environments
- **CI/CD Ready**: Includes GitHub Actions and Azure DevOps pipeline templates
- **HashiCorp Best Practices**: Follows official Terraform style and structure guidelines
- **Type Safety**: Comprehensive variable validation and type constraints
- **Git Integration**: Optional workspace Git integration support
- **Medallion Architecture**: Example configurations for bronze/silver/gold lakehouses

## Project Structure

```
terraform-fabric/
├── README.md                          # This file
├── main.tf                            # Root module orchestration
├── variables.tf                       # Root module variables
├── outputs.tf                         # Root module outputs
├── versions.tf                        # Provider and Terraform version constraints
├── terraform.tfvars.example           # Example variable values
├── azure-pipelines.yml                # Azure DevOps pipeline
├── .github/
│   └── workflows/
│       └── terraform.yml              # GitHub Actions workflow
├── modules/
│   ├── workspace/                     # Workspace module
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── lakehouse/                     # Lakehouse module
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   ├── warehouse/                     # Warehouse module
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   └── outputs.tf
│   └── notebook/                      # Notebook module
│       ├── main.tf
│       ├── variables.tf
│       └── outputs.tf
└── environments/
    ├── dev/                           # Development environment
    │   ├── main.tf
    │   ├── variables.tf
    │   └── terraform.tfvars.example
    └── prod/                          # Production environment
        ├── main.tf
        ├── variables.tf
        └── terraform.tfvars.example
```

## Prerequisites

- [Terraform](https://www.terraform.io/downloads.html) >= 1.8.0
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) (for authentication)
- Microsoft Fabric capacity with appropriate permissions
- Azure subscription (for remote state storage, optional)

## Getting Started

### 1. Authentication

Set up authentication for Microsoft Fabric provider:

```bash
# Option 1: Using environment variables
export FABRIC_TENANT_ID="your-tenant-id"
export FABRIC_CLIENT_ID="your-client-id"
export FABRIC_CLIENT_SECRET="your-client-secret"

# Option 2: Using Azure CLI
az login
```

### 2. Configure Variables

Copy and customize the example tfvars file:

```bash
# For root module
cp terraform.tfvars.example terraform.tfvars

# Or for environment-specific deployment
cp environments/dev/terraform.tfvars.example environments/dev/terraform.tfvars
```

Edit the file and update:
- `capacity_id`: Your Microsoft Fabric capacity ID
- `workspaces`: Workspaces to create
- `lakehouses`: Lakehouses to create
- `warehouses`: Warehouses to create
- Other configuration as needed

### 3. Initialize Terraform

```bash
# For root module deployment
terraform init

# For environment-specific deployment
cd environments/dev
terraform init
```

### 4. Plan and Apply

```bash
# Review planned changes
terraform plan

# Apply changes
terraform apply
```

## Usage Examples

### Basic Workspace and Lakehouse

```hcl
workspaces = {
  "analytics" = {
    display_name = "Analytics Workspace"
    description  = "Workspace for analytics workloads"
  }
}

lakehouses = {
  "bronze" = {
    display_name = "Bronze Lakehouse"
    description  = "Raw data lakehouse"
    workspace_id = module.workspace["analytics"].id
  }
}
```

### Medallion Architecture

```hcl
lakehouses = {
  "bronze" = {
    display_name = "Bronze Lakehouse"
    description  = "Raw data layer"
    workspace_id = module.workspace["data-engineering"].id
  }
  "silver" = {
    display_name = "Silver Lakehouse"
    description  = "Cleansed and curated data"
    workspace_id = module.workspace["data-engineering"].id
  }
  "gold" = {
    display_name = "Gold Lakehouse"
    description  = "Business-ready aggregated data"
    workspace_id = module.workspace["analytics"].id
  }
}
```

### Workspace with Git Integration

```hcl
enable_git_integration = true

git_config = {
  organization_name = "your-org"
  project_name      = "your-project"
  repository_name   = "fabric-workspace"
  branch_name       = "main"
  directory_name    = "workspace-config"
}
```

## CI/CD Pipelines

### GitHub Actions

The included GitHub Actions workflow (`.github/workflows/terraform.yml`) provides:

- **Automatic validation** on pull requests
- **Terraform plan** for both dev and prod environments
- **Automatic apply** on merge to main branch
- **PR comments** with plan results

#### Required GitHub Secrets:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`
- `FABRIC_TENANT_ID`
- `FABRIC_CLIENT_ID`
- `FABRIC_CLIENT_SECRET`
- `FABRIC_CAPACITY_ID`

### Azure DevOps

The included Azure Pipeline (`azure-pipelines.yml`) provides:

- Multi-stage pipeline with validation, planning, and deployment
- Separate stages for dev and prod environments
- Manual approval gates for production deployments

#### Required Pipeline Variables:

- `FABRIC_TENANT_ID`
- `FABRIC_CLIENT_ID`
- `FABRIC_CLIENT_SECRET`
- `DEV_FABRIC_CAPACITY_ID`
- `PROD_FABRIC_CAPACITY_ID`

## Module Documentation

### Workspace Module

Creates a Microsoft Fabric workspace with optional Git integration.

**Inputs:**
- `display_name` (required): Display name for the workspace
- `description` (optional): Description for the workspace
- `capacity_id` (required): Microsoft Fabric capacity ID
- `enable_git_integration` (optional): Enable Git integration
- `git_config` (optional): Git configuration object

**Outputs:**
- `id`: The workspace ID
- `display_name`: The workspace display name
- `capacity_id`: The capacity ID

### Lakehouse Module

Creates a Microsoft Fabric lakehouse within a workspace.

**Inputs:**
- `display_name` (required): Display name for the lakehouse
- `description` (optional): Description for the lakehouse
- `workspace_id` (required): Parent workspace ID

**Outputs:**
- `id`: The lakehouse ID
- `display_name`: The lakehouse display name
- `workspace_id`: The workspace ID

### Warehouse Module

Creates a Microsoft Fabric warehouse within a workspace.

**Inputs:**
- `display_name` (required): Display name for the warehouse
- `description` (optional): Description for the warehouse
- `workspace_id` (required): Parent workspace ID

**Outputs:**
- `id`: The warehouse ID
- `display_name`: The warehouse display name
- `workspace_id`: The workspace ID

### Notebook Module

Creates a Microsoft Fabric notebook within a workspace.

**Inputs:**
- `display_name` (required): Display name for the notebook
- `description` (optional): Description for the notebook
- `workspace_id` (required): Parent workspace ID

**Outputs:**
- `id`: The notebook ID
- `display_name`: The notebook display name
- `workspace_id`: The workspace ID

## Remote State Configuration

For team collaboration, configure remote state storage in Azure:

```hcl
terraform {
  backend "azurerm" {
    resource_group_name  = "terraform-state-rg"
    storage_account_name = "tfstate"
    container_name       = "tfstate"
    key                  = "fabric.terraform.tfstate"
  }
}
```

## Best Practices

1. **Use Remote State**: Store state files remotely for team collaboration
2. **Environment Separation**: Use separate state files for each environment
3. **Variable Validation**: Leverage built-in variable validation
4. **Module Versioning**: Pin module versions in production
5. **Naming Conventions**: Use consistent, descriptive resource names
6. **Tagging Strategy**: Apply comprehensive tags for resource management
7. **Least Privilege**: Use service principals with minimum required permissions

## Troubleshooting

### Authentication Issues

```bash
# Verify Azure CLI authentication
az account show

# Verify Fabric provider credentials
echo $FABRIC_TENANT_ID
echo $FABRIC_CLIENT_ID
```

### State Lock Issues

```bash
# Force unlock (use with caution)
terraform force-unlock <lock-id>
```

### Provider Issues

```bash
# Reinstall providers
terraform init -upgrade
```

## Contributing

1. Create a feature branch
2. Make your changes
3. Run `terraform fmt` and `terraform validate`
4. Submit a pull request

## Resources

- [Microsoft Fabric Terraform Provider](https://registry.terraform.io/providers/microsoft/fabric/latest/docs)
- [Terraform Best Practices](https://www.terraform.io/docs/cloud/guides/recommended-practices/index.html)
- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
- [HashiCorp Learn - Terraform](https://learn.hashicorp.com/terraform)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Open an issue in this repository
- Consult the [Microsoft Fabric Terraform Provider GitHub](https://github.com/microsoft/terraform-provider-fabric)
- Review [Terraform documentation](https://www.terraform.io/docs)

## Version History

- **v1.0.0** - Initial release with workspace, lakehouse, warehouse, and notebook support
