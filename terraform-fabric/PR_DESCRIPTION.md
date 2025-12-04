# Pull Request: Add Terraform Infrastructure for Microsoft Fabric

## Summary

This PR introduces a comprehensive, production-ready Terraform project for managing Microsoft Fabric resources following HashiCorp best practices.

### Key Features

- ✅ **Modular Architecture**: Reusable modules for workspace, lakehouse, warehouse, and notebook resources
- ✅ **Multi-Environment Support**: Separate configurations for dev and prod environments
- ✅ **CI/CD Ready**: Complete pipelines for both GitHub Actions and Azure DevOps
- ✅ **Best Practices**: Follows HashiCorp's official Terraform style guide and structure
- ✅ **Type Safety**: Comprehensive variable validation and type constraints
- ✅ **Git Integration**: Optional workspace Git integration support
- ✅ **Medallion Architecture**: Example configurations for bronze/silver/gold lakehouses

### Project Structure

```
terraform-fabric/
├── modules/           # Reusable Terraform modules
│   ├── workspace/
│   ├── lakehouse/
│   ├── warehouse/
│   └── notebook/
├── environments/      # Environment-specific configurations
│   ├── dev/
│   └── prod/
├── .github/workflows/ # GitHub Actions CI/CD
└── azure-pipelines.yml # Azure DevOps pipeline
```

### What's Included

**Core Terraform Configuration:**
- Root module with orchestration logic
- Individual modules for each Fabric resource type
- Comprehensive variable definitions with validation
- Detailed outputs for all created resources
- Provider configuration with version constraints

**CI/CD Pipelines:**
- GitHub Actions workflow with automatic validation, planning, and deployment
- Azure DevOps multi-stage pipeline with approval gates
- Support for multiple environments
- Automatic PR comments with plan results

**Documentation:**
- Comprehensive README with usage examples
- CONTRIBUTING guidelines
- Module documentation
- Configuration examples for common patterns

**Best Practices:**
- Remote state configuration examples
- Proper .gitignore for Terraform projects
- Tagging strategy for resource management
- Security considerations for secrets management
- Environment-specific variable files

### Usage Example

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

### Testing Checklist

- [x] Terraform format check passes
- [x] Terraform validation passes
- [x] All modules have proper input/output definitions
- [x] Documentation is complete and accurate
- [x] CI/CD pipelines are configured correctly
- [x] Example configurations provided

### References

- [Microsoft Fabric Terraform Provider](https://registry.terraform.io/providers/microsoft/fabric/latest/docs)
- [Terraform Best Practices](https://www.terraform.io/docs/cloud/guides/recommended-practices/index.html)
- [HashiCorp Learn - Terraform](https://learn.hashicorp.com/terraform)
