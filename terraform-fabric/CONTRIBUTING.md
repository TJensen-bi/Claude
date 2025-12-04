# Contributing to Terraform Microsoft Fabric

Thank you for your interest in contributing to this project!

## Development Setup

1. Install prerequisites:
   - Terraform >= 1.8.0
   - Azure CLI
   - Git

2. Clone the repository
3. Create a feature branch

## Code Style

- Follow [HashiCorp Terraform Style Guide](https://www.terraform.io/docs/language/syntax/style.html)
- Run `terraform fmt -recursive` before committing
- Use meaningful variable and resource names
- Add comments for complex logic

## Testing

Before submitting:

```bash
# Format code
terraform fmt -recursive

# Validate syntax
terraform validate

# Run plan to check for errors
terraform plan
```

## Pull Request Process

1. Update documentation for any changed functionality
2. Add examples for new features
3. Ensure all CI checks pass
4. Request review from maintainers

## Module Guidelines

When creating new modules:

1. Follow the standard module structure:
   - `main.tf` - Resource definitions
   - `variables.tf` - Input variables
   - `outputs.tf` - Output values
   - `README.md` - Module documentation

2. Use clear variable descriptions
3. Provide sensible defaults where appropriate
4. Include validation rules for inputs
5. Document all outputs

## Commit Messages

Use clear, descriptive commit messages:

```
feat: add support for semantic models
fix: correct workspace role assignment
docs: update README with new examples
chore: update provider version constraints
```

## Questions?

Open an issue for any questions or concerns.
