variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "westeurope"
}

variable "capacity_id" {
  description = "Microsoft Fabric capacity ID"
  type        = string
}

variable "workspaces" {
  description = "Map of workspaces to create"
  type = map(object({
    display_name = string
    description  = optional(string, "")
  }))
  default = {}
}

variable "lakehouses" {
  description = "Map of lakehouses to create"
  type = map(object({
    display_name = string
    description  = optional(string, "")
    workspace_id = string
  }))
  default = {}
}

variable "warehouses" {
  description = "Map of warehouses to create"
  type = map(object({
    display_name = string
    description  = optional(string, "")
    workspace_id = string
  }))
  default = {}
}

variable "notebooks" {
  description = "Map of notebooks to create"
  type = map(object({
    display_name = string
    description  = optional(string, "")
    workspace_id = string
  }))
  default = {}
}

variable "workspace_role_assignments" {
  description = "Map of workspace role assignments"
  type = map(object({
    workspace_id = string
    principal_id = string
    role         = string
  }))
  default = {}
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    managed_by = "terraform"
  }
}

variable "enable_git_integration" {
  description = "Enable Git integration for workspaces"
  type        = bool
  default     = false
}

variable "git_config" {
  description = "Git configuration for workspaces"
  type = object({
    organization_name = string
    project_name      = string
    repository_name   = string
    branch_name       = string
    directory_name    = string
  })
  default = null
}
