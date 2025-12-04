variable "display_name" {
  description = "Display name for the workspace"
  type        = string
}

variable "description" {
  description = "Description for the workspace"
  type        = string
  default     = ""
}

variable "capacity_id" {
  description = "Microsoft Fabric capacity ID"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "tags" {
  description = "Tags to apply to the workspace"
  type        = map(string)
  default     = {}
}

variable "enable_git_integration" {
  description = "Enable Git integration for workspace"
  type        = bool
  default     = false
}

variable "git_config" {
  description = "Git configuration for workspace"
  type = object({
    organization_name = string
    project_name      = string
    repository_name   = string
    branch_name       = string
    directory_name    = string
  })
  default = null
}
