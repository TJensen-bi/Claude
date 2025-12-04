variable "display_name" {
  description = "Display name for the warehouse"
  type        = string
}

variable "description" {
  description = "Description for the warehouse"
  type        = string
  default     = ""
}

variable "workspace_id" {
  description = "ID of the workspace where warehouse will be created"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "tags" {
  description = "Tags to apply to the warehouse"
  type        = map(string)
  default     = {}
}
