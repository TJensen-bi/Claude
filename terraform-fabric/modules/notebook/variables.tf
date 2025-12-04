variable "display_name" {
  description = "Display name for the notebook"
  type        = string
}

variable "description" {
  description = "Description for the notebook"
  type        = string
  default     = ""
}

variable "workspace_id" {
  description = "ID of the workspace where notebook will be created"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "tags" {
  description = "Tags to apply to the notebook"
  type        = map(string)
  default     = {}
}
