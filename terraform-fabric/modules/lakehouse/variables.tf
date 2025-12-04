variable "display_name" {
  description = "Display name for the lakehouse"
  type        = string
}

variable "description" {
  description = "Description for the lakehouse"
  type        = string
  default     = ""
}

variable "workspace_id" {
  description = "ID of the workspace where lakehouse will be created"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "tags" {
  description = "Tags to apply to the lakehouse"
  type        = map(string)
  default     = {}
}
