variable "project_name" {
  description = "Project name used in resource naming"
  type        = string
  default     = "fabric"
}

variable "environment" {
  description = "Environment name (dev, test, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "test", "prod"], var.environment)
    error_message = "Environment must be dev, test, or prod."
  }
}

variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "westeurope"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

###########################################
# Key Vault Configuration
###########################################

variable "key_vault_id" {
  description = "Resource ID of the Key Vault containing secrets"
  type        = string
}

variable "key_vault_name" {
  description = "Name of the Key Vault containing secrets"
  type        = string
}

###########################################
# Service Principal Configuration
###########################################

variable "fabric_sp_app_id" {
  description = "Application (client) ID of the Fabric service principal to rotate"
  type        = string
}

###########################################
# Rotation Configuration
###########################################

variable "secret_validity_days" {
  description = "Number of days a new secret should be valid"
  type        = number
  default     = 90
}

variable "rotation_days_before" {
  description = "Number of days before expiry to rotate the secret"
  type        = number
  default     = 30
}
